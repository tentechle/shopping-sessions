// Shopping Sessions: native pipeline.
//
// Phases (all threads, barrier between phases):
//   1. parse   events -> records partitioned by event_id hash
//              clicks -> records partitioned by user hash
//              (thread 0 first parses links and runs union-find)
//   2. dedup   per event partition: keep min ingest copy, drop invalid, map
//              user -> person, repartition events and clicks by person
//   3. build   per person partition: group, sort, sessionize, attribute,
//              aggregate into per-thread output columns
//   4. write   one user_daily parquet per thread, one campaign_daily parquet
#include <immintrin.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

typedef uint64_t u64;
typedef uint32_t u32;
typedef int64_t i64;
typedef uint16_t u16;
typedef uint8_t u8;

static const i64 BAD_TS = INT64_MIN;
static const i64 DAY_MS = 86400000LL;
static const i64 W7 = 7 * DAY_MS;
static const i64 GAP = 1800000LL;
static const u64 FLAG = 1ULL << 63;
static const u64 PTR_MASK = (1ULL << 48) - 1;
static const u32 DEAD = 0xFFFFFFFFu;

static bool g_debug = false;
static std::chrono::steady_clock::time_point g_t0;
static void tlog(const char* what) {
  if (!g_debug) return;
  double ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - g_t0).count();
  fprintf(stderr, "[%8.2f ms] %s\n", ms, what);
}

[[noreturn]] static void die(const char* msg) {
  fprintf(stderr, "fatal: %s\n", msg);
  exit(1);
}

// ---------------------------------------------------------------- hashing

static inline u64 r8(const char* p) { u64 v; memcpy(&v, p, 8); return v; }
static inline u64 r4(const char* p) { u32 v; memcpy(&v, p, 4); return v; }
static inline u64 wymix(u64 a, u64 b) {
  __uint128_t m = (__uint128_t)a * b;
  return (u64)m ^ (u64)(m >> 64);
}
static const u64 S0 = 0xa0761d6478bd642fULL, S1 = 0xe7037ed1a0b428dbULL,
                 S2 = 0x8ebc6af09c88c6e3ULL, S3 = 0x589965cc75374cc3ULL;

struct H2 { u64 a, b; };

__attribute__((always_inline)) static inline H2 hash2(const char* p, size_t n) {
  u64 seed = S0 ^ n, a, b;
  if (n <= 16) {
    if (n >= 4) {
      a = (r4(p) << 32) | r4(p + ((n >> 3) << 2));
      b = (r4(p + n - 4) << 32) | r4(p + n - 4 - ((n >> 3) << 2));
    } else if (n > 0) {
      a = ((u64)(u8)p[0] << 16) | ((u64)(u8)p[n >> 1] << 8) | (u8)p[n - 1];
      b = 0;
    } else {
      a = b = 0;
    }
  } else {
    size_t i = n;
    const char* q = p;
    while (i > 16) {
      seed = wymix(r8(q) ^ S1, r8(q + 8) ^ seed);
      q += 16;
      i -= 16;
    }
    a = r8(q + i - 16);
    b = r8(q + i - 8);
  }
  a ^= S1;
  b ^= seed;
  __uint128_t m = (__uint128_t)a * b;
  a = (u64)m;
  b = (u64)(m >> 64);
  return H2{wymix(a ^ S0 ^ n, b ^ S1), wymix(a ^ S2, b ^ S3 ^ n)};
}
static inline u64 hash1(const char* p, size_t n) { return hash2(p, n).a; }

// Length of a value stored by pointer: values never hold , " or \n, and every
// mapped file is followed by zero padding.
static inline size_t vlen(const char* s) {
  const char* p = s;
  while (*p != ',' && *p != '"' && *p != '\n' && *p != 0) p++;
  return (size_t)(p - s);
}
static inline int vcmp(const char* a, const char* b) {
  size_t na = vlen(a), nb = vlen(b);
  int c = memcmp(a, b, na < nb ? na : nb);
  if (c) return c;
  return na < nb ? -1 : (na > nb ? 1 : 0);
}

// ---------------------------------------------------------------- files

struct MFile { const char* base; size_t size; };

static MFile map_file(const std::string& path) {
  int fd = open(path.c_str(), O_RDONLY);
  if (fd < 0) die(("open " + path).c_str());
  struct stat st;
  fstat(fd, &st);
  size_t sz = (size_t)st.st_size;
  size_t pages = (sz + 4095) / 4096;
  size_t total = pages * 4096 + 8192;
  void* region = mmap(nullptr, total, PROT_READ, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
  if (region == MAP_FAILED) die("mmap anon");
  if (sz) {
    void* r = mmap(region, sz, PROT_READ, MAP_PRIVATE | MAP_FIXED | MAP_POPULATE * 0, fd, 0);
    if (r == MAP_FAILED) die("mmap file");
  }
  close(fd);
  return MFile{(const char*)region, sz};
}

static std::string read_small(const std::string& path) {
  FILE* f = fopen(path.c_str(), "rb");
  if (!f) die(("open " + path).c_str());
  std::string s;
  char buf[65536];
  size_t n;
  while ((n = fread(buf, 1, sizeof buf, f)) > 0) s.append(buf, n);
  fclose(f);
  return s;
}

static std::vector<std::string> list_csv(const std::string& dir) {
  std::vector<std::string> out;
  DIR* d = opendir(dir.c_str());
  if (!d) return out;
  while (struct dirent* e = readdir(d)) {
    std::string n = e->d_name;
    if (n.size() >= 4 && n.compare(n.size() - 4, 4, ".csv") == 0) out.push_back(dir + "/" + n);
  }
  closedir(d);
  std::sort(out.begin(), out.end());
  return out;
}

// ---------------------------------------------------------------- CSV

// Splits one line starting at p. Fills up to 8 raw field bounds, returns the
// start of the next line (end + 1 at EOF without newline). nf is the number of
// fields, quotes the number of '"' in the line.
static inline const char* split_line(const char* p, const char* end, const char** fs, const char** fe,
                                     int& nf, int& quotes) {
  const __m256i C = _mm256_set1_epi8(','), N = _mm256_set1_epi8('\n'), Q = _mm256_set1_epi8('"');
  nf = 0;
  quotes = 0;
  fs[0] = p;
  for (;;) {
    __m256i v = _mm256_loadu_si256((const __m256i*)p);
    u32 mc = (u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(v, C));
    u32 mn = (u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(v, N));
    u32 mq = (u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(v, Q));
    size_t rem = (size_t)(end - p);
    if (rem < 32) {
      u32 lim = (1u << rem) - 1;
      mc &= lim;
      mn = (mn & lim) | (1u << rem);
      mq &= lim;
    }
    u32 sep = mc | mn;
    while (sep) {
      int i = __builtin_ctz(sep);
      u32 bit = 1u << i;
      if (mn & bit) {
        quotes += __builtin_popcount(mq & (bit - 1));
        if (nf < 8) fe[nf] = p + i;
        nf++;
        return p + i + 1;
      }
      if (nf < 8) fe[nf] = p + i;
      nf++;
      if (nf < 8) fs[nf] = p + i + 1;
      sep &= sep - 1;
    }
    quotes += __builtin_popcount(mq);
    p += 32;
  }
}

static inline void strip(const char*& s, const char*& e, int& q) {
  if (e - s >= 2 && s[0] == '"' && e[-1] == '"') {
    s++;
    e--;
    q += 2;
  }
}

static inline void strip_bl(const char*& s, const char*& e, int& q) {
  long n = e - s;
  int t = (n >= 2) & (s[0] == '"') & (e[-1] == '"');
  s += t;
  e -= t;
  q += t;
}

// Parses a line that must have NF fields. Fast path looks at 128 bytes from
// the line start; longer lines and EOF take split_line. ok is false when the
// line is malformed. Returns the start of the next line.
template <int NF>
static inline const char* parse_line(const char* p, const char* end, const char** fs, const char** fe, bool& ok) {
  if (__builtin_expect(end - p >= 128, 1)) {
    const __m256i C = _mm256_set1_epi8(','), N = _mm256_set1_epi8('\n'), Q = _mm256_set1_epi8('"');
    __m256i v0 = _mm256_loadu_si256((const __m256i*)p), v1 = _mm256_loadu_si256((const __m256i*)(p + 32));
    __m256i v2 = _mm256_loadu_si256((const __m256i*)(p + 64)), v3 = _mm256_loadu_si256((const __m256i*)(p + 96));
#define MM(x, k) ((u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(x, k)))
    u64 nlo = MM(v0, N) | (MM(v1, N) << 32);
    u64 nhi = MM(v2, N) | (MM(v3, N) << 32);
    if (__builtin_expect((nlo | nhi) != 0, 1)) {
      u64 clo = MM(v0, C) | (MM(v1, C) << 32), chi = MM(v2, C) | (MM(v3, C) << 32);
      u64 qlo = MM(v0, Q) | (MM(v1, Q) << 32), qhi = MM(v2, Q) | (MM(v3, Q) << 32);
#undef MM
      u32 nl = nlo ? (u32)_tzcnt_u64(nlo) : 64 + (u32)_tzcnt_u64(nhi);
      u64 klo = _bzhi_u64(~0ULL, nl);
      u64 khi = nl >= 64 ? _bzhi_u64(~0ULL, nl - 64) : 0;
      clo &= klo;
      chi &= khi;
      int quotes = (int)(_mm_popcnt_u64(qlo & klo) + _mm_popcnt_u64(qhi & khi));
      bool good = (_mm_popcnt_u64(clo) + _mm_popcnt_u64(chi)) == NF - 1;
      fs[0] = p;
      for (int k = 0; k < NF - 1; k++) {
        u32 tl = (u32)_tzcnt_u64(clo), th = (u32)_tzcnt_u64(chi);
        bool L = clo != 0;
        u32 pos = L ? tl : 64 + th;
        fe[k] = p + pos;
        fs[k + 1] = p + pos + 1;
        clo = _blsr_u64(clo);
        chi = L ? chi : _blsr_u64(chi);
      }
      fe[NF - 1] = p + nl;
      int qq = 0;
      for (int k = 0; k < NF; k++) strip_bl(fs[k], fe[k], qq);
      ok = good & (qq * 2 == quotes);
      return p + nl + 1;
    }
  }
  int nf, q;
  const char* nx = split_line(p, end, fs, fe, nf, q);
  ok = nf == NF;
  if (ok) {
    int qq = 0;
    for (int k = 0; k < NF; k++) strip(fs[k], fe[k], qq);
    ok = qq == q;
  }
  return nx;
}

// Parses a whole small CSV (products, campaigns) into rows of fields.
static std::vector<std::vector<std::string>> parse_small_csv(const std::string& text, int nexp) {
  std::vector<std::vector<std::string>> rows;
  std::string buf = text;
  buf.append(64, '\0');
  const char* base = buf.data();
  const char* end = base + text.size();
  const char* p = (const char*)memchr(base, '\n', text.size());
  if (!p) return rows;
  p++;
  const char* fs[8];
  const char* fe[8];
  int nf, q;
  while (p < end) {
    p = split_line(p, end, fs, fe, nf, q);
    if (nf != nexp) continue;
    int qq = 0;
    for (int k = 0; k < nf; k++) strip(fs[k], fe[k], qq);
    if (qq != q) continue;
    std::vector<std::string> r;
    for (int k = 0; k < nf; k++) r.emplace_back(fs[k], fe[k]);
    rows.push_back(std::move(r));
  }
  return rows;
}

// ---------------------------------------------------------------- timestamps

static inline i64 days_from_civil(i64 y, unsigned m, unsigned d) {
  y -= m <= 2;
  const i64 era = (y >= 0 ? y : y - 399) / 400;
  const unsigned yoe = (unsigned)(y - era * 400);
  const unsigned doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
  const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
  return era * 146097 + (i64)doe - 719468;
}

static inline bool isd(char c) { return (unsigned)(c - '0') < 10; }
static inline int d2(const char* s) { return (s[0] - '0') * 10 + (s[1] - '0'); }

// Months around the window: day number of the 1st and length, so the common
// case needs no calendar arithmetic. Other months use the full computation.
static int mt_base = 0, mt_n = 0;
static i64 mt_first[512];
static int mt_len[512];
static inline int month_len(int y, int mo) {
  static const int dim[13] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
  if (mo == 2 && ((y % 4 == 0 && y % 100 != 0) || y % 400 == 0)) return 29;
  return dim[mo];
}
static void init_months(int y0, int m0, int count) {
  mt_base = y0 * 12 + m0 - 1;
  mt_n = count;
  for (int k = 0; k < count; k++) {
    int y = (mt_base + k) / 12, mo = (mt_base + k) % 12 + 1;
    mt_first[k] = days_from_civil(y, (unsigned)mo, 1);
    mt_len[k] = month_len(y, mo);
  }
}

__attribute__((always_inline)) static inline i64 parse_ts(const char* s, size_t n) {
  if (n >= 5 && s[4] == '-') {
    if (n != 20 && n != 25) return BAD_TS;
    {
      const __m128i v = _mm_loadu_si128((const __m128i*)s);
      const __m128i d = _mm_sub_epi8(v, _mm_set1_epi8('0'));
      const __m128i dig = _mm_cmpeq_epi8(_mm_min_epu8(d, _mm_set1_epi8(9)), d);
      const __m128i pat = _mm_setr_epi8('0', '0', '0', '0', '-', '0', '0', '-', '0', '0', 'T', '0', '0', ':', '0', '0');
      const __m128i sep = _mm_cmpeq_epi8(v, pat);
      u32 m = ((u32)_mm_movemask_epi8(dig) & 0xDB6Fu) | ((u32)_mm_movemask_epi8(sep) & 0x2490u);
      if (m != 0xFFFFu || s[16] != ':' || !isd(s[17]) || !isd(s[18])) return BAD_TS;
    }
    int y = d2(s) * 100 + d2(s + 2);
    int mo = d2(s + 5), d = d2(s + 8), h = d2(s + 11), mi = d2(s + 14), se = d2(s + 17);
    if (h > 23 || mi > 59 || se > 59) return BAD_TS;
    i64 days;
    int mk = y * 12 + mo - 1 - mt_base;
    if (mo >= 1 && mo <= 12 && (unsigned)mk < (unsigned)mt_n) {
      if (d < 1 || d > mt_len[mk]) return BAD_TS;
      days = mt_first[mk] + d - 1;
    } else {
      if (mo < 1 || mo > 12 || d < 1 || d > month_len(y, mo)) return BAD_TS;
      days = days_from_civil(y, (unsigned)mo, (unsigned)d);
    }
    i64 off = 0;
    if (n == 20) {
      if (s[19] != 'Z') return BAD_TS;
    } else {
      char sg = s[19];
      if ((sg != '+' && sg != '-') || !isd(s[20]) || !isd(s[21]) || s[22] != ':' || !isd(s[23]) || !isd(s[24]))
        return BAD_TS;
      int oh = d2(s + 20), om = d2(s + 23);
      if (oh > 23 || om > 59) return BAD_TS;
      off = (i64)(oh * 60 + om) * 60000;
      if (sg == '-') off = -off;
    }
    return (days * 86400 + h * 3600 + mi * 60 + se) * 1000 - off;
  }
  if (n == 0) return BAD_TS;
  size_t i = 0;
  while (i < n && s[i] == '0') i++;
  if (n - i > 18) {
    for (size_t k = i; k < n; k++)
      if (!isd(s[k])) return BAD_TS;
    return BAD_TS;  // too large: outside every window
  }
  i64 v = 0;
  for (; i < n; i++) {
    if (!isd(s[i])) return BAD_TS;
    v = v * 10 + (s[i] - '0');
  }
  return v;
}

// ingest_ts is always valid F1; returns seconds (BAD_TS if not F1-shaped)
__attribute__((always_inline)) static inline i64 parse_ingest_s(const char* s, size_t n) {
  if (__builtin_expect(n == 20 && s[19] == 'Z', 1)) {
    int y = d2(s) * 100 + d2(s + 2), mo = d2(s + 5), d = d2(s + 8);
    int mk = y * 12 + mo - 1 - mt_base;
    if (__builtin_expect((unsigned)mk < (unsigned)mt_n && mo >= 1 && mo <= 12, 1)) {
      i64 days = mt_first[mk] + d - 1;
      return days * 86400 + d2(s + 11) * 3600 + d2(s + 14) * 60 + d2(s + 17);
    }
  }
  i64 v = parse_ts(s, n);
  return v == BAD_TS ? BAD_TS : (v >= 0 ? v / 1000 : -((-v + 999) / 1000));
}

// digits only, 1..10,000,000; -1 if bad
static inline i64 parse_price(const char* s, size_t n) {
  if (n == 0) return -1;
  size_t i = 0;
  while (i < n && s[i] == '0') i++;
  if (n - i > 8) {
    return -1;  // either non-digit or too large
  }
  i64 v = 0;
  for (; i < n; i++) {
    if (!isd(s[i])) return -1;
    v = v * 10 + (s[i] - '0');
  }
  if (v < 1 || v > 10000000) return -1;
  return v;
}

// ---------------------------------------------------------------- dictionary

// Exact string -> int map with a fast path for keys like "p123" / "c_45".
struct Dict {
  bool fast = false;
  std::string prefix;
  std::vector<int32_t> arr;
  std::unordered_map<std::string, int> m;

  static bool split_num(std::string_view k, std::string_view& pre, u64& num) {
    size_t e = k.size(), s = e;
    while (s > 0 && isd(k[s - 1])) s--;
    if (s == e || e - s > 7) return false;
    if (k[s] == '0' && e - s > 1) return false;
    pre = k.substr(0, s);
    num = 0;
    for (size_t i = s; i < e; i++) num = num * 10 + (u64)(k[i] - '0');
    return true;
  }
  void build(const std::vector<std::pair<std::string_view, int>>& kv) {
    fast = !kv.empty();
    u64 mx = 0;
    std::string_view pre0;
    for (size_t i = 0; i < kv.size() && fast; i++) {
      std::string_view pre;
      u64 num;
      if (!split_num(kv[i].first, pre, num)) fast = false;
      else if (i == 0) pre0 = pre;
      else if (pre != pre0) fast = false;
      mx = std::max(mx, num);
    }
    if (!fast) {
      for (auto& p : kv) m[std::string(p.first)] = p.second;
    } else {
      prefix = std::string(pre0);
      arr.assign(mx + 1, -1);
      for (auto& p : kv) {
        std::string_view pre;
        u64 num;
        split_num(p.first, pre, num);
        arr[num] = p.second;
      }
    }
  }
  inline int get(const char* s, size_t n) const {
    if (fast) {
      size_t pl = prefix.size();
      if (n <= pl || n - pl > 7 || memcmp(s, prefix.data(), pl) != 0) return -1;
      const char* d = s + pl;
      size_t dn = n - pl;
      if (d[0] == '0' && dn > 1) return -1;
      u64 v = 0;
      for (size_t i = 0; i < dn; i++) {
        if (!isd(d[i])) return -1;
        v = v * 10 + (u64)(d[i] - '0');
      }
      return v < arr.size() ? arr[v] : -1;
    }
    auto it = m.find(std::string(s, n));
    return it == m.end() ? -1 : it->second;
  }
};

// ---------------------------------------------------------------- arenas, block lists

struct Arena {
  char* base = nullptr;
  size_t cap = 0, used = 0;
  void init(size_t c) {
    cap = c;
    base = (char*)mmap(nullptr, cap, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
    if (base == MAP_FAILED) die("arena mmap");
    madvise(base, cap, MADV_HUGEPAGE);
  }
  inline char* alloc(size_t n) {
    if (used + n > cap) die("arena full");
    char* p = base + used;
    used += n;
    return p;
  }
};

static const size_t BLK_BYTES = 32768;
struct Blk {
  Blk* next;
  u32 n;
  u32 pad;
};
struct List {
  Blk* head = nullptr;
  Blk* last = nullptr;
};
struct Cur {
  char* cur = nullptr;
  char* end = nullptr;
};
// Per-partition append streams: hot cursors in one array, block chains in another.
template <class R>
struct Streams {
  std::vector<List> l;
  std::vector<Cur> c;
  static constexpr u32 CAP = (u32)((BLK_BYTES - sizeof(Blk)) / sizeof(R));
  void init(int n) { l.assign(n, List()); c.assign(n, Cur()); }
  __attribute__((noinline)) void grow(u32 p, Arena& a) {
    List& L = l[p];
    if (L.last) L.last->n = CAP;
    Blk* nb = (Blk*)a.alloc(BLK_BYTES);
    nb->next = nullptr;
    nb->n = 0;
    if (L.last) L.last->next = nb;
    else L.head = nb;
    L.last = nb;
    c[p].cur = (char*)(nb + 1);
    c[p].end = c[p].cur + (size_t)CAP * sizeof(R);
  }
  inline R* push(u32 p, Arena& a) {
    Cur& k = c[p];
    if (__builtin_expect(k.cur == k.end, 0)) grow(p, a);
    R* r = (R*)k.cur;
    k.cur += sizeof(R);
    return r;
  }
  void finish() {
    for (size_t p = 0; p < l.size(); p++)
      if (l[p].last) l[p].last->n = (u32)((c[p].cur - (char*)(l[p].last + 1)) / sizeof(R));
  }
};
template <class R>
static inline R* blk_data(Blk* b) { return (R*)(b + 1); }

// ---------------------------------------------------------------- records

#pragma pack(push, 4)
struct DRec {      // dedup key, partitioned by event_id hash
  u64 eid;         // event_id hash
  u64 ref;         // Ev2 pointer (48 bits, 0 if the copy is invalid) | eid check (16 bits)
  u32 ing;         // ingest seconds, biased
};
struct Ev2 {       // valid event copy, partitioned by person; ts = DEAD once deduped away
  u64 pkey;
  u64 uref;
  u32 ts;
  u32 pc;
};
struct Cl2 {       // clicks, partitioned by person in phase 1
  u64 pkey;        // ts: ms since window_start - 7 days
  u64 cref;
  u32 ts;
};
#pragma pack(pop)

// ---------------------------------------------------------------- globals

static int T = 8;
static int LOGP = 6, P = 64;      // person partitions
static int DLOG = 6, DP = 64;     // dedup partitions
static i64 WS, WE;       // window ms
static i64 day0;          // day number of window_start
static int ND;            // days in window
static Dict products, campaigns;
static std::vector<std::string> cat_names;   // sorted; index = rank
static std::vector<int> prod_cat;            // product dict value -> category rank
static int cat_unknown;
static std::vector<std::string> camp_names, camp_channels;
static int NCAMP;

static std::vector<MFile> ev_files, cl_files, ln_files;

struct Chunk { int kind; const MFile* f; size_t a, b; };
static std::vector<Chunk> chunks, lchunks;
static std::atomic<int> chunk_next{0};

// Link table: user hash -> person. LP regions by the hash's high bits, each an
// open-addressing table of words: hash tag (high 40 bits, bit 24 set) | id.
static int LLOG = 6, LP = 64;
static std::vector<u64*> lk_reg;
static std::vector<u64> lk_mask;
static std::vector<u32> lk_cnt, lk_base;
static std::vector<std::vector<const char*>> lk_lnames;
static std::vector<const char*> lk_names;   // global id -> name pointer
static std::atomic<u32>* uf_par = nullptr;
// zeroed huge-page memory for the link tables, a fixed slice per region
struct LkMem {
  char* base = nullptr;
  size_t slice = 0;
  void init(size_t total_bytes, int regions) {
    slice = ((total_bytes / regions) * 2 + (1 << 16) + 4095) & ~(size_t)4095;
    size_t sz = slice * regions;
    base = (char*)mmap(nullptr, sz, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
    madvise(base, sz, MADV_HUGEPAGE);
  }
  void* alloc(int r, size_t bytes) {
    if (bytes <= slice) return base + slice * (size_t)r;
    return calloc(bytes, 1);
  }
};
static LkMem lk_mem;
static std::atomic<u32>* uf_best = nullptr;
static u32 lk_total = 0;
static const u64 TAG_MASK = ~0xFFFFFFULL;
static inline u64 lk_tag(u64 h) { return (h | (1ULL << 24)) & TAG_MASK; }

struct Endpoint { u64 h; const char* name; };

struct CL { u32 ts; u32 camp; const char* id; };    // a person's click, local copy
struct Agg { u32 mn, mx, cnt, sid; u64 rev; };       // one 30-minute bucket of a person
struct Sess { u32 start; int day; u32 cnt; u32 pad; u64 rev; };
struct ThreadCtx {
  Arena arena;
  std::vector<std::vector<Endpoint>> lend;   // per link region
  std::vector<std::pair<u64, u64>> ledges;
  Streams<DRec> ev1;
  Streams<Ev2> ev2;
  Streams<Cl2> cl2;
  std::vector<u32> cnt2;     // ev2 + cl2 records per partition
  // phase 3 scratch
  std::vector<u64> tkeys;
  std::vector<u32> tvals;
  std::vector<u32> lid, cnt, off, ccnt, coff;
  std::vector<u64> evs, tmp;
  std::vector<CL> clv;
  std::vector<u64> pkeys, pur;
  std::vector<u32> tb;
  std::vector<Agg> bag;
  std::vector<Sess> sess;
  std::vector<i64> catday;
  std::vector<u32> mark;
  std::vector<u64> ckey;
  std::vector<int> dfirst;
  u32 stamp = 0;
  std::vector<u64> pname;
  std::vector<const Ev2*> batch;
  std::vector<int> wcnt, wpos, wlist;
  // outputs
  std::string o_name;        // PLAIN byte arrays
  std::vector<int32_t> o_day;
  std::vector<i64> o_sess, o_evn, o_rev;
  std::vector<int16_t> o_top;
  std::vector<i64> c_sess, c_purch, c_rev, c_assist;   // (NCAMP+1) x ND
  std::vector<u64> dtab;
};
static std::vector<ThreadCtx> ctx;

// ---------------------------------------------------------------- barrier

struct Barrier {
  std::atomic<int> count{0};
  std::atomic<int> gen{0};
  int n = 0;
  void wait() {
    int g = gen.load(std::memory_order_acquire);
    if (count.fetch_add(1, std::memory_order_acq_rel) == n - 1) {
      count.store(0, std::memory_order_relaxed);
      gen.fetch_add(1, std::memory_order_release);
    } else {
      while (gen.load(std::memory_order_acquire) == g) _mm_pause();
    }
  }
};
static Barrier bar;

// ---------------------------------------------------------------- phase 1

static inline u32 part_of(u64 h) { return (u32)(h >> (64 - DLOG)); }
static inline u32 part_of_pkey(u64 k) { return (u32)((k * 0x9E3779B97F4A7C15ULL) >> (64 - LOGP)); }

static inline const char* chunk_start(const MFile& f, size_t a) {
  const char* s;
  if (a == 0) s = (const char*)memchr(f.base, '\n', f.size);
  else s = (const char*)memchr(f.base + a - 1, '\n', f.size - (a - 1));
  return s ? s + 1 : nullptr;
}

// ---------------------------------------------------------------- links

static inline u32 lreg(u64 h) { return (u32)(h >> (64 - LLOG)); }

static void parse_links(ThreadCtx& c, const MFile& f, size_t a, size_t b) {
  const char* p = chunk_start(f, a);
  if (!p) return;
  const char* lim = f.base + b;
  const char* end = f.base + f.size;
  const char* fs[8];
  const char* fe[8];
  while (p < lim) {
    bool ok;
    p = parse_line<2>(p, end, fs, fe, ok);
    if (!ok || fe[0] == fs[0] || fe[1] == fs[1]) continue;
    u64 ha = hash1(fs[0], (size_t)(fe[0] - fs[0]));
    u64 hb = hash1(fs[1], (size_t)(fe[1] - fs[1]));
    c.ledges.push_back({ha, hb});
    c.lend[lreg(ha)].push_back({ha, fs[0]});
    if (hb != ha) c.lend[lreg(hb)].push_back({hb, fs[1]});
  }
}

static inline u64* lk_find_slot(u64 h) {
  u32 r = lreg(h);
  u64* t = lk_reg[r];
  u64 m = lk_mask[r], tag = lk_tag(h), s = h & m;
  for (;;) {
    u64 w = t[s];
    if ((w & TAG_MASK) == tag) return t + s;
    if (w == 0) return nullptr;
    s = (s + 1) & m;
  }
}

static inline void lk_prefetch(u64 h) {
  u32 r = lreg(h);
  __builtin_prefetch(lk_reg[r] + (h & lk_mask[r]));
}

// region r: insert endpoints, assign local ids
static void links_build_region(int r) {
  size_t n = 0;
  for (int t = 0; t < T; t++) n += ctx[t].lend[r].size();
  size_t slots = 16;
  while (slots * 4 < n * 5) slots <<= 1;
  u64* tab = (u64*)lk_mem.alloc(r, slots * 8);
  lk_reg[r] = tab;
  lk_mask[r] = slots - 1;
  auto& names = lk_lnames[r];
  for (int t = 0; t < T; t++)
    for (auto& e : ctx[t].lend[r]) {
      u64 tag = lk_tag(e.h), s = e.h & (slots - 1);
      for (;;) {
        u64 w = tab[s];
        if ((w & TAG_MASK) == tag) break;
        if (w == 0) {
          tab[s] = tag | (u64)names.size();
          names.push_back(e.name);
          break;
        }
        s = (s + 1) & (slots - 1);
      }
    }
  lk_cnt[r] = (u32)names.size();
}

static void links_globalize_region(int r) {
  u32 base = lk_base[r];
  auto& names = lk_lnames[r];
  for (u32 i = 0; i < names.size(); i++) {
    lk_names[base + i] = names[i];
    uf_par[base + i].store(base + i, std::memory_order_relaxed);
    uf_best[base + i].store(UINT32_MAX, std::memory_order_relaxed);
  }
  u64* tab = lk_reg[r];
  for (u64 s = 0; s <= lk_mask[r]; s++)
    if (tab[s]) tab[s] = (tab[s] & TAG_MASK) | (base + (tab[s] & 0xFFFFFF));
}

static inline u32 uf_find(u32 x) {
  for (;;) {
    u32 p = uf_par[x].load(std::memory_order_relaxed);
    if (p == x) return x;
    u32 g = uf_par[p].load(std::memory_order_relaxed);
    if (g != p) uf_par[x].store(g, std::memory_order_relaxed);
    x = g;
  }
}

static void links_union(ThreadCtx& c) {
  for (auto& e : c.ledges) {
    u32 a = (u32)(*lk_find_slot(e.first) & 0xFFFFFF);
    u32 b = (u32)(*lk_find_slot(e.second) & 0xFFFFFF);
    for (;;) {
      a = uf_find(a);
      b = uf_find(b);
      if (a == b) break;
      if (a < b) std::swap(a, b);
      u32 exp = a;
      if (uf_par[a].compare_exchange_strong(exp, b)) break;
    }
  }
}

static void links_best(u32 lo, u32 hi) {
  for (u32 i = lo; i < hi; i++) {
    u32 r = uf_find(i);
    u32 cur = uf_best[r].load(std::memory_order_relaxed);
    for (;;) {
      if (cur != UINT32_MAX && vcmp(lk_names[i], lk_names[cur]) >= 0) break;
      if (uf_best[r].compare_exchange_weak(cur, i)) break;
    }
  }
}

static void links_finalize_region(int r) {
  u64* tab = lk_reg[r];
  for (u64 s = 0; s <= lk_mask[r]; s++)
    if (tab[s]) {
      u32 id = (u32)(tab[s] & 0xFFFFFF);
      tab[s] = (tab[s] & TAG_MASK) | uf_best[uf_find(id)].load(std::memory_order_relaxed);
    }
}

static inline u64 person_of(u64 uh) {
  const u64* sl = lk_find_slot(uh);
  if (sl) return FLAG | (*sl & 0xFFFFFF);
  u64 pk = uh & ~FLAG;
  return pk ? pk : 2;
}

struct Pend { u64 uh, eid, uref; u32 ing, ts, pc; };
static const int RING = 16;

static inline void emit_event(ThreadCtx& c, const Pend& e) {
#ifdef EXP_NOPERSON
  u64 pk = e.uh;
#else
  u64 pk = person_of(e.uh);
#endif

  u32 pp = part_of_pkey(pk);
  Ev2* o = c.ev2.push(pp, c.arena);
  o->pkey = pk;
  o->uref = e.uref & PTR_MASK;
  o->ts = e.ts;
  o->pc = e.pc;
  c.cnt2[pp]++;
  DRec* d = c.ev1.push(part_of(e.eid), c.arena);
  d->eid = e.eid;
  d->ref = ((u64)(uintptr_t)o & PTR_MASK) | (e.uref & ~PTR_MASK);
  d->ing = e.ing;
}

static void parse_events(ThreadCtx& c, const MFile& f, size_t a, size_t b) {
  const char* p = chunk_start(f, a);
  if (!p) return;
  const char* lim = f.base + b;
  const char* end = f.base + f.size;
  const char* fs[8];
  const char* fe[8];
  const i64 ingbias = (i64)(1u << 31) - WS / 1000;
  Pend ring[RING];
  int rh = 0, rn = 0;
  while (p < lim) {
    bool ok;
    p = parse_line<7>(p, end, fs, fe, ok);
    if (!ok) continue;
    size_t en = (size_t)(fe[0] - fs[0]);
    if (!en) continue;
    H2 h = hash2(fs[0], en);
    u64 urefchk = ((u64)(uintptr_t)fs[1] & PTR_MASK) | (h.b << 48);
    i64 ing_s = parse_ingest_s(fs[3], (size_t)(fe[3] - fs[3]));
    i64 ib = (ing_s == BAD_TS ? INT64_MAX / 2000 : ing_s) + ingbias;
    u32 ingr = ib < 0 ? 0 : (ib > 0xFFFFFFFFLL ? 0xFFFFFFFFu : (u32)ib);
    size_t un = (size_t)(fe[1] - fs[1]);
    bool valid = false;
    u32 pc = 0;
    i64 ts = 0;
    if (un && fe[5] != fs[5]) {
      ts = parse_ts(fs[2], (size_t)(fe[2] - fs[2]));
      if (ts != BAD_TS && ts >= WS && ts < WE && !(ing_s != BAD_TS && ing_s * 1000 - ts > DAY_MS)) {
        size_t tn = (size_t)(fe[4] - fs[4]);
        const char* t = fs[4];
        if (tn == 4) {
          u32 w = (u32)r4(t);
          valid = w == 0x77656976u /*view*/ || w == 0x74726163u /*cart*/;
        } else if (tn == 8 && r8(t) == 0x6573616863727570ULL /*purchase*/) {
          i64 pr = parse_price(fs[6], (size_t)(fe[6] - fs[6]));
          if (pr >= 0) {
            int pi = products.get(fs[5], (size_t)(fe[5] - fs[5]));
            int cat = pi < 0 ? cat_unknown : prod_cat[pi];
            pc = ((u32)pr << 8) | (u32)cat;
            valid = true;
          }
        }
      }
    }
    if (!valid) {
      DRec* d = c.ev1.push(part_of(h.a), c.arena);
      d->eid = h.a;
      d->ref = urefchk & ~PTR_MASK;
      d->ing = ingr;
      continue;
    }
    u64 uh = hash1(fs[1], un);
    lk_prefetch(uh);
    Pend e{uh, h.a, urefchk, ingr, (u32)(ts - WS), pc};
    if (rn == RING) {
      emit_event(c, ring[rh]);
      ring[rh] = e;
      rh = (rh + 1) & (RING - 1);
    } else {
      ring[(rh + rn) & (RING - 1)] = e;
      rn++;
    }
  }
  for (int k = 0; k < rn; k++) emit_event(c, ring[(rh + k) & (RING - 1)]);
}

struct PendC { u64 uh, cref; u32 ts; };

static inline void emit_click(ThreadCtx& c, const PendC& e) {
  u64 pk = person_of(e.uh);
  u32 pp = part_of_pkey(pk);
  Cl2* r = c.cl2.push(pp, c.arena);
  r->pkey = pk;
  r->cref = e.cref;
  r->ts = e.ts;
  c.cnt2[pp]++;
}

static void parse_clicks(ThreadCtx& c, const MFile& f, size_t a, size_t b) {
  const char* p = chunk_start(f, a);
  if (!p) return;
  const char* lim = f.base + b;
  const char* end = f.base + f.size;
  const char* fs[8];
  const char* fe[8];
  const i64 lo = WS - W7;
  PendC ring[RING];
  int rh = 0, rn = 0;
  while (p < lim) {
    bool ok;
    p = parse_line<4>(p, end, fs, fe, ok);
    if (!ok || fe[0] == fs[0] || fe[1] == fs[1]) continue;
    int camp = campaigns.get(fs[2], (size_t)(fe[2] - fs[2]));
    if (camp < 0) continue;
    i64 ts = parse_ts(fs[3], (size_t)(fe[3] - fs[3]));
    if (ts == BAD_TS || ts < lo || ts >= WE) continue;
    u64 uh = hash1(fs[1], (size_t)(fe[1] - fs[1]));
    lk_prefetch(uh);
    PendC e{uh, ((u64)(uintptr_t)fs[0] & PTR_MASK) | ((u64)camp << 48), (u32)(ts - lo)};
    if (rn == RING) {
      emit_click(c, ring[rh]);
      ring[rh] = e;
      rh = (rh + 1) & (RING - 1);
    } else {
      ring[(rh + rn) & (RING - 1)] = e;
      rn++;
    }
  }
  for (int k = 0; k < rn; k++) emit_click(c, ring[(rh + k) & (RING - 1)]);
}

// ---------------------------------------------------------------- phase 2

static inline void kill(const DRec* d) {
  u64 p = d->ref & PTR_MASK;
  if (p) ((Ev2*)(uintptr_t)p)->ts = DEAD;
}

static void phase2(ThreadCtx& c, int p) {
  size_t n = 0;
  for (int t = 0; t < T; t++)
    for (Blk* b = ctx[t].ev1.l[p].head; b; b = b->next) n += b->n;
  size_t slots = 16;
  while (slots < n * 2) slots <<= 1;
  u64 mask = slots - 1;
  // words: DRec pointer (48 bits) | event_id hash tag (16 bits)
  if (c.dtab.size() < slots) c.dtab.resize(slots);
  u64* tab = c.dtab.data();
  memset(tab, 0, slots * 8);
  for (int t = 0; t < T; t++) {
    for (Blk* b = ctx[t].ev1.l[p].head; b; b = b->next) {
      const DRec* rs = blk_data<DRec>(b);
      for (u32 i = 0; i < b->n; i++) {
        const DRec* r = rs + i;
        u64 tag = r->eid & ~PTR_MASK;
        u64 w = (u64)(uintptr_t)r | tag;
        u64 s = r->eid & mask;
        for (;;) {
          u64 e = tab[s];
          if (e == 0) {
            tab[s] = w;
            break;
          }
          if ((e & ~PTR_MASK) == tag) {
            const DRec* o = (const DRec*)(uintptr_t)(e & PTR_MASK);
            if (o->eid == r->eid && (o->ref >> 48) == (r->ref >> 48)) {
              if (r->ing < o->ing) {
                kill(o);
                tab[s] = w;
              } else {
                kill(r);
              }
              break;
            }
          }
          s = (s + 1) & mask;
        }
      }
    }
  }
}

// ---------------------------------------------------------------- phase 3


static inline bool cl_less(const CL& x, const CL& y) {
  if (x.ts != y.ts) return x.ts < y.ts;
  // equal ts: larger click_id first, so the last qualifying click has the smallest id
  return vcmp(x.id, y.id) > 0;
}

static inline void sort_u32(u32* a, u32 n) {
  if (n <= 24) {
    for (u32 i = 1; i < n; i++) {
      u32 v = a[i], j = i;
      while (j > 0 && a[j - 1] > v) { a[j] = a[j - 1]; j--; }
      a[j] = v;
    }
  } else {
    std::sort(a, a + n);
  }
}

static int NB = 0;        // 30-minute buckets in the window
static int NCAT = 0;

static void phase3(ThreadCtx& c, int p) {
  size_t E = 0, C = 0;
  for (int t = 0; t < T; t++) {
    for (Blk* b = ctx[t].ev2.l[p].head; b; b = b->next) E += b->n;
    for (Blk* b = ctx[t].cl2.l[p].head; b; b = b->next) C += b->n;
  }
  if (E == 0) return;
  // person table: pkey -> local id, grows as needed
  size_t slots = 64;
  while (slots < E / 8) slots <<= 1;
  if (c.tkeys.size() < slots) { c.tkeys.resize(slots); c.tvals.resize(slots); }
  memset(c.tkeys.data(), 0, slots * 8);
  u64 mask = slots - 1;
  if (c.lid.size() < std::max(E, C)) c.lid.resize(std::max(E, C));
  c.cnt.clear();
  c.pname.clear();
  c.pkeys.clear();
  u32 np = 0;
  auto hslot = [](u64 pk) { return (pk * 0xD6E8FEB86659FD93ULL) >> 20; };
  auto rehash = [&]() {
    slots <<= 1;
    mask = slots - 1;
    if (c.tkeys.size() < slots) { c.tkeys.resize(slots); c.tvals.resize(slots); }
    u64* tk = c.tkeys.data();
    u32* tv = c.tvals.data();
    memset(tk, 0, slots * 8);
    for (u32 id = 0; id < np; id++) {
      u64 s = hslot(c.pkeys[id]) & mask;
      while (tk[s]) s = (s + 1) & mask;
      tk[s] = c.pkeys[id];
      tv[s] = id;
    }
  };
  size_t i = 0;
  for (int t = 0; t < T; t++) {
    for (Blk* b = ctx[t].ev2.l[p].head; b; b = b->next) {
      const Ev2* rs = blk_data<Ev2>(b);
      for (u32 k = 0; k < b->n; k++, i++) {
        if (rs[k].ts == DEAD) { c.lid[i] = UINT32_MAX; continue; }
        u64 pk = rs[k].pkey;
        u64* tk = c.tkeys.data();
        u32* tv = c.tvals.data();
        u64 s = hslot(pk) & mask;
        u32 id;
        for (;;) {
          if (tk[s] == pk) { id = tv[s]; break; }
          if (tk[s] == 0) {
            tk[s] = pk;
            tv[s] = id = np++;
            c.cnt.push_back(0);
            c.pkeys.push_back(pk);
            c.pname.push_back((pk & FLAG) ? (u64)(uintptr_t)lk_names[(u32)pk] : rs[k].uref);
            if ((size_t)np * 2 > slots) rehash();
            break;
          }
          s = (s + 1) & mask;
        }
        c.lid[i] = id;
        c.cnt[id]++;
      }
    }
  }
  if (np == 0) return;
  c.off.resize(np + 1);
  u32 acc = 0;
  for (u32 k = 0; k < np; k++) { c.off[k] = acc; acc += c.cnt[k]; }
  c.off[np] = acc;
  if (c.evs.size() < acc) c.evs.resize(acc);
  u64* evs = c.evs.data();
  {
    u32* pos = c.cnt.data();  // reuse as cursor
    for (u32 k = 0; k < np; k++) pos[k] = c.off[k];
    i = 0;
    for (int t = 0; t < T; t++)
      for (Blk* b = ctx[t].ev2.l[p].head; b; b = b->next) {
        const Ev2* rs = blk_data<Ev2>(b);
        for (u32 k = 0; k < b->n; k++, i++)
          if (c.lid[i] != UINT32_MAX) evs[pos[c.lid[i]]++] = ((u64)rs[k].ts << 32) | rs[k].pc;
      }
  }
  // clicks grouped by person (only persons with events)
  c.ccnt.assign(np + 1, 0);
  {
    const u64* tk = c.tkeys.data();
    const u32* tv = c.tvals.data();
    i = 0;
    for (int t = 0; t < T; t++)
      for (Blk* b = ctx[t].cl2.l[p].head; b; b = b->next) {
        const Cl2* rs = blk_data<Cl2>(b);
        for (u32 k = 0; k < b->n; k++, i++) {
          u64 pk = rs[k].pkey;
          u64 s = hslot(pk) & mask;
          u32 id = UINT32_MAX;
          for (;;) {
            if (tk[s] == pk) { id = tv[s]; break; }
            if (tk[s] == 0) break;
            s = (s + 1) & mask;
          }
          c.lid[i] = id;
          if (id != UINT32_MAX) c.ccnt[id]++;
        }
      }
  }
  c.coff.resize(np + 1);
  acc = 0;
  for (u32 k = 0; k < np; k++) { c.coff[k] = acc; acc += c.ccnt[k]; }
  c.coff[np] = acc;
  if (c.clv.size() < acc) c.clv.resize(acc);
  {
    for (u32 k = 0; k < np; k++) c.ccnt[k] = c.coff[k];
    i = 0;
    for (int t = 0; t < T; t++)
      for (Blk* b = ctx[t].cl2.l[p].head; b; b = b->next) {
        const Cl2* rs = blk_data<Cl2>(b);
        for (u32 k = 0; k < b->n; k++, i++)
          if (c.lid[i] != UINT32_MAX)
            c.clv[c.ccnt[c.lid[i]]++] = CL{rs[k].ts, (u32)(rs[k].cref >> 48), (const char*)(uintptr_t)(rs[k].cref & PTR_MASK)};
      }
  }

  const int organic = NCAMP;
  const u32 G = (u32)GAP;
  Agg* bag = c.bag.data();
  i64* catday = c.catday.data();
  u32* mark = c.mark.data();
  for (u32 pi = 0; pi < np; pi++) {
    const u64* ev = evs + c.off[pi];
    u32 n = c.off[pi + 1] - c.off[pi];
    if (c.tb.size() < n) { c.tb.resize(n); c.pur.resize(n); c.sess.resize(n); }
    u32* tb = c.tb.data();
    u64* pur = c.pur.data();
    u32 ntb = 0, npur = 0;
    for (u32 k = 0; k < n; k++) {
      u32 ts = (u32)(ev[k] >> 32);
      u32 bk = ts / G;
      Agg& a = bag[bk];
      if (a.cnt == 0) {
        tb[ntb++] = bk;
        a.mn = a.mx = ts;
      } else {
        a.mn = std::min(a.mn, ts);
        a.mx = std::max(a.mx, ts);
      }
      a.cnt++;
      u32 price = (u32)ev[k] >> 8;
      if (price) {
        a.rev += price;
        pur[npur++] = ev[k];
      }
    }
    sort_u32(tb, ntb);
    Sess* S = c.sess.data();
    u32 ns = 0, prev = 0;
    for (u32 k = 0; k < ntb; k++) {
      Agg& a = bag[tb[k]];
      if (k == 0 || a.mn - prev > G) {
        S[ns].start = a.mn;
        S[ns].day = (int)((WS + (i64)a.mn) / DAY_MS - day0);
        S[ns].cnt = 0;
        S[ns].rev = 0;
        ns++;
      }
      S[ns - 1].cnt += a.cnt;
      S[ns - 1].rev += a.rev;
      a.sid = ns - 1;
      prev = a.mx;
    }
    for (u32 k = 0; k < npur; k++) {
      u32 ts = (u32)(pur[k] >> 32), pc = (u32)pur[k];
      catday[(size_t)S[bag[ts / G].sid].day * NCAT + (pc & 255)] += pc >> 8;
    }
    for (u32 k = 0; k < ntb; k++) {
      Agg& a = bag[tb[k]];
      a.cnt = 0;
      a.rev = 0;
    }
    // attribution
    CL* ck = c.clv.data() + c.coff[pi];
    u32 m = c.coff[pi + 1] - c.coff[pi];
    if (m == 0) {
      for (u32 k = 0; k < ns; k++) {
        size_t ci = (size_t)organic * ND + S[k].day;
        c.c_sess[ci]++;
        if (S[k].rev) { c.c_purch[ci]++; c.c_rev[ci] += (i64)S[k].rev; }
      }
    } else if ((u64)ns * m <= 64 + 8ULL * (ns + m)) {
      for (u32 k = 0; k < ns; k++) {
        u32 lo = S[k].start;
        u64 hi = (u64)lo + (u64)W7;
        int day = S[k].day;
        u32 stamp = ++c.stamp;
        int best = -1;
        u32 bt = 0;
        for (u32 q = 0; q < m; q++) {
          u32 t = ck[q].ts;
          if (t >= lo && t <= hi) {
            u32 cp = ck[q].camp;
            if (mark[cp] != stamp) {
              mark[cp] = stamp;
              c.c_assist[(size_t)cp * ND + day]++;
            }
            if (best < 0 || t > bt || (t == bt && vcmp(ck[q].id, ck[best].id) < 0)) {
              best = (int)q;
              bt = t;
            }
          }
        }
        int camp = best >= 0 ? (int)ck[best].camp : organic;
        size_t ci = (size_t)camp * ND + day;
        c.c_sess[ci]++;
        if (S[k].rev) { c.c_purch[ci]++; c.c_rev[ci] += (i64)S[k].rev; }
      }
    } else {
      // many sessions and clicks: credit by two pointers over sorted clicks,
      // assists by merging each campaign's click intervals [t - 7d, t]
      std::sort(ck, ck + m, cl_less);
      u32 hi = 0;
      for (u32 k = 0; k < ns; k++) {
        u32 s0 = S[k].start;
        u64 sc = (u64)s0 + (u64)W7;
        while (hi < m && ck[hi].ts <= sc) hi++;
        int camp = (hi > 0 && ck[hi - 1].ts >= s0) ? (int)ck[hi - 1].camp : organic;
        size_t ci = (size_t)camp * ND + S[k].day;
        c.c_sess[ci]++;
        if (S[k].rev) { c.c_purch[ci]++; c.c_rev[ci] += (i64)S[k].rev; }
      }
      if (c.ckey.size() < m) c.ckey.resize(m);
      u64* key = c.ckey.data();
      for (u32 q = 0; q < m; q++) key[q] = ((u64)ck[q].camp << 32) | ck[q].ts;
      std::sort(key, key + m);
      // first session index of each day
      int* dfirst = c.dfirst.data();
      {
        u32 k = 0;
        for (int d = 0; d <= ND; d++) {
          while (k < ns && S[k].day < d) k++;
          dfirst[d] = (int)k;
        }
      }
      auto starts_lb = [&](i64 v) {  // first session with start >= v
        u32 l = 0, r = ns;
        while (l < r) {
          u32 mid = (l + r) >> 1;
          if ((i64)S[mid].start < v) l = mid + 1;
          else r = mid;
        }
        return l;
      };
      u32 q = 0;
      while (q < m) {
        u32 cp = (u32)(key[q] >> 32);
        // merged intervals of session starts [t - 7d, t] (event coordinates: t - 7d .. t)
        i64 L = (i64)(u32)key[q] - W7, R = (i64)(u32)key[q];
        q++;
        auto flush_iv = [&](i64 l, i64 r) {
          u32 a0 = starts_lb(l), b0 = starts_lb(r + 1);
          if (a0 >= b0) return;
          for (int d = S[a0].day; d <= S[b0 - 1].day; d++) {
            int lo2 = std::max((int)a0, dfirst[d]), hi2 = std::min((int)b0, dfirst[d + 1]);
            if (hi2 > lo2) c.c_assist[(size_t)cp * ND + d] += hi2 - lo2;
          }
        };
        while (q < m && (u32)(key[q] >> 32) == cp) {
          i64 l2 = (i64)(u32)key[q] - W7, r2 = (i64)(u32)key[q];
          if (l2 <= R + 1) {
            R = std::max(R, r2);
          } else {
            flush_iv(L, R);
            L = l2;
            R = r2;
          }
          q++;
        }
        flush_iv(L, R);
      }
    }
    // user_daily rows
    const char* name = (const char*)(uintptr_t)c.pname[pi];
    u32 namelen = (u32)vlen(name);
    for (u32 k = 0; k < ns;) {
      int day = S[k].day;
      i64 dsess = 0, devn = 0, drev = 0;
      for (; k < ns && S[k].day == day; k++) {
        dsess++;
        devn += S[k].cnt;
        drev += (i64)S[k].rev;
      }
      int top = -1;
      if (drev) {
        i64* cs = catday + (size_t)day * NCAT;
        i64 bv = 0;
        for (int ct = 0; ct < NCAT; ct++) {
          if (cs[ct] > bv) { bv = cs[ct]; top = ct; }
          cs[ct] = 0;
        }
      }
      c.o_name.append((const char*)&namelen, 4);
      c.o_name.append(name, namelen);
      c.o_day.push_back((int32_t)(day0 + day));
      c.o_sess.push_back(dsess);
      c.o_evn.push_back(devn);
      c.o_rev.push_back(drev);
      c.o_top.push_back((int16_t)top);
    }
  }
}

// ---------------------------------------------------------------- parquet

struct TW {
  std::string b;
  int last = 0;
  std::vector<int> stk;
  void varint(u64 v) {
    while (v >= 0x80) { b += (char)(v | 0x80); v >>= 7; }
    b += (char)v;
  }
  void zz(i64 v) { varint((u64)((v << 1) ^ (v >> 63))); }
  void fh(int id, int type) {
    int d = id - last;
    if (d > 0 && d <= 15) b += (char)((d << 4) | type);
    else { b += (char)type; zz(id); }
    last = id;
  }
  void i32f(int id, i64 v) { fh(id, 5); zz(v); }
  void i64f(int id, i64 v) { fh(id, 6); zz(v); }
  void strf(int id, const std::string& s) { fh(id, 8); varint(s.size()); b += s; }
  void sbeg(int id) { fh(id, 12); stk.push_back(last); last = 0; }
  void ebeg() { stk.push_back(last); last = 0; }  // struct as list element
  void send() { b += (char)0; last = stk.back(); stk.pop_back(); }
  void listh(int id, int et, int n) {
    fh(id, 9);
    if (n < 15) b += (char)((n << 4) | et);
    else { b += (char)(0xF0 | et); varint((u64)n); }
  }
};

enum PType { PT_INT32 = 1, PT_INT64 = 2, PT_BYTES = 6 };
struct PCol {
  std::string name;
  int ptype;
  bool optional;
  int logical;   // 0 none, 1 string, 6 date
  const char* data;
  size_t size;    // value bytes (PLAIN)
  const std::vector<int16_t>* defs;  // for optional: >=0 present
  size_t nvals;
};

static void write_parquet(const std::string& path, std::vector<PCol>& cols, size_t nrows) {
  std::string out = "PAR1";
  std::vector<i64> page_off(cols.size()), chunk_sz(cols.size());
  for (size_t ci = 0; ci < cols.size() && nrows; ci++) {
    PCol& c = cols[ci];
    std::string levels;
    if (c.optional) {
      std::string rle;
      size_t groups = (nrows + 7) / 8;
      TW v;
      v.varint((groups << 1) | 1);
      rle = v.b;
      size_t base = rle.size();
      rle.resize(base + groups, 0);
      for (size_t r = 0; r < nrows; r++)
        if ((*c.defs)[r] >= 0) rle[base + r / 8] |= (char)(1 << (r % 8));
      u32 ln = (u32)rle.size();
      levels.append((const char*)&ln, 4);
      levels += rle;
    }
    size_t psz = levels.size() + c.size;
    TW h;
    h.i32f(1, 0);
    h.i32f(2, (i64)psz);
    h.i32f(3, (i64)psz);
    h.sbeg(5);
    h.i32f(1, (i64)nrows);
    h.i32f(2, 0);
    h.i32f(3, 3);
    h.i32f(4, 3);
    h.send();
    h.b += (char)0;
    page_off[ci] = (i64)out.size();
    out += h.b;
    out += levels;
    out.append(c.data, c.size);
    chunk_sz[ci] = (i64)out.size() - page_off[ci];
  }
  TW m;
  m.i32f(1, 1);
  m.listh(2, 12, (int)cols.size() + 1);
  m.ebeg();
  m.strf(4, "schema");
  m.i32f(5, (i64)cols.size());
  m.send();
  for (auto& c : cols) {
    m.ebeg();
    m.i32f(1, c.ptype);
    m.i32f(3, c.optional ? 1 : 0);
    m.strf(4, c.name);
    if (c.logical == 1) m.i32f(6, 0);
    if (c.logical == 6) m.i32f(6, 6);
    if (c.logical) {
      m.sbeg(10);
      m.sbeg(c.logical);
      m.send();
      m.send();
    }
    m.send();
  }
  m.i64f(3, (i64)nrows);
  if (nrows) {
    m.listh(4, 12, 1);
    m.ebeg();
    m.listh(1, 12, (int)cols.size());
    i64 total = 0;
    for (size_t ci = 0; ci < cols.size(); ci++) {
      PCol& c = cols[ci];
      total += chunk_sz[ci];
      m.ebeg();
      m.i64f(2, page_off[ci]);
      m.sbeg(3);
      m.i32f(1, c.ptype);
      m.listh(2, 5, 2);
      m.zz(0);
      m.zz(3);
      m.listh(3, 8, 1);
      m.varint(c.name.size());
      m.b += c.name;
      m.i32f(4, 0);
      m.i64f(5, (i64)nrows);
      m.i64f(6, chunk_sz[ci]);
      m.i64f(7, chunk_sz[ci]);
      m.i64f(9, page_off[ci]);
      m.send();
      m.send();
    }
    m.i64f(2, total);
    m.i64f(3, (i64)nrows);
    m.send();
  } else {
    m.listh(4, 12, 0);
  }
  m.b += (char)0;
  out += m.b;
  u32 fl = (u32)m.b.size();
  out.append((const char*)&fl, 4);
  out += "PAR1";
  int fd = open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0666);
  if (fd < 0) die(("create " + path).c_str());
  size_t w = 0;
  while (w < out.size()) {
    ssize_t r = write(fd, out.data() + w, out.size() - w);
    if (r <= 0) die("write");
    w += (size_t)r;
  }
  close(fd);
}

static void write_user_daily(ThreadCtx& c, const std::string& path) {
  size_t n = c.o_day.size();
  std::string tops;
  for (size_t r = 0; r < n; r++) {
    int t = c.o_top[r];
    if (t >= 0) {
      u32 l = (u32)cat_names[t].size();
      tops.append((const char*)&l, 4);
      tops += cat_names[t];
    }
  }
  std::vector<PCol> cols = {
      {"user_id", PT_BYTES, false, 1, c.o_name.data(), c.o_name.size(), nullptr, n},
      {"day", PT_INT32, false, 6, (const char*)c.o_day.data(), n * 4, nullptr, n},
      {"sessions", PT_INT64, false, 0, (const char*)c.o_sess.data(), n * 8, nullptr, n},
      {"events", PT_INT64, false, 0, (const char*)c.o_evn.data(), n * 8, nullptr, n},
      {"revenue_cents", PT_INT64, false, 0, (const char*)c.o_rev.data(), n * 8, nullptr, n},
      {"top_category", PT_BYTES, true, 1, tops.data(), tops.size(), &c.o_top, n},
  };
  write_parquet(path, cols, n);
}

static void write_campaign_daily(const std::string& path) {
  size_t cells = (size_t)(NCAMP + 1) * ND;
  std::vector<i64> s(cells, 0), pu(cells, 0), rv(cells, 0), as(cells, 0);
  for (auto& c : ctx)
    for (size_t k = 0; k < cells; k++) {
      s[k] += c.c_sess[k];
      pu[k] += c.c_purch[k];
      rv[k] += c.c_rev[k];
      as[k] += c.c_assist[k];
    }
  std::string ids, chans;
  std::vector<int32_t> days;
  std::vector<i64> os, op, orv, oa;
  for (int cp = 0; cp <= NCAMP; cp++)
    for (int d = 0; d < ND; d++) {
      size_t k = (size_t)cp * ND + d;
      if (!s[k] && !as[k]) continue;
      const std::string& id = cp == NCAMP ? std::string("organic") : camp_names[cp];
      const std::string& ch = cp == NCAMP ? std::string("organic") : camp_channels[cp];
      u32 l = (u32)id.size();
      ids.append((const char*)&l, 4);
      ids += id;
      l = (u32)ch.size();
      chans.append((const char*)&l, 4);
      chans += ch;
      days.push_back((int32_t)(day0 + d));
      os.push_back(s[k]);
      op.push_back(pu[k]);
      orv.push_back(rv[k]);
      oa.push_back(as[k]);
    }
  size_t n = days.size();
  std::vector<PCol> cols = {
      {"campaign_id", PT_BYTES, false, 1, ids.data(), ids.size(), nullptr, n},
      {"channel", PT_BYTES, false, 1, chans.data(), chans.size(), nullptr, n},
      {"day", PT_INT32, false, 6, (const char*)days.data(), n * 4, nullptr, n},
      {"sessions", PT_INT64, false, 0, (const char*)os.data(), n * 8, nullptr, n},
      {"purchasing_sessions", PT_INT64, false, 0, (const char*)op.data(), n * 8, nullptr, n},
      {"revenue_cents", PT_INT64, false, 0, (const char*)orv.data(), n * 8, nullptr, n},
      {"assisted_sessions", PT_INT64, false, 0, (const char*)oa.data(), n * 8, nullptr, n},
  };
  write_parquet(path, cols, n);
}

// ---------------------------------------------------------------- main

static std::string json_str(const std::string& j, const char* key) {
  size_t k = j.find(std::string("\"") + key + "\"");
  if (k == std::string::npos) die("manifest key");
  size_t c = j.find(':', k);
  size_t q1 = j.find('"', c + 1);
  size_t q2 = j.find('"', q1 + 1);
  return j.substr(q1 + 1, q2 - q1 - 1);
}

static std::string OUT;
static std::vector<int> p3order;

static std::atomic<int> q_links{0}, q_lreg{0}, q_lglob{0}, q_lfin{0}, q_p2{0}, q_p3{0};

static void worker(int tid) {
  ThreadCtx& c = ctx[tid];
  // links: parse, build regions, union, smallest name per person
  for (;;) {
    int k = q_links.fetch_add(1, std::memory_order_relaxed);
    if (k >= (int)lchunks.size()) break;
    parse_links(c, *lchunks[k].f, lchunks[k].a, lchunks[k].b);
  }
  bar.wait();
  for (;;) {
    int r = q_lreg.fetch_add(1, std::memory_order_relaxed);
    if (r >= LP) break;
    links_build_region(r);
  }
  bar.wait();
  if (tid == 0) {
    u32 acc = 0;
    for (int r = 0; r < LP; r++) { lk_base[r] = acc; acc += lk_cnt[r]; }
    lk_total = acc;
    if (acc >= (1u << 24)) die("too many linked ids");
    lk_names.resize(acc);
    uf_par = new std::atomic<u32>[acc + 1];
    uf_best = new std::atomic<u32>[acc + 1];
    tlog("links regions");
  }
  bar.wait();
  for (;;) {
    int r = q_lglob.fetch_add(1, std::memory_order_relaxed);
    if (r >= LP) break;
    links_globalize_region(r);
  }
  bar.wait();
  links_union(c);
  bar.wait();
  {
    u32 per = (lk_total + T - 1) / T;
    u32 lo = std::min(lk_total, per * tid), hi = std::min(lk_total, lo + per);
    links_best(lo, hi);
  }
  bar.wait();
  for (;;) {
    int r = q_lfin.fetch_add(1, std::memory_order_relaxed);
    if (r >= LP) break;
    links_finalize_region(r);
  }
  bar.wait();
  if (tid == 0) tlog("links done");
  for (;;) {
    int k = chunk_next.fetch_add(1, std::memory_order_relaxed);
    if (k >= (int)chunks.size()) break;
    const Chunk& ch = chunks[k];
    if (ch.kind == 0) parse_events(c, *ch.f, ch.a, ch.b);
#ifndef EXP_NOCLICKS
    else parse_clicks(c, *ch.f, ch.a, ch.b);
#endif
  }
  c.ev1.finish();
  c.ev2.finish();
  c.cl2.finish();
  bar.wait();
  if (tid == 0) tlog("phase 1 done");
  for (;;) {
    int p = q_p2.fetch_add(1, std::memory_order_relaxed);
    if (p >= DP) break;
    phase2(c, p);
  }
  bar.wait();
  if (tid == 0) {
    tlog("phase 2 done");
    std::vector<std::pair<u64, int>> sz(P);
    for (int p = 0; p < P; p++) {
      u64 s = 0;
      for (int t = 0; t < T; t++) s += ctx[t].cnt2[p];
      sz[p] = {s, p};
    }
    std::sort(sz.begin(), sz.end(), [](auto& a, auto& b) { return a.first > b.first; });
    p3order.resize(P);
    for (int p = 0; p < P; p++) p3order[p] = sz[p].second;
  }
  bar.wait();
  for (;;) {
    int k = q_p3.fetch_add(1, std::memory_order_relaxed);
    if (k >= P) break;
    phase3(c, p3order[k]);
  }
  bar.wait();
  if (tid == 0) tlog("phase 3 done");

  if (tid == 0 || !c.o_day.empty())
    write_user_daily(c, OUT + "/user_daily/part-" + std::to_string(tid) + ".parquet");
  if (tid == T - 1) write_campaign_daily(OUT + "/campaign_daily/part-0.parquet");
  if (tid == 0) tlog("written");
}

int main() {
  g_t0 = std::chrono::steady_clock::now();
  umask(0);  // outputs removable by any user (the scorer's host user is not uid 1000)
  g_debug = getenv("PIPE_DEBUG") != nullptr;
  const char* e;
  std::string DATA = (e = getenv("DATA_DIR")) ? e : "/data";
  OUT = (e = getenv("OUTPUT_DIR")) ? e : "/output";
  T = (e = getenv("NCPU")) ? atoi(e) : (int)std::thread::hardware_concurrency();
  if ((e = getenv("PIPE_THREADS"))) T = atoi(e);
  if (T < 1) T = 1;
  if (T > 64) T = 64;

  std::string man = read_small(DATA + "/manifest.json");
  std::string ws = json_str(man, "window_start"), we = json_str(man, "window_end");
  WS = parse_ts(ws.data(), ws.size());
  WE = parse_ts(we.data(), we.size());
  if (WS == BAD_TS || WE == BAD_TS || WE <= WS) die("bad manifest");
  if (WE - WS >= 0xFFFFFFFFLL - W7 - 1) die("window too long");
  day0 = WS / DAY_MS;
  {
    // months from 60 days before the click window to 60 days after the window
    i64 dlo = (WS - W7) / DAY_MS - 60;
    i64 z = dlo + 719468, era = (z >= 0 ? z : z - 146096) / 146097;
    unsigned doe = (unsigned)(z - era * 146097), yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    unsigned doy = doe - (365 * yoe + yoe / 4 - yoe / 100), mp = (5 * doy + 2) / 153;
    int mo = (int)(mp < 10 ? mp + 3 : mp - 9);
    int y = (int)(yoe + era * 400) + (mo <= 2);
    int months = (int)((WE - WS + W7) / DAY_MS / 28) + 8;
    init_months(y, mo, std::min(months, 500));
  }
  ND = (int)((WE - 1) / DAY_MS - day0 + 1);
  NB = (int)((WE - WS) / GAP + 2);

  // products & categories, campaigns: rows parsed in place
  std::string ptxt = read_small(DATA + "/products.csv"), ctxt = read_small(DATA + "/campaigns.csv");
  auto rows_of = [](std::string& txt) {
    std::vector<std::pair<std::string_view, std::string_view>> rows;
    size_t n = txt.size();
    txt.append(256, '\0');
    const char* base = txt.data();
    const char* end = base + n;
    const char* p = (const char*)memchr(base, '\n', n);
    if (!p) return rows;
    p++;
    const char* fs[8];
    const char* fe[8];
    while (p < end) {
      bool ok;
      p = parse_line<2>(p, end, fs, fe, ok);
      if (ok) rows.push_back({std::string_view(fs[0], (size_t)(fe[0] - fs[0])), std::string_view(fs[1], (size_t)(fe[1] - fs[1]))});
    }
    return rows;
  };
  {
    auto rows = rows_of(ptxt);
    std::unordered_map<std::string_view, int> seen;
    std::vector<int> first_id(rows.size());
    std::vector<std::string_view> order;
    for (size_t i = 0; i < rows.size(); i++) {
      auto it = seen.find(rows[i].second);
      if (it == seen.end()) {
        it = seen.emplace(rows[i].second, (int)order.size()).first;
        order.push_back(rows[i].second);
      }
      first_id[i] = it->second;
    }
    std::vector<std::string> cats(order.begin(), order.end());
    if (!seen.count("unknown")) cats.push_back("unknown");
    std::vector<std::string> sorted = cats;
    std::sort(sorted.begin(), sorted.end());
    if (sorted.size() > 255) die("too many categories");
    cat_names = sorted;
    NCAT = (int)sorted.size();
    std::vector<int> rank_of(cats.size());
    for (size_t i = 0; i < cats.size(); i++)
      rank_of[i] = (int)(std::lower_bound(sorted.begin(), sorted.end(), cats[i]) - sorted.begin());
    cat_unknown = (int)(std::lower_bound(sorted.begin(), sorted.end(), std::string("unknown")) - sorted.begin());
    std::vector<std::pair<std::string_view, int>> kv;
    kv.reserve(rows.size());
    prod_cat.resize(rows.size());
    for (size_t i = 0; i < rows.size(); i++) {
      kv.push_back({rows[i].first, (int)i});
      prod_cat[i] = rank_of[first_id[i]];
    }
    products.build(kv);
  }
  {
    auto rows = rows_of(ctxt);
    std::vector<std::pair<std::string_view, int>> kv;
    for (auto& r : rows) {
      kv.push_back({r.first, (int)camp_names.size()});
      camp_names.emplace_back(r.first);
      camp_channels.emplace_back(r.second);
    }
    NCAMP = (int)camp_names.size();
    if (NCAMP >= 65535) die("too many campaigns");
    campaigns.build(kv);
  }
  tlog("small files");

  for (auto& f : list_csv(DATA + "/events")) ev_files.push_back(map_file(f));
  for (auto& f : list_csv(DATA + "/clicks")) cl_files.push_back(map_file(f));
  for (auto& f : list_csv(DATA + "/links")) ln_files.push_back(map_file(f));
  size_t ev_bytes = 0, cl_bytes = 0;
  for (auto& f : ev_files) ev_bytes += f.size;
  for (auto& f : cl_files) cl_bytes += f.size;
  tlog("mapped");

  size_t est_events = ev_bytes / 80 + 1;
  LOGP = 4;
  while (LOGP < 12 && (est_events >> LOGP) > 24000) LOGP++;
  LOGP = std::max(4, LOGP - 2);
  DLOG = LOGP;
  if ((e = getenv("PIPE_LOGP"))) LOGP = atoi(e);
  if ((e = getenv("PIPE_DLOG"))) DLOG = atoi(e);
  P = 1 << LOGP;
  DP = 1 << DLOG;

  const size_t CH = 4 << 20;
  for (auto& f : ev_files)
    for (size_t a = 0; a < f.size; a += CH) chunks.push_back({0, &f, a, std::min(f.size, a + CH)});
  for (auto& f : cl_files)
    for (size_t a = 0; a < f.size; a += CH) chunks.push_back({1, &f, a, std::min(f.size, a + CH)});
  for (auto& f : ln_files)
    for (size_t a = 0; a < f.size; a += (1 << 20)) lchunks.push_back({2, &f, a, std::min(f.size, a + (1 << 20))});
  {
    size_t lb = 0;
    for (auto& f : ln_files) lb += f.size;
    size_t est_ends = lb / 10 + 1024;   // about 2 endpoints per ~20-byte line
    size_t sl = 16;
    while (sl * 4 < (est_ends / LP) * 5) sl <<= 1;
    lk_mem.init(sl * 8 * LP, LP);
  }
  lk_reg.resize(LP); lk_mask.resize(LP); lk_cnt.resize(LP); lk_base.resize(LP); lk_lnames.resize(LP);

  mkdir((OUT + "/user_daily").c_str(), 0777);
  mkdir((OUT + "/campaign_daily").c_str(), 0777);

  ctx.resize(T);
  size_t per_thread = ((ev_bytes + cl_bytes) / (size_t)T) * 2 + (size_t)P * 4 * BLK_BYTES + (64ull << 20);
  size_t cells = (size_t)(NCAMP + 1) * ND;
  for (int t = 0; t < T; t++) {
    ThreadCtx& c = ctx[t];
    c.arena.init(per_thread * 3);
    c.ev1.init(DP); c.ev2.init(P); c.cl2.init(P);
    c.lend.resize(LP);
    c.cnt2.assign(P, 0);
    c.c_sess.assign(cells, 0); c.c_purch.assign(cells, 0); c.c_rev.assign(cells, 0); c.c_assist.assign(cells, 0);
    c.bag.assign(NB, Agg{0, 0, 0, 0, 0});
    c.catday.assign((size_t)ND * NCAT, 0);
    c.mark.assign(NCAMP + 1, 0);
    c.dfirst.assign(ND + 2, 0);
    c.wcnt.assign(NCAMP + 1, 0); c.wpos.assign(NCAMP + 1, 0); c.wlist.assign(NCAMP + 1, 0);
  }
  bar.n = T;
  tlog("setup");
  std::vector<std::thread> th;
  for (int t = 1; t < T; t++) th.emplace_back(worker, t);
  worker(0);
  for (auto& x : th) x.join();
  tlog("done");
  if (g_debug) {
    std::string sm = read_small("/proc/self/smaps_rollup");
    fprintf(stderr, "%s\n", sm.c_str()); size_t k = sm.find("AnonHugePages");
    if (k != std::string::npos) fprintf(stderr, "%s\n", sm.substr(k, sm.find('\n', k) - k).c_str());
    std::string st = read_small("/proc/self/stat");
    fprintf(stderr, "stat: %s\n", st.c_str());
  }
  fflush(stdout);
  _exit(0);
}
