#!/usr/bin/env python3
"""
Reference implementation of the "Shopping Sessions" pipeline (see ../rules.md).

Written for clarity and literal adherence to the rules, not speed.
Pure standard library, plus pyarrow to write the Parquet outputs.

Usage:
    DATA_DIR=... OUTPUT_DIR=... python reference.py
    python reference.py DATA_DIR OUTPUT_DIR
"""
import bisect
import glob
import json
import os
import sys
from collections import defaultdict

import pyarrow as pa
import pyarrow.parquet as pq

# F3 values may be arbitrarily long digit strings; Python must be allowed to convert them.
if hasattr(sys, "set_int_max_str_digits"):
    sys.set_int_max_str_digits(0)

MS_PER_SECOND = 1000
MS_PER_MINUTE = 60 * MS_PER_SECOND
MS_PER_HOUR = 60 * MS_PER_MINUTE
MS_PER_DAY = 24 * MS_PER_HOUR

SESSION_GAP_MS = 30 * MS_PER_MINUTE          # 1,800,000
LATE_LIMIT_MS = 24 * MS_PER_HOUR             # 86,400,000
ATTRIBUTION_WINDOW_MS = 7 * MS_PER_DAY       # 604,800,000

ORGANIC = b"organic"
UNKNOWN_CATEGORY = b"unknown"
VALID_EVENT_TYPES = (b"view", b"cart", b"purchase")


# --------------------------------------------------------------------------------------
# CSV dialect
# --------------------------------------------------------------------------------------

QUOTE = ord('"')


def parse_field(raw):
    """Return the field value (bytes; None means null/empty), or raise ValueError if malformed.

    A field either contains no quote at all, or is wrapped entirely in quotes with no
    quote inside. An empty value (bare or "") is null.
    """
    if QUOTE not in raw:
        value = raw
    elif len(raw) >= 2 and raw[0] == QUOTE and raw[-1] == QUOTE and QUOTE not in raw[1:-1]:
        value = raw[1:-1]
    else:
        raise ValueError("stray quote")
    return value if value != b"" else None


def parse_line(line, n_fields):
    """Split one line (bytes, no \\n) into exactly n_fields values, or return None if malformed."""
    if line == b"":
        return None
    raw_fields = line.split(b",")
    if len(raw_fields) != n_fields:
        return None
    try:
        return [parse_field(raw) for raw in raw_fields]
    except ValueError:
        return None


def read_csv_lines(path):
    """Yield the data lines of a file (bytes), split on b"\\n" only, skipping the first line (header)."""
    with open(path, "rb") as f:
        content = f.read()
    lines = content.split(b"\n")
    # A trailing "\n" terminates the last line; it does not start a new (empty) one.
    if lines and lines[-1] == b"":
        lines.pop()
    # Only the first line of each file is the header.
    for line in lines[1:]:
        yield line


def read_rows(path, n_fields):
    """Yield parsed (well-formed) rows of a CSV file as lists of bytes-or-None."""
    for line in read_csv_lines(path):
        row = parse_line(line, n_fields)
        if row is not None:
            yield row


def csv_files(directory):
    return sorted(glob.glob(os.path.join(directory, "*.csv")))


# --------------------------------------------------------------------------------------
# Timestamps
# --------------------------------------------------------------------------------------

def is_ascii_digits(b):
    return len(b) > 0 and all(48 <= c <= 57 for c in b)


def is_leap(year):
    return year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)


def days_in_month(year, month):
    if month == 2:
        return 29 if is_leap(year) else 28
    if month in (4, 6, 9, 11):
        return 30
    return 31


def days_from_civil(year, month, day):
    """Days since 1970-01-01 for a proleptic Gregorian date (Howard Hinnant's algorithm)."""
    y = year - 1 if month <= 2 else year
    era = y // 400
    yoe = y - era * 400
    mp = (month + 9) % 12  # March = 0
    doy = (153 * mp + 2) // 5 + day - 1
    doe = yoe * 365 + yoe // 4 - yoe // 100 + doy
    return era * 146097 + doe - 719468


def parse_datetime_part(b):
    """Parse exactly 'YYYY-MM-DDTHH:MM:SS' (19 bytes) to epoch ms (as if UTC), or None."""
    if len(b) != 19:
        return None
    if b[4:5] != b"-" or b[7:8] != b"-" or b[10:11] != b"T" or b[13:14] != b":" or b[16:17] != b":":
        return None
    parts = (b[0:4], b[5:7], b[8:10], b[11:13], b[14:16], b[17:19])
    if not all(is_ascii_digits(p) for p in parts):
        return None
    year, month, day, hour, minute, second = (int(p) for p in parts)
    if not (1 <= month <= 12):
        return None
    if not (1 <= day <= days_in_month(year, month)):
        return None
    if hour > 23 or minute > 59 or second > 59:
        return None
    days = days_from_civil(year, month, day)
    return days * MS_PER_DAY + hour * MS_PER_HOUR + minute * MS_PER_MINUTE + second * MS_PER_SECOND


def parse_ts(b):
    """Parse an F1, F2 or F3 timestamp (bytes) to integer epoch ms UTC, or None if bad."""
    if b is None or b == b"":
        return None
    # F3: digits only, any length, leading zeros allowed.
    if is_ascii_digits(b):
        return int(b)
    # F1: YYYY-MM-DDTHH:MM:SSZ
    if len(b) == 20 and b[19:20] == b"Z":
        return parse_datetime_part(b[:19])
    # F2: YYYY-MM-DDTHH:MM:SS+HH:MM or -HH:MM
    if len(b) == 25 and b[19:20] in (b"+", b"-"):
        local = parse_datetime_part(b[:19])
        if local is None:
            return None
        off_h, colon, off_m = b[20:22], b[22:23], b[23:25]
        if colon != b":" or not is_ascii_digits(off_h) or not is_ascii_digits(off_m):
            return None
        oh, om = int(off_h), int(off_m)
        if oh > 23 or om > 59:
            return None
        offset_ms = oh * MS_PER_HOUR + om * MS_PER_MINUTE
        sign = 1 if b[19:20] == b"+" else -1
        return local - sign * offset_ms
    return None


# --------------------------------------------------------------------------------------
# Linked IDs (union-find)
# --------------------------------------------------------------------------------------

class UnionFind:
    def __init__(self):
        self.parent = {}

    def find(self, x):
        if x not in self.parent:
            self.parent[x] = x
            return x
        root = x
        while self.parent[root] != root:
            root = self.parent[root]
        # path compression
        while self.parent[x] != root:
            self.parent[x], x = root, self.parent[x]
        return root

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[ra] = rb


def build_person_names(links_dir):
    """Return dict: user_id -> person name (byte-wise smallest ID of its linked group)."""
    uf = UnionFind()
    for path in csv_files(links_dir):
        for a, b in read_rows(path, 2):
            if a is None or b is None:
                continue
            uf.union(a, b)
    smallest = {}
    for x in list(uf.parent):
        root = uf.find(x)
        if root not in smallest or x < smallest[root]:
            smallest[root] = x
    return {x: smallest[uf.find(x)] for x in uf.parent}


# --------------------------------------------------------------------------------------
# Pipeline
# --------------------------------------------------------------------------------------

def main():
    if len(sys.argv) >= 3:
        data_dir, output_dir = sys.argv[-2], sys.argv[-1]
    else:
        data_dir, output_dir = os.environ["DATA_DIR"], os.environ["OUTPUT_DIR"]

    with open(os.path.join(data_dir, "manifest.json"), "rb") as f:
        manifest = json.load(f)
    window_start = parse_ts(manifest["window_start"].encode())
    window_end = parse_ts(manifest["window_end"].encode())
    assert window_start is not None and window_end is not None
    click_window_start = window_start - ATTRIBUTION_WINDOW_MS

    # Reference tables (clean).
    product_category = {}
    for product_id, category in read_rows(os.path.join(data_dir, "products.csv"), 2):
        product_category[product_id] = category
    campaign_channel = {}
    for campaign_id, channel in read_rows(os.path.join(data_dir, "campaigns.csv"), 2):
        campaign_channel[campaign_id] = channel

    # Linked IDs.
    person_names = build_person_names(os.path.join(data_dir, "links"))

    def person_of(user_id):
        return person_names.get(user_id, user_id)

    # ---------------- Events ----------------
    # 1. Parse + 2. Dedup on event_id keeping the smallest ingest_ts (before validation).
    kept_events = {}  # event_id -> (ingest_ms, row)
    for path in csv_files(os.path.join(data_dir, "events")):
        for row in read_rows(path, 7):
            event_id = row[0]
            if event_id is None:
                continue
            ingest_ms = parse_ts(row[3])
            if ingest_ms is None:
                raise ValueError(f"ingest_ts is not valid: {row[3]!r} in {path}")
            prev = kept_events.get(event_id)
            if prev is None or ingest_ms < prev[0]:
                kept_events[event_id] = (ingest_ms, row)

    # 3. Validate + 4. Drop late events.
    events_by_person = defaultdict(list)  # person -> list of (ts_ms, event_type, product_id, price)
    for event_id, (ingest_ms, row) in kept_events.items():
        _, user_id, ts_raw, _, event_type, product_id, price_raw = row
        if user_id is None:
            continue
        ts_ms = parse_ts(ts_raw)
        if ts_ms is None or not (window_start <= ts_ms < window_end):
            continue
        if event_type not in VALID_EVENT_TYPES:
            continue
        if product_id is None:
            continue
        price = 0
        if event_type == b"purchase":
            if price_raw is None or not is_ascii_digits(price_raw):
                continue
            price = int(price_raw)
            if not (1 <= price <= 10_000_000):
                continue
        if ingest_ms - ts_ms > LATE_LIMIT_MS:
            continue
        events_by_person[person_of(user_id)].append((ts_ms, event_type, product_id, price))
    del kept_events

    # 5. Sessionize per person.
    # A session: dict(person, start, events=[...]).
    sessions_by_person = {}
    for person, evs in events_by_person.items():
        evs.sort(key=lambda e: e[0])
        sessions = []
        current = None
        prev_ts = None
        for ev in evs:
            ts_ms = ev[0]
            if current is None or ts_ms - prev_ts > SESSION_GAP_MS:
                current = {"start": ts_ms, "events": []}
                sessions.append(current)
            current["events"].append(ev)
            prev_ts = ts_ms
        sessions_by_person[person] = sessions
    del events_by_person

    # ---------------- Clicks ----------------
    # 1. Parse + 2. Dedup on click_id (keep any copy: the first one read).
    kept_clicks = {}
    for path in csv_files(os.path.join(data_dir, "clicks")):
        for row in read_rows(path, 4):
            click_id = row[0]
            if click_id is None:
                continue
            if click_id not in kept_clicks:
                kept_clicks[click_id] = row

    # 3. Validate.
    clicks_by_person = defaultdict(list)  # person -> list of (ts_ms, click_id, campaign_id)
    for click_id, (_, user_id, campaign_id, ts_raw) in kept_clicks.items():
        if user_id is None:
            continue
        ts_ms = parse_ts(ts_raw)
        if ts_ms is None or not (click_window_start <= ts_ms < window_end):
            continue
        if campaign_id is None or campaign_id not in campaign_channel:
            continue
        clicks_by_person[person_of(user_id)].append((ts_ms, click_id, campaign_id))
    del kept_clicks

    # 4-7. Credit and assists, per person.
    # For each session: lo <= click.ts <= start where lo = start - 7 days.
    # Clicks sorted by (ts, click_id). Sessions are already in increasing start order, so a
    # sliding window keeps per-campaign counts of the qualifying clicks.
    for person, sessions in sessions_by_person.items():
        clicks = sorted(clicks_by_person.get(person, ()))
        click_ts = [c[0] for c in clicks]
        campaign_counts = defaultdict(int)
        add_idx = 0     # clicks[:add_idx] have ts <= current session start
        remove_idx = 0  # clicks[:remove_idx] have ts < current session start - 7 days
        for session in sessions:
            start = session["start"]
            lo = start - ATTRIBUTION_WINDOW_MS
            while add_idx < len(clicks) and clicks[add_idx][0] <= start:
                campaign_counts[clicks[add_idx][2]] += 1
                add_idx += 1
            while remove_idx < add_idx and clicks[remove_idx][0] < lo:
                cid = clicks[remove_idx][2]
                campaign_counts[cid] -= 1
                if campaign_counts[cid] == 0:
                    del campaign_counts[cid]
                remove_idx += 1
            if remove_idx < add_idx:
                # Latest ts among qualifying clicks; ties -> smallest click_id, which is the
                # first click with that ts in (ts, click_id) order.
                latest_ts = clicks[add_idx - 1][0]
                first = bisect.bisect_left(click_ts, latest_ts, remove_idx, add_idx)
                session["campaign"] = clicks[first][2]
                session["assists"] = set(campaign_counts)
            else:
                session["campaign"] = ORGANIC
                session["assists"] = set()

    # ---------------- Outputs ----------------
    # user_daily: per (person, day)
    user_daily = {}  # (person, day) -> dict
    campaign_daily = {}  # (campaign, day) -> dict

    def campaign_row(campaign, day):
        key = (campaign, day)
        if key not in campaign_daily:
            campaign_daily[key] = {"sessions": 0, "purchasing_sessions": 0,
                                   "revenue_cents": 0, "assisted_sessions": 0}
        return campaign_daily[key]

    for person, sessions in sessions_by_person.items():
        for session in sessions:
            day = session["start"] // MS_PER_DAY  # days since epoch = date32
            key = (person, day)
            if key not in user_daily:
                user_daily[key] = {"sessions": 0, "events": 0, "revenue_cents": 0,
                                   "category_revenue": defaultdict(int)}
            u = user_daily[key]
            u["sessions"] += 1
            u["events"] += len(session["events"])
            session_revenue = 0
            session_has_purchase = False
            for ts_ms, event_type, product_id, price in session["events"]:
                if event_type == b"purchase":
                    session_has_purchase = True
                    session_revenue += price
                    category = product_category.get(product_id, UNKNOWN_CATEGORY)
                    u["category_revenue"][category] += price
            u["revenue_cents"] += session_revenue

            c = campaign_row(session["campaign"], day)
            c["sessions"] += 1
            c["purchasing_sessions"] += 1 if session_has_purchase else 0
            c["revenue_cents"] += session_revenue
            for assisting in session["assists"]:
                campaign_row(assisting, day)["assisted_sessions"] += 1

    def top_category(category_revenue):
        if not category_revenue:
            return None
        best = max(category_revenue.values())
        return min(cat for cat, rev in category_revenue.items() if rev == best)

    def text(b):
        return b.decode("utf-8")

    ud_keys = sorted(user_daily)
    user_table = pa.table({
        "user_id": pa.array([text(p) for p, _ in ud_keys], pa.string()),
        "day": pa.array([d for _, d in ud_keys], pa.date32()),
        "sessions": pa.array([user_daily[k]["sessions"] for k in ud_keys], pa.int64()),
        "events": pa.array([user_daily[k]["events"] for k in ud_keys], pa.int64()),
        "revenue_cents": pa.array([user_daily[k]["revenue_cents"] for k in ud_keys], pa.int64()),
        "top_category": pa.array(
            [None if (t := top_category(user_daily[k]["category_revenue"])) is None else text(t)
             for k in ud_keys], pa.string()),
    })

    cd_keys = sorted(campaign_daily)
    campaign_table = pa.table({
        "campaign_id": pa.array([text(c) for c, _ in cd_keys], pa.string()),
        "channel": pa.array([text(ORGANIC if c == ORGANIC else campaign_channel[c]) for c, _ in cd_keys],
                            pa.string()),
        "day": pa.array([d for _, d in cd_keys], pa.date32()),
        "sessions": pa.array([campaign_daily[k]["sessions"] for k in cd_keys], pa.int64()),
        "purchasing_sessions": pa.array([campaign_daily[k]["purchasing_sessions"] for k in cd_keys], pa.int64()),
        "revenue_cents": pa.array([campaign_daily[k]["revenue_cents"] for k in cd_keys], pa.int64()),
        "assisted_sessions": pa.array([campaign_daily[k]["assisted_sessions"] for k in cd_keys], pa.int64()),
    })

    for name, table in (("user_daily", user_table), ("campaign_daily", campaign_table)):
        folder = os.path.join(output_dir, name)
        os.makedirs(folder, exist_ok=True)
        pq.write_table(table, os.path.join(folder, "part-0.parquet"))


if __name__ == "__main__":
    main()
