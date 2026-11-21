# Shopping Sessions: native C++ pipeline

One static C++17 binary (`src/main.cpp`, no dependencies) in a `FROM scratch` image.
It implements `rules.md` exactly and writes Parquet with a small built-in writer.

## How it works

1. **Links**: all threads parse `links/`, build hash-partitioned ID tables, then run a
   lock-free union-find; each person is named by its byte-wise smallest ID.
2. **Parse**: files are mmapped and split into chunks. A branch-free AVX2 line splitter
   finds the fields of a line from 128 bytes of comma/newline/quote bitmasks. Each event
   is validated, then (through a 16-deep prefetch ring that hides the person-table
   miss) written to a person partition; a 20-byte dedup key goes to an event_id partition.
   Clicks go straight to person partitions.
3. **Dedup**: per event_id partition, keep the copy with the smallest `ingest_ts` and
   tombstone the others in place (invalid winners have no person record).
4. **Sessions**: per person partition, events are bucketed into 30-minute buckets; a
   session break can only fall between consecutive non-empty buckets, so no sort of
   events is needed. Clicks are attributed by a scan (or, for heavy persons, two pointers
   plus merged per-campaign intervals for assists).
5. **Write**: one `user_daily` Parquet file per thread, one `campaign_daily` file.

## Local checks

    make fixtures      # 36/36 edge cases
    make check-dev     # 1M-event dev set vs the published answer
    make bench         # phase timings (PIPE_DEBUG=1)
