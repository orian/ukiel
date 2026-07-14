-- Issue 0014: let the catalog reject a part it can *prove* does not hold the
-- tenant, instead of shipping the row so the client can prove it.
--
-- `live_parts_pruned` filters on the key *range* a part declares, which is a
-- superset: a packed file whose keys span 100..4500 "contains" tenant 3000 even
-- when it holds none of tenant 3000's rows — the ordinary case, because a file
-- holds the tenants active in the window it covers, not the tenants its endpoints
-- bracket. Measured on a product-shaped fixture (one hypertable, 100k tenants,
-- heavily packed): the median tenant matched **20,000 parts by range and 7 by key
-- set**. Plan 16 measured the same effect on real data as 108 planned files
-- falling to 7 — and closed it in the *provider*, after the catalog had shipped
-- all 20,000 rows, each carrying the bitmap that proved it was not needed.
--
-- Two things this deliberately is NOT, both rejected on measurements:
--
--   * NOT an inverted index over the keys. A GIN index on a BIGINT[] of the key
--     set reads beautifully and writes catastrophically — one entry per key per
--     part took commits from **7,815/s to 2,254/s** and PostgreSQL from 4.4 to
--     11.7 of 12 cores, because every insert and every compaction REPLACE churns
--     an entry for each key. That is the (part, key) expansion, hiding inside an
--     index.
--   * NOT the exact key set in the index payload. Index tuples cap at ~2704
--     bytes; a thousand keys is 8 KB and the insert would simply fail.
--
-- So: a small, fixed-size, *one-per-part* Bloom filter, carried in the INCLUDE
-- payload of the range index. The scan rejects parts **without touching the heap**
-- (`Index Only Scan`, `Heap Fetches: 0`) and fetches only the survivors.
--
-- The error is one-directional: the filter never says "absent" for a key that is
-- present. It may say "maybe" for one that is absent, which keeps a part that could
-- have been skipped — costing a little pruning, never a row. NULL, an unknown size,
-- a malformed blob: all keep. Pruning may only remove what it can prove is absent,
-- which is plan 16's rule, unweakened.
--
-- Note what is *not* here: no hash function, and no membership function. Sizing the
-- blob per part meant the bit a key lands on depended on the part, so only the
-- database could compute it — and a plpgsql call per candidate row made the query
-- **slower than the scan it replaced** (16.5 ms against 12.8 ms) despite reading a
-- sixth of the buffers. The size is fixed instead, so the probe positions depend on
-- the key alone: `ukiel_core::keyfilter` computes them once per query and binds
-- them, and SQL is left with four `get_byte(...) & mask <> 0` tests. One
-- implementation of the layout, in Rust; the database is handed bit offsets.
--
-- The exact key set is NOT stored. Storing it is what made writes collapse.
-- `column_stats.packing_keys` (plan 16) remains the exact record, read by the
-- provider; the filter is the part of it the *catalog* can act on.
--
-- A part with a single key gets no filter: its `packing_key_min` and
-- `packing_key_max` already *are* its key set, so the range predicate is exact for
-- it. Dedicated (unpacked) parts are all of that shape.
ALTER TABLE parts ADD COLUMN key_filter BYTEA;

-- The range index gains the filter as an INCLUDE payload, so the scan can reject a
-- part from the index alone. One entry per part — exactly what the old index cost —
-- so the write path keeps its shape. `id` rides along so the candidate pass is
-- index-only and the heap is touched only for survivors.
DROP INDEX parts_live_idx;
CREATE INDEX parts_live_idx
    ON parts (hypertable_id, packing_key_min, packing_key_max)
    INCLUDE (key_filter, id)
    WHERE deleted_by_commit IS NULL;
