# parquet-rewrite

Rewrite a laboratory snapshot under one explicit variant spec into a verified storage
variant. Standalone and offline — Arrow + Parquet only.

```text
parquet-rewrite --manifest SNAPSHOT.json --spec VARIANT.toml --output DIR [--replace]
```

The spec (TOML) exposes only the pinned Parquet 58.3 controls the matrix varies:

```toml
label = "pages-rowgroup-32k"
row_group_rows = 32768
key_boundary_flush = true
write_batch_rows = 1024
data_page_bytes = 65536
dictionary_page_bytes = 1048576
statistics = "page"           # none | chunk | page
offset_index = true
compression = "zstd(3)"       # zstd(N) | lz4_raw | snappy | uncompressed

[[column]]
name = "timestamp"
encoding = "delta_binary_packed"   # plain | delta_* | byte_stream_split | rle
dictionary = false
# compression = "zstd(6)"
# bloom_fpp = 0.01
# bloom_ndv = 100000
# physical_type = "int32"          # int8/16/32/64 | timestamp_ms | date32
```

## Guarantees

- **Correctness is absolute.** The variant manifest is published only after the logical
  fingerprint over the rewritten rows equals the snapshot's; a changed logical value
  invalidates the entire variant, and nothing is published.
- **Requested ≠ resolved.** Every column's *resolved* encodings, dictionary use, and codec
  are read back from the output footers — dictionary fallback shows there — never trusted
  from the request.
- **Lossless projection only.** An integer narrowing is refused unless every value fits; a
  non-integer cannot be reinterpreted as an integer, a float is never narrowed, and a
  `Date32` is only allowed for a column *declared* a date, never guessed from an integer.
- **Membership preserved.** File count and row order are unchanged; only row-group/page
  boundaries and the physical schema may change.
- Output is written to a sibling temp tree and renamed into place atomically; `--replace`
  is required to overwrite.

The **product control is never rewritten** — the original snapshot bytes are the control.
`bench/config/parquet-lab/product-control.toml` documents the product's policy for
comparison but the runner never substitutes freshly encoded bytes for the control arm.
