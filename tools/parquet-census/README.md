# parquet-census

Inspect the physical Parquet structure of a laboratory snapshot (or variant), by file,
row group, and column chunk. Standalone and offline — Arrow + Parquet only, no
DataFusion, no Ukiel service, no object store.

```text
parquet-census --manifest FILE --report FILE [--scan-columns] [--replace]
```

The JSON report carries raw per-file / row-group / column-chunk records plus per-column
aggregates. Per column chunk it reads, **from the footer**: physical and Parquet logical
type, the declared laboratory logical type, compression, the encodings *actually written*
(dictionary fallback shows here), dictionary-page presence, compressed/uncompressed
bytes, value and null counts, min/max exactness (truncation), Bloom-filter offset/length,
and page/offset-index presence. Per file it reports a metadata-overhead ceiling (file
bytes not accounted for by compressed column data).

Every metric carries a **provenance**: `footer`, `page_index`, `scanned`, or
`unavailable`. Where the footer cannot supply NDV or value width, `--scan-columns`
streams the column and labels the result `scanned` — the tool never invents a distinct
count from min/max.

The census verifies each file against its parent manifest's digest before reading it, so
a report always names the exact bytes it measured. Reports are written atomically and
refuse to overwrite without `--replace`.
