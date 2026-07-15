# Parquet storage screen — analysis

- seed: `47`  repetitions: 2
- control: 1288132743 compressed bytes, 292.361 ms warm median
- suite digest: `d830a5c560ae608757d19e4af91bfea055e857686d4e4eda157fe7d95b02759f`  control digest: `ecbba18b797bc6dc39504e9912cce01c26b5810b0c9b07634b92be18885ee4e1`
- noise bands: size 5.0% (threshold), time 6.9%

## Formulas

- `suite_total` = T_i = sum_q warm_ms[q][i]
- `time_band` = 3 * MAD(control T_i pool) / median(control T_i pool)
- `size_band` = 0.05 fixed decision threshold

## Control drift

| rep | start ms | end ms | drift % | exceeds |
|---|---|---|---|---|
| 0 | 287.146 | 286.538 | 0.21 | False |
| 1 | 296.159 | 291.636 | 1.55 | False |

## Variants

| variant | size Δ% | time Δ% | per-rep time Δ% | classification |
|---|---|---|---|---|
| blooms-distinct-id-01 | 0.74 | -0.7 | {0: 1.59, 1: -1.59} | no-demonstrated-change |
| compression-lz4-raw | 71.19 | -4.86 | {0: -3.06, 1: -5.33} | dominated |
| compression-zstd-1 | -0.04 | -2.2 | {0: -1.41, 1: -1.47} | no-demonstrated-change |
| compression-zstd-6 | -12.08 | -2.45 | {0: -1.1, 1: -2.51} | pareto-candidate |
| encodings-strings-no-dict | 14.56 | 55.38 | {0: 60.34, 1: 53.65} | dominated |
| encodings-ts-plain | 1.67 | 1.34 | {0: 2.94, 1: 2.67} | no-demonstrated-change |
| pages-page-64k | 1.42 | -0.97 | {0: 1.33, 1: -3.42} | no-demonstrated-change |
| pages-rowgroup-32k | 4.32 | 0.99 | {0: 1.58, 1: 2.46} | no-demonstrated-change |
| pages-rowgroup-512k | 0.15 | 1.93 | {0: 2.88, 1: 2.47} | no-demonstrated-change |
| types-team-int32 | 0.74 | -0.88 | {0: 0.66, 1: 0.03} | no-demonstrated-change |
| types-ts-timestamp | 6.53 | 2.34 | {0: 4.35, 1: 1.39} | dominated |

## Scheduled order

`rep0/order-000-product-control → rep0/order-001-types-team-int32 → rep0/order-002-pages-rowgroup-32k → rep0/order-003-types-ts-timestamp → rep0/order-004-encodings-strings-no-dict → rep0/order-005-blooms-distinct-id-01 → rep0/order-006-pages-page-64k → rep0/order-007-encodings-ts-plain → rep0/order-008-compression-zstd-6 → rep0/order-009-compression-zstd-1 → rep0/order-010-pages-rowgroup-512k → rep0/order-011-compression-lz4-raw → rep0/order-012-product-control → rep1/order-000-product-control → rep1/order-001-types-ts-timestamp → rep1/order-002-blooms-distinct-id-01 → rep1/order-003-pages-rowgroup-32k → rep1/order-004-encodings-strings-no-dict → rep1/order-005-pages-rowgroup-512k → rep1/order-006-compression-zstd-6 → rep1/order-007-compression-zstd-1 → rep1/order-008-encodings-ts-plain → rep1/order-009-pages-page-64k → rep1/order-010-compression-lz4-raw → rep1/order-011-types-team-int32 → rep1/order-012-product-control`
