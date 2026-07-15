# parquet-skip-index

Build conservative **experimental** skip-index sidecars for a laboratory variant, to price
custom pruning against native Parquet statistics and Bloom filters. Standalone and offline —
Arrow + Parquet only.

```text
parquet-skip-index build --manifest VARIANT.json --spec SKIP.toml --output DIR [--replace]
```

The spec names the columns to index and the prototype kind:

```toml
[[column]]
name = "team_id"
kind = "value_set"      # zone_map | value_set | prefix_set
budget_bytes = 4096     # value_set / prefix_set payload cap per row group
prefix_len = 8          # prefix_set only
```

Three bounded prototypes:

- **zone_map** — external min/max per row group (any scalar or string column);
- **value_set** — the *complete* distinct value set of a low-NDV row group, capped by an
  explicit byte budget, for equality / `IN`;
- **prefix_set** — the complete set of byte-prefixes at a declared length, for
  `LIKE 'literal%'`.

## The one invariant

Every decision is conservative: a row group is skipped **only** on `NoMatch`, and `NoMatch`
is returned only when the index *proves* the group holds no matching row — an all-null
group, a value outside a complete set, a prefix outside every stored prefix. Any doubt —
an unsupported predicate, a budget-exceeded group, a corrupt or missing payload, a wrong
column type — resolves to `Unknown` (keep), never a skip. This no-false-negative property is
property-tested over thousands of random groups per index kind.

The sidecar (`skip.json` + `payload.bin`) is bound to the exact variant-manifest digest and
every file digest. `parquet-lab-bench --skip-manifest` refuses to credit a sidecar that is
not provably bound, and records its build/storage/coverage cost so a report can weigh it
against the native row-group pruning DataFusion reports separately.

A sidecar is a reversible laboratory carrier, **not** a proposed production format — it
exists to price a candidate before deciding whether a real implementation belongs in
Parquet metadata, an object sidecar, or the catalog.
