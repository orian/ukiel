//! Parse, validate, and summarize the anonymized workload profile in
//! `docs/prod-info/`.
//!
//! Four files, tracked in the repository, never fetched from a network:
//!
//! * `table.json`         — table-level metadata (provenance).
//! * `show-create.sql`    — the DDL (provenance; hashed, not parsed — see below).
//! * `part-geometry.jsonl` — one record per observed part.
//! * `tenant-fanout.jsonl` — one record per sampled tenant.
//!
//! `show-create.sql` is hashed and never interpreted. It is ClickHouse DDL, and
//! Ukiel supports neither its type system nor its materialized-column expressions;
//! a parser for it would be a parser for a language we do not implement, and its
//! only real job here is to pin *which* schema the profile came from.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use prod_synth_contract::{FileDigest, Quantiles, SourceSummary};

#[derive(Debug, thiserror::Error)]
pub enum ProfileError {
    #[error("{path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    /// Always names the file *and* the record. A profile error that says only
    /// "invalid number" leaves someone bisecting a 500-line JSONL by hand.
    #[error("{file}:{line}: {message}")]
    Record {
        file: String,
        line: usize,
        message: String,
    },
    #[error("{file}: {message}")]
    File { file: String, message: String },
}

type Result<T> = std::result::Result<T, ProfileError>;

/// Table-level metadata. Provenance only: none of it becomes a generated value.
#[derive(Debug, Clone, PartialEq)]
pub struct TableObservation {
    pub clickhouse_version: String,
    pub engine: String,
    pub partition_key: String,
    pub sorting_key: String,
    pub total_rows: i64,
    pub total_bytes: i64,
}

/// One observed source part.
///
/// The physical fields (`physical_rows`, `bytes_on_disk`) describe the *whole*
/// part; the geometry fields (`observed_rows`, `distinct_keys`, `key_span`)
/// describe only the rows that matched the 14-day extraction filter. Conflating
/// them is the easiest way to build a fixture that quietly lies, so they stay
/// separate all the way through.
#[derive(Debug, Clone, PartialEq)]
pub struct PartObservation {
    /// Deterministic 64-bit hashes of the ClickHouse part/partition names. They are
    /// **u64**: the source hashes fill the whole 64-bit domain, and reading them as
    /// i64 rejects the half of the file above 2^63. Anonymous join keys within this
    /// capture, never an Ukiel identity.
    pub part_id: u64,
    pub partition_id: u64,
    pub level: i64,
    pub part_type: String,
    pub physical_rows: i64,
    pub bytes_on_disk: i64,
    pub data_compressed_bytes: i64,
    pub data_uncompressed_bytes: i64,
    pub observed_rows: i64,
    pub distinct_keys: i64,
    pub key_span: i64,
    pub key_density: f64,
    pub time_span_ms: i64,
}

/// One sampled tenant. The four fields are kept together everywhere downstream:
/// activity and fanout are correlated, and averaging them independently is what
/// erases the behaviour the benchmark exists to measure.
#[derive(Debug, Clone, PartialEq)]
pub struct TenantObservation {
    pub tenant_rows: i64,
    pub exact_parts: i64,
    pub range_parts: i64,
    pub range_overfetch: f64,
}

#[derive(Debug, Clone)]
pub struct ProductionProfile {
    pub table: TableObservation,
    pub parts: Vec<PartObservation>,
    pub tenants: Vec<TenantObservation>,
    /// Digests of the raw bytes of all four files, in a stable order.
    pub files: Vec<FileDigest>,
}

/// The quantile definition, pinned.
///
/// The value at sorted rank `round(p * (n - 1))`, halves rounding up. This is not
/// an arbitrary pick: it is the only definition that reproduces *every* figure the
/// plan quotes from the committed profile — exact-parts 11/43, range 63/63,
/// overfetch 5.727/63, tenant-rows 127/12493/439457, density
/// 0.00625175/0.10264053/0.13749292. Nearest-rank and linear interpolation each
/// reproduce some and miss others.
///
/// So it is written here rather than delegated to whichever statistics crate a
/// future reader reaches for, because a fidelity gate stated in quantiles is only
/// as reproducible as its quantile definition.
pub fn quantile(sorted: &[f64], p: f64) -> f64 {
    debug_assert!(sorted.windows(2).all(|w| w[0] <= w[1]), "must be sorted");
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = (p * (sorted.len() - 1) as f64 + 0.5) as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn quantiles(values: &mut [f64]) -> Quantiles {
    values.sort_by(|a, b| a.partial_cmp(b).expect("no NaN: validated at parse"));
    Quantiles {
        p10: quantile(values, 0.10),
        p50: quantile(values, 0.50),
        p90: quantile(values, 0.90),
        p99: quantile(values, 0.99),
    }
}

impl ProductionProfile {
    /// Load and validate all four files from a profile directory.
    pub fn load(dir: &Path) -> Result<Self> {
        let table_path = dir.join("table.json");
        let ddl_path = dir.join("show-create.sql");
        let parts_path = dir.join("part-geometry.jsonl");
        let tenants_path = dir.join("tenant-fanout.jsonl");

        let table_bytes = read(&table_path)?;
        let ddl_bytes = read(&ddl_path)?;
        let parts_bytes = read(&parts_path)?;
        let tenants_bytes = read(&tenants_path)?;

        let table = parse_table(&table_path, &table_bytes)?;
        let parts = parse_parts(&parts_path, &parts_bytes)?;
        let tenants = parse_tenants(&tenants_path, &tenants_bytes)?;

        if parts.is_empty() {
            return Err(ProfileError::File {
                file: name(&parts_path),
                message: "no part records: there is no geometry to reproduce".into(),
            });
        }
        if tenants.is_empty() {
            return Err(ProfileError::File {
                file: name(&tenants_path),
                message: "no tenant records: there is no activity skew to reproduce".into(),
            });
        }

        // Digests over the raw bytes as read, not over the parsed structs. The
        // point is to name the exact file that was compiled; a round-trip through
        // serde would launder precisely the corruption this is here to catch.
        let files = vec![
            FileDigest::of("table.json", &table_bytes),
            FileDigest::of("show-create.sql", &ddl_bytes),
            FileDigest::of("part-geometry.jsonl", &parts_bytes),
            FileDigest::of("tenant-fanout.jsonl", &tenants_bytes),
        ];

        Ok(ProductionProfile {
            table,
            parts,
            tenants,
            files,
        })
    }

    /// `sum(part.distinct_keys)` — the total part-to-tenant memberships observed.
    pub fn memberships(&self) -> i64 {
        self.parts.iter().map(|p| p.distinct_keys).sum()
    }

    pub fn mean_exact_parts(&self) -> f64 {
        let total: i64 = self.tenants.iter().map(|t| t.exact_parts).sum();
        total as f64 / self.tenants.len() as f64
    }

    /// The active-tenant estimate: `memberships / mean(exact_parts)`.
    ///
    /// Two independent margins of the same bipartite graph. Every membership is
    /// counted once from the part side (`sum(distinct_keys)`) and, on average,
    /// `mean(exact_parts)` times per tenant from the tenant side — so their ratio
    /// is the tenant count, *if* the tenant sample is uniform over active tenants
    /// and covers the same part scope.
    ///
    /// It is an estimate. It is not a discovered production total, and nothing
    /// downstream may present it as one.
    pub fn estimated_active_tenants(&self) -> u64 {
        (self.memberships() as f64 / self.mean_exact_parts()).round() as u64
    }

    /// Parts whose key range is wider than a point.
    ///
    /// The ceiling on any tenant's `range_parts`: a part holding one key has range
    /// `[k, k]` and can only ever be a range candidate for tenant `k` itself. See
    /// [`Self::scope_inconsistency`].
    pub fn multi_key_parts(&self) -> u64 {
        self.parts.iter().filter(|p| p.distinct_keys > 1).count() as u64
    }

    /// The two capture files disagree about the part population — reported, never
    /// silently absorbed.
    ///
    /// `tenant-fanout.jsonl` says the median tenant is a range candidate in 63
    /// parts. `part-geometry.jsonl` contains only 61 parts whose range is wider
    /// than a point. A tenant cannot be inside 63 ranges when only 61 ranges have
    /// any width, so the two files were captured against different part scopes —
    /// exactly the hazard `docs/prod-info/README.md` warns about ("treat the JSONL
    /// files as distribution samples unless the original query scope is available").
    ///
    /// This is not fixable by generating harder: no graph over 61 multi-key parts
    /// puts a tenant in 63 ranges. It changes what a saturation gate can honestly
    /// assert, so it is surfaced here and printed in the profile summary rather
    /// than discovered later as a mysterious 0.2-part gate failure.
    pub fn scope_inconsistency(&self) -> Option<String> {
        let mut rp: Vec<f64> = self.tenants.iter().map(|t| t.range_parts as f64).collect();
        let max_range = rp.iter().cloned().fold(0.0f64, f64::max) as u64;
        let multi = self.multi_key_parts();
        if max_range <= multi {
            return None;
        }
        rp.sort_by(|a, b| a.partial_cmp(b).unwrap());
        Some(format!(
            "the two capture files disagree about the part population: tenant-fanout.jsonl reports \
             up to {max_range} range candidates per tenant (p50 {p50}), but part-geometry.jsonl \
             holds only {multi} parts whose key range is wider than a single point — and a \
             single-key part can only be a range candidate for its own key. The files were \
             captured at different part scopes. Range saturation can therefore only be asserted \
             against the achievable maximum of {multi}, not against {max_range}.",
            p50 = quantile(&rp, 0.5) as u64,
        ))
    }

    pub fn summary(&self) -> SourceSummary {
        let mut exact: Vec<f64> = self.tenants.iter().map(|t| t.exact_parts as f64).collect();
        let mut range: Vec<f64> = self.tenants.iter().map(|t| t.range_parts as f64).collect();
        let mut over: Vec<f64> = self.tenants.iter().map(|t| t.range_overfetch).collect();
        let mut rows: Vec<f64> = self.tenants.iter().map(|t| t.tenant_rows as f64).collect();
        let mut dens: Vec<f64> = self.parts.iter().map(|p| p.key_density).collect();

        SourceSummary {
            parts: self.parts.len() as u64,
            tenant_samples: self.tenants.len() as u64,
            memberships: self.memberships() as u64,
            multi_key_parts: self.multi_key_parts(),
            mean_exact_parts: self.mean_exact_parts(),
            estimated_active_tenants: self.estimated_active_tenants(),
            exact_parts: quantiles(&mut exact),
            range_parts: quantiles(&mut range),
            range_overfetch: quantiles(&mut over),
            tenant_rows: quantiles(&mut rows),
            key_density: quantiles(&mut dens),
            max_key_span: self.parts.iter().map(|p| p.key_span).max().unwrap_or(1) as u64,
            observed_rows: self.parts.iter().map(|p| p.observed_rows).sum::<i64>() as u64,
        }
    }

    /// The assumptions the estimate rests on, carried into every artifact so a
    /// reader never has to go and find the plan to know what they are holding.
    pub fn assumptions(&self) -> Vec<String> {
        let mut a = vec![
            format!(
                "estimated_active_tenants = sum(part.distinct_keys) / mean(tenant.exact_parts) \
                 = {} / {:.6} = {}. An ESTIMATE from two margins of one bipartite graph, valid \
                 only if the tenant sample is uniform over active tenants and covers the same \
                 part scope. It is not a discovered production total.",
                self.memberships(),
                self.mean_exact_parts(),
                self.estimated_active_tenants()
            ),
            "The capture is one shard of ten, over a 14-day window. It describes the active \
             working set, not full retention, and its per-part sizes, densities and ratios are \
             not multipliable by ten."
                .to_string(),
            "The capture contains no real tenant IDs, no part-to-tenant matrix, no per-event \
             distributions, no property cardinalities, and no query frequencies. Everything in \
             those dimensions is declared synthetic (see value_model) and claims nothing about \
             production."
                .to_string(),
            "ClickHouse bytes, part types and merge levels are recorded as provenance only. They \
             never become a generated Ukiel part's size or compaction level: a ClickHouse merge \
             level is not a rung on Ukiel's ladder, and copying it across would give a real \
             number a false meaning."
                .to_string(),
        ];
        if let Some(note) = self.scope_inconsistency() {
            a.push(note);
        }
        a
    }
}

fn name(p: &Path) -> String {
    p.file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| p.display().to_string())
}

fn read(path: &PathBuf) -> Result<Vec<u8>> {
    std::fs::read(path).map_err(|source| ProfileError::Io {
        path: path.display().to_string(),
        source,
    })
}

fn parse_table(path: &Path, bytes: &[u8]) -> Result<TableObservation> {
    let v: serde_json::Value = serde_json::from_slice(bytes).map_err(|e| ProfileError::File {
        file: name(path),
        message: format!("invalid JSON: {e}"),
    })?;
    let s = |k: &str| -> Result<String> {
        v.get(k)
            .and_then(|x| x.as_str())
            .map(String::from)
            .ok_or_else(|| ProfileError::File {
                file: name(path),
                message: format!("missing string field '{k}'"),
            })
    };
    let i = |k: &str| -> Result<i64> {
        v.get(k)
            .and_then(|x| x.as_i64())
            .ok_or_else(|| ProfileError::File {
                file: name(path),
                message: format!("missing integer field '{k}'"),
            })
    };
    Ok(TableObservation {
        clickhouse_version: s("clickhouse_version")?,
        engine: s("engine")?,
        partition_key: s("partition_key")?,
        sorting_key: s("sorting_key")?,
        total_rows: i("total_rows")?,
        total_bytes: i("total_bytes")?,
    })
}

/// One JSONL record, with the field accessors that carry the file/line into
/// every error. Streaming, line by line — the format was chosen so the files need
/// never be held whole, and honouring that here keeps the choice meaningful even
/// though today's files are small.
struct Record<'a> {
    file: &'a str,
    line: usize,
    value: serde_json::Value,
}

impl Record<'_> {
    fn err(&self, message: impl Into<String>) -> ProfileError {
        ProfileError::Record {
            file: self.file.to_string(),
            line: self.line,
            message: message.into(),
        }
    }

    fn int(&self, key: &str) -> Result<i64> {
        let v = self
            .value
            .get(key)
            .ok_or_else(|| self.err(format!("missing field '{key}'")))?;
        let n = v
            .as_i64()
            .ok_or_else(|| self.err(format!("field '{key}' is not an integer: {v}")))?;
        if n < 0 {
            return Err(self.err(format!("field '{key}' is negative: {n}")));
        }
        Ok(n)
    }

    fn float(&self, key: &str) -> Result<f64> {
        let v = self
            .value
            .get(key)
            .ok_or_else(|| self.err(format!("missing field '{key}'")))?;
        let n = v
            .as_f64()
            .ok_or_else(|| self.err(format!("field '{key}' is not a number: {v}")))?;
        if !n.is_finite() {
            return Err(self.err(format!("field '{key}' is not finite: {n}")));
        }
        if n < 0.0 {
            return Err(self.err(format!("field '{key}' is negative: {n}")));
        }
        Ok(n)
    }

    fn text(&self, key: &str) -> Result<String> {
        self.value
            .get(key)
            .and_then(|v| v.as_str())
            .map(String::from)
            .ok_or_else(|| self.err(format!("missing string field '{key}'")))
    }
}

fn records<'a>(file: &'a str, bytes: &'a [u8]) -> Result<Vec<Record<'a>>> {
    let text = std::str::from_utf8(bytes).map_err(|e| ProfileError::File {
        file: file.to_string(),
        message: format!("not UTF-8: {e}"),
    })?;
    let mut out = Vec::new();
    for (i, line) in text.lines().enumerate() {
        let line_no = i + 1;
        if line.trim().is_empty() {
            continue;
        }
        let value: serde_json::Value =
            serde_json::from_str(line).map_err(|e| ProfileError::Record {
                file: file.to_string(),
                line: line_no,
                message: format!("invalid JSON: {e}"),
            })?;
        if !value.is_object() {
            return Err(ProfileError::Record {
                file: file.to_string(),
                line: line_no,
                message: "expected a JSON object".into(),
            });
        }
        out.push(Record {
            file,
            line: line_no,
            value,
        });
    }
    Ok(out)
}

/// The source's *derived* fields are recomputed and compared, never trusted: a
/// record whose density does not match its own keys and span is a record we cannot
/// reason about, and a fixture built on it would be built on sand.
///
/// The tolerances are exactly the source's own rounding, and no wider. It records
/// `range_overfetch` to three decimals and `key_density` to eight, so half an ulp
/// at those precisions — 5e-4 and 5e-9 — is the most a truthful record can differ
/// by. Measured across the committed profile, the worst observed errors are
/// 5.0e-4 and 4.8e-9: right at the bound, which is what rounding looks like. A
/// looser tolerance here would start accepting records that are simply wrong.
///
/// (The epsilon absorbs float representation noise at the boundary itself.)
const OVERFETCH_TOLERANCE: f64 = 5e-4 + 1e-12;
const DENSITY_TOLERANCE: f64 = 5e-9 + 1e-15;

fn parse_parts(path: &Path, bytes: &[u8]) -> Result<Vec<PartObservation>> {
    let file = name(path);
    let mut out = Vec::new();
    let mut seen = BTreeMap::new();
    for r in records(&file, bytes)? {
        let part_id = r
            .value
            .get("part_id")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| r.err("missing u64 field 'part_id'"))?;
        let partition_id = r
            .value
            .get("partition_id")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| r.err("missing u64 field 'partition_id'"))?;

        let physical_rows = r.int("physical_rows")?;
        let observed_rows = r.int("observed_rows")?;
        let distinct_keys = r.int("distinct_keys")?;
        let key_span = r.int("key_span")?;
        let key_density = r.float("key_density")?;

        if observed_rows > physical_rows {
            return Err(r.err(format!(
                "observed_rows ({observed_rows}) exceeds physical_rows ({physical_rows}): the \
                 filtered rows cannot outnumber the part's own"
            )));
        }
        if distinct_keys < 1 {
            return Err(r.err("distinct_keys must be at least 1"));
        }
        if key_span < distinct_keys {
            return Err(r.err(format!(
                "key_span ({key_span}) is smaller than distinct_keys ({distinct_keys}): a range \
                 cannot hold more keys than it spans"
            )));
        }
        let expected = distinct_keys as f64 / key_span as f64;
        if (expected - key_density).abs() > DENSITY_TOLERANCE {
            return Err(r.err(format!(
                "key_density ({key_density}) does not equal distinct_keys/key_span \
                 ({distinct_keys}/{key_span} = {expected})"
            )));
        }
        if let Some(prev) = seen.insert(part_id, r.line) {
            return Err(r.err(format!(
                "duplicate part_id {part_id} (first seen at line {prev})"
            )));
        }

        out.push(PartObservation {
            part_id,
            partition_id,
            level: r.int("level")?,
            part_type: r.text("part_type")?,
            physical_rows,
            bytes_on_disk: r.int("bytes_on_disk")?,
            data_compressed_bytes: r.int("data_compressed_bytes")?,
            data_uncompressed_bytes: r.int("data_uncompressed_bytes")?,
            observed_rows,
            distinct_keys,
            key_span,
            key_density,
            time_span_ms: r.int("time_span_ms")?,
        });
    }
    Ok(out)
}

fn parse_tenants(path: &Path, bytes: &[u8]) -> Result<Vec<TenantObservation>> {
    let file = name(path);
    let mut out = Vec::new();
    for r in records(&file, bytes)? {
        let tenant_rows = r.int("tenant_rows")?;
        let exact_parts = r.int("exact_parts")?;
        let range_parts = r.int("range_parts")?;
        let range_overfetch = r.float("range_overfetch")?;

        if exact_parts < 1 {
            return Err(r.err("exact_parts must be at least 1: a sampled tenant is in some part"));
        }
        if range_parts < exact_parts {
            return Err(r.err(format!(
                "range_parts ({range_parts}) is smaller than exact_parts ({exact_parts}): every \
                 part that holds the tenant also brackets it, so the range set is a superset"
            )));
        }
        let expected = range_parts as f64 / exact_parts as f64;
        if (expected - range_overfetch).abs() > OVERFETCH_TOLERANCE {
            return Err(r.err(format!(
                "range_overfetch ({range_overfetch}) does not equal range_parts/exact_parts \
                 ({range_parts}/{exact_parts} = {expected})"
            )));
        }
        out.push(TenantObservation {
            tenant_rows,
            exact_parts,
            range_parts,
            range_overfetch,
        });
    }
    Ok(out)
}
