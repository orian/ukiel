//! `manifest.json` and `topology.json`: the two files the pipeline speaks through.
//!
//! The split is deliberate. The **manifest** is identity — what was generated,
//! from what, with which seed, into which bytes. It stays small enough to read,
//! diff, and paste into a report. The **topology** is the membership graph, which
//! is large (a baseline fixture holds ~168k tenant-part edges) and is only needed
//! by a tool that intends to walk it.
//!
//! The manifest names the topology and carries its digest, so the pair cannot be
//! silently mismatched: a topology from another seed is a digest error, not a
//! subtly wrong benchmark.

use serde::{Deserialize, Serialize};

use crate::{Digest, FileDigest};

/// The tier a fixture was generated at.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Tier {
    /// 1,000 tenants / 100k rows: parser, generator, and load correctness.
    Smoke,
    /// 10,000 tenants / 5M rows: the query and catalog baseline, and the only
    /// tier the fidelity gates are asserted against.
    Baseline,
    /// The estimated source tenant count / 100M rows: an optional cardinality
    /// confirmation. Not an acceptance gate, and emphatically not a claim to
    /// reproduce the source's observed row count.
    Shape,
}

impl Tier {
    /// The versioned defaults. `--tenants`, `--rows-per-shard`, `--shards` and
    /// `--seed` override them, and every override is recorded in the manifest.
    ///
    /// `Shape`'s tenant count is `None` because it is not a constant: it is the
    /// active-tenant count *estimated from the profile*, so it moves if the
    /// profile does. Resolving it anywhere but against the profile in hand would
    /// bake a stale number into the binary.
    pub fn default_tenants(self) -> Option<u64> {
        match self {
            Tier::Smoke => Some(1_000),
            Tier::Baseline => Some(10_000),
            Tier::Shape => None,
        }
    }

    /// Rows per shard.
    ///
    /// **`Baseline` is 30M, not the 5M the plan tiers it at, and that is a
    /// correction rather than a preference.** The two cannot both hold:
    ///
    /// A baseline topology has ~168k tenant-part memberships, and every membership
    /// must carry at least one row — otherwise the part does not contain the tenant
    /// the catalog says it contains, and the key filter under test would be
    /// answering about a graph the files do not have. At 5M rows that floor, not
    /// the activity weights, decides the row count of **70% of tenants**: the median
    /// tenant's weight-proportional share is 2 rows while its membership floor is 11.
    /// The measured p90/p50 activity ratio collapses to 18 against the source's 98,
    /// and `heavy`/`median`/`light` query classes stop meaning anything.
    ///
    /// The budget at which the median tenant's weight-share reaches its floor is
    /// 26.4M rows; 30M clears it, and the skew gates pass there (p90/p50 80.4 vs 98.4,
    /// p99/p50 3141 vs 3460). The source itself carries 1,871 rows per membership —
    /// 30M gives 178, still far short, but enough for the distribution to be the
    /// weights' rather than the floor's.
    ///
    /// The cost is a ~1.3GB fixture instead of ~220MB. The alternative is a fixture
    /// that reproduces production's *geometry* perfectly and its *activity skew* not
    /// at all, which is half of what plan 45 exists to do.
    pub fn default_rows_per_shard(self) -> u64 {
        match self {
            Tier::Smoke => 100_000,
            Tier::Baseline => 30_000_000,
            Tier::Shape => 100_000_000,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Tier::Smoke => "smoke",
            Tier::Baseline => "baseline",
            Tier::Shape => "shape",
        }
    }
}

impl std::str::FromStr for Tier {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "smoke" => Ok(Tier::Smoke),
            "baseline" => Ok(Tier::Baseline),
            "shape" => Ok(Tier::Shape),
            other => Err(format!(
                "unknown tier '{other}' (expected smoke, baseline, or shape)"
            )),
        }
    }
}

/// Which generator produced this, and with what randomness.
///
/// The RNG algorithm is named in the artifact rather than assumed, because
/// "deterministic given the seed" is only a useful promise if the reader can tell
/// *which* determinism it is getting. A generator that swapped its PRNG without
/// changing this string would produce a different fixture under the same seed and
/// call it the same fixture.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Generator {
    pub name: String,
    pub version: String,
    pub rng: String,
}

/// Everything that, together with the profile, determines the output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GenerationConfig {
    pub tier: Tier,
    pub tenants: u64,
    pub rows_per_shard: u64,
    pub shards: u32,
    pub seed: u64,
    /// Which values came from `--flags` rather than the tier defaults. Recorded
    /// so a report can never claim to be a stock tier when it is not.
    pub overrides: Vec<String>,
}

/// A quantile triple/quad, at the ranks the fidelity gates are stated in.
///
/// One quantile definition, everywhere, named in the artifact: the value at
/// sorted rank `round(p * (n - 1))`. It reproduces every figure quoted in the
/// plan from the committed profile exactly, and any other definition does not —
/// so it is pinned here rather than left to whichever library a reader reaches
/// for.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Quantiles {
    pub p10: f64,
    pub p50: f64,
    pub p90: f64,
    pub p99: f64,
}

/// What the source profile said, as the generator read it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SourceSummary {
    pub parts: u64,
    pub tenant_samples: u64,
    /// `sum(part.distinct_keys)` — the total part-to-tenant memberships observed.
    pub memberships: u64,
    /// Parts whose key range is wider than a single point. Only these can put a
    /// tenant other than their own into a range candidate set, so this — not the
    /// part count — is the ceiling on any tenant's `range_parts`.
    pub multi_key_parts: u64,
    pub mean_exact_parts: f64,
    /// `memberships / mean_exact_parts`, rounded.
    ///
    /// An **estimate**, not a discovered production total. It is only meaningful
    /// if the tenant sample is uniform over active tenants and covers the same
    /// part scope; see [`SourceProfile::assumptions`].
    pub estimated_active_tenants: u64,
    pub exact_parts: Quantiles,
    pub range_parts: Quantiles,
    pub range_overfetch: Quantiles,
    pub tenant_rows: Quantiles,
    pub key_density: Quantiles,
    pub max_key_span: u64,
    pub observed_rows: u64,
}

/// The profile the artifact was compiled from, by digest.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SourceProfile {
    /// Digests of the raw bytes of each input file, so a manifest names the exact
    /// profile it read — not a reserialized approximation of it.
    pub files: Vec<FileDigest>,
    pub summary: SourceSummary,
    /// The estimate's formula and the assumptions it rests on, spelled out in the
    /// artifact so no reader has to go and find the plan.
    pub assumptions: Vec<String>,
}

/// What the generator actually produced, measured from the finished graph and the
/// finished files — never from a scaled source counter.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GeneratedSummary {
    pub tenants: u64,
    pub parts: u64,
    pub multi_key_parts: u64,
    pub memberships: u64,
    pub rows: u64,
    pub bytes: u64,
    /// The key universe synthetic tenant IDs were drawn from, scaled from the
    /// source's maximum key span. Sparsity is a *consequence* of drawing IDs from
    /// this and then reading each part's range off its actual members — it is
    /// never assigned.
    pub key_universe: u64,
    /// Tenants whose degree the feasibility repair changed, and why it had to.
    pub degree_repairs: u64,
    /// Tenants whose row count is decided by the one-row-per-membership floor
    /// rather than by their activity weight.
    ///
    /// The single number that explains whether the activity skew is real. A tenant
    /// in 11 parts must carry at least 11 rows; if its weight only affords 2, the
    /// floor wins and its row count says nothing about how active it is. When this
    /// is most of the population, `tenant_rows` quantiles are measuring the degree
    /// distribution in disguise, and every ratio taken against them is an artifact.
    /// See `Tier::default_rows_per_shard`.
    pub floor_pinned_tenants: u64,
    pub exact_parts: Quantiles,
    pub range_parts: Quantiles,
    pub range_overfetch: Quantiles,
    pub tenant_rows: Quantiles,
    pub key_density: Quantiles,
    /// Rows whose sampled time span was clipped to the fixture window.
    pub time_clamps: u64,
}

/// One source/generated pair, with the verdict. Every gate is printed whether it
/// passed or not: a tolerance that is only visible when it fails is a tolerance
/// that gets quietly widened.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FidelityCheck {
    pub name: String,
    pub source: f64,
    pub generated: f64,
    pub tolerance: String,
    pub pass: bool,
    /// Set when the gate cannot be stated against the source value as-is. The one
    /// case in practice: the source's `range_parts` (max 63) exceeds its own
    /// multi-key part count (61), so the two capture files disagree about the part
    /// population and no generator can reproduce 63 from 61. Saturation is asserted
    /// against the achievable maximum instead, and this says so.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

/// The event table the fixture materializes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableSpec {
    pub packing_key: String,
    pub sort_key: Vec<String>,
    pub ts_column: String,
    /// The Ukiel table schema, exactly as it will be handed to `create_hypertable`.
    pub schema: serde_json::Value,
}

/// A value and how often it is drawn, relative to its siblings.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WeightedValue {
    pub value: String,
    pub weight: f64,
}

/// The synthetic value distributions — the part of the fixture the profile says
/// **nothing** about.
///
/// The captured aggregates describe geometry: which tenants live in which parts,
/// how sparse a part's key range is, how skewed tenant activity is. They describe
/// no column values at all. So the values here are declared, versioned, and stored
/// verbatim in the manifest, and they are *useful query data, not a claim about
/// production*. Anyone reading a number off this fixture can see exactly what was
/// invented and what was derived.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ValueModel {
    pub version: String,
    pub events: Vec<WeightedValue>,
    pub urls: Vec<WeightedValue>,
    pub hosts: Vec<WeightedValue>,
    pub libs: Vec<WeightedValue>,
    /// Persons per tenant scale with its row count, so `distinct_id` is neither
    /// unique per row nor constant per tenant — both of which would make "count
    /// distinct persons" a meaningless query.
    pub persons_per_tenant_min: u64,
    pub persons_per_tenant_max: u64,
    /// Rows per person follow a Zipf-like law with this exponent.
    pub person_zipf_exponent: f64,
}

/// Where a generated part came from, recorded so provenance is auditable — and
/// *only* as provenance.
///
/// None of these become an Ukiel part's size, row count, or level. ClickHouse
/// bytes describe a different storage engine with different compression; a
/// ClickHouse merge level is not a rung on Ukiel's compaction ladder. Copying
/// either across would give a real number a false meaning.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartProvenance {
    /// The source's deterministic 64-bit name hashes, unsigned: they fill the whole
    /// domain, so i64 would reject half of them.
    pub source_part_id: u64,
    pub source_partition_id: u64,
    pub source_level: i64,
    pub source_part_type: String,
    pub source_physical_rows: i64,
    pub source_observed_rows: i64,
    pub source_bytes_on_disk: i64,
    pub source_distinct_keys: i64,
    pub source_key_span: i64,
}

/// One generated Parquet file. Row count and byte size are read back from the
/// **closed file**, never predicted.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ManifestPart {
    pub index: u32,
    pub shard: u32,
    /// Relative to the manifest's directory, e.g. `parquet/shard-000/part-00017.parquet`.
    pub path: String,
    pub rows: u64,
    pub bytes: u64,
    pub digest: Digest,
    /// Derived from the part's actual members, after the graph was realized.
    pub key_min: i64,
    pub key_max: i64,
    pub distinct_keys: u64,
    pub key_density: f64,
    pub ts_min: i64,
    pub ts_max: i64,
    pub provenance: PartProvenance,
}

/// Tenants a benchmark should ask about, chosen from the finished graph so the
/// classes mean something: `heavy`/`median`/`light` by row count,
/// `high_overfetch`/`low_overfetch` by how badly range pruning over-selects for
/// them — which is the entire behaviour under test.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Representatives {
    pub heavy: i64,
    pub median: i64,
    pub light: i64,
    pub high_overfetch: i64,
    pub low_overfetch: i64,
    /// A deterministic spread across the whole population, for distribution checks.
    pub sample: Vec<i64>,
}

/// The artifact's identity. Small on purpose.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Manifest {
    pub manifest_version: String,
    pub disclaimer: String,
    pub generator: Generator,
    pub config: GenerationConfig,
    pub source: SourceProfile,
    pub generated: GeneratedSummary,
    pub fidelity: Vec<FidelityCheck>,
    pub table: TableSpec,
    pub value_model: ValueModel,
    /// `topology.json`, by digest — so a topology from another seed is a digest
    /// error rather than a subtly wrong benchmark.
    pub topology: FileDigest,
    pub parts: Vec<ManifestPart>,
    pub representatives: Representatives,
}

/// One tenant's identity and weight in the graph.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Tenant {
    /// The synthetic packing-key value. Drawn from the scaled key universe; it is
    /// not a real tenant identifier and does not correspond to one.
    pub id: i64,
    pub rows: u64,
    /// The tenant record this one was resampled from — kept whole, so activity and
    /// fanout stay joined. Drawing them independently is exactly how the previous
    /// fixture managed to look plausible and answer the wrong question.
    pub sample_rows: u64,
    pub sample_exact_parts: u64,
    pub exact_parts: u64,
    pub range_parts: u64,
    pub range_overfetch: f64,
    /// True if the feasibility repair moved this tenant's degree off its sample.
    pub repaired: bool,
}

/// One part's exact membership.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Membership {
    pub part: u32,
    /// The tenant IDs actually present in this part, ascending. This is the truth
    /// every other key structure is derived from: the part's `key_min`/`key_max`,
    /// the writer's roaring bitmap, and the catalog's Bloom filter all read off
    /// this list. Nothing about keys is assigned independently of it.
    pub tenants: Vec<i64>,
    /// Rows per member, aligned with `tenants`. Every member gets at least one row.
    pub rows: Vec<u64>,
    pub ts_min: i64,
    pub ts_max: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopologyShard {
    pub shard: u32,
    pub memberships: Vec<Membership>,
}

/// The membership graph. Large, and only read by a tool that walks it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Topology {
    pub manifest_version: String,
    pub disclaimer: String,
    pub config: GenerationConfig,
    pub key_universe: u64,
    pub tenants: Vec<Tenant>,
    pub shards: Vec<TopologyShard>,
}

impl Manifest {
    /// Parse and validate the contract: right version, and it says what it is.
    ///
    /// Fails closed on an unknown version. A benchmark that half-reads a format it
    /// does not know reports a number nobody can trust, and "forward compatible"
    /// is not a property this contract claims.
    pub fn parse(path: &str, bytes: &[u8]) -> Result<Self, crate::ContractError> {
        let value: serde_json::Value =
            serde_json::from_slice(bytes).map_err(|source| crate::ContractError::Parse {
                path: path.to_string(),
                source,
            })?;
        check_version(path, &value)?;
        let manifest: Manifest =
            serde_json::from_value(value).map_err(|source| crate::ContractError::Parse {
                path: path.to_string(),
                source,
            })?;
        if manifest.disclaimer != crate::SYNTHETIC_DISCLAIMER {
            return Err(crate::ContractError::MissingDisclaimer {
                path: path.to_string(),
            });
        }
        Ok(manifest)
    }
}

impl Topology {
    pub fn parse(path: &str, bytes: &[u8]) -> Result<Self, crate::ContractError> {
        let value: serde_json::Value =
            serde_json::from_slice(bytes).map_err(|source| crate::ContractError::Parse {
                path: path.to_string(),
                source,
            })?;
        check_version(path, &value)?;
        serde_json::from_value(value).map_err(|source| crate::ContractError::Parse {
            path: path.to_string(),
            source,
        })
    }

    /// Every membership edge as `(tenant_id, part_index, rows)`, ascending by part.
    pub fn edges(&self) -> impl Iterator<Item = (i64, u32, u64)> + '_ {
        self.shards.iter().flat_map(|s| {
            s.memberships.iter().flat_map(|m| {
                m.tenants
                    .iter()
                    .zip(m.rows.iter())
                    .map(move |(t, r)| (*t, m.part, *r))
            })
        })
    }
}

/// The version gate both files go through. Read as a bare `Value` first: a
/// mismatched version must be reported *as* a version mismatch, not as whatever
/// field happens to fail to deserialize first.
fn check_version(path: &str, value: &serde_json::Value) -> Result<(), crate::ContractError> {
    let found = value
        .get("manifest_version")
        .and_then(|v| v.as_str())
        .unwrap_or("<absent>");
    if found != crate::MANIFEST_VERSION {
        return Err(crate::ContractError::Version {
            path: path.to_string(),
            found: found.to_string(),
        });
    }
    Ok(())
}

/// blake3 of a file's raw bytes, lowercase hex. The one hash the pipeline speaks.
///
/// Deliberately over the bytes on disk, not over a reserialized struct: the point
/// is to prove *these* bytes are the ones that were generated, and a round-trip
/// through serde would launder exactly the corruption this is meant to catch.
pub fn digest_bytes(bytes: &[u8]) -> Digest {
    blake3_hex(bytes)
}

fn blake3_hex(bytes: &[u8]) -> String {
    // A tiny dependency-free wrapper so the contract crate does not pull blake3
    // into every consumer's graph twice under different features. The hash itself
    // is blake3's, via the workspace pin.
    blake3::hash(bytes).to_hex().to_string()
}

impl FileDigest {
    pub fn of(path: impl Into<String>, bytes: &[u8]) -> Self {
        FileDigest {
            path: path.into(),
            bytes: bytes.len() as u64,
            digest: digest_bytes(bytes),
        }
    }

    /// Re-check a digest at a tool boundary. Called by the loader *before* it
    /// connects to anything, so a corrupt artifact fails on the filesystem rather
    /// than halfway through writing to a catalog.
    pub fn verify(&self, path: &str, bytes: &[u8]) -> Result<(), crate::ContractError> {
        let actual = digest_bytes(bytes);
        if actual != self.digest {
            return Err(crate::ContractError::Digest {
                path: path.to_string(),
                expected: self.digest.clone(),
                actual,
            });
        }
        Ok(())
    }
}
