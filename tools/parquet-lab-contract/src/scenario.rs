//! `ukiel-parquet-scenario/v1` — one measured cell of the layered framework.
//!
//! A scenario names exactly one measurement: which layer runs (L0 census through
//! L5 SQL), which projection role and row-group access plan it exercises, the
//! predicate or query it consumes, the sink that forces the work to happen, the
//! backend it reads from, the cache profile it must run under, and the sample
//! policy that decides how many times it repeats. Nothing here times anything; it
//! is the immutable declaration a bench tool binds a raw report to.
//!
//! Roles are generic (`fixed_width_key`, `wide_text`, …) so the same scenario pack
//! binds to a prod-synth event shape and a ClickBench OLAP shape through each
//! dataset's own workload manifest. The bench tool resolves a role onto a concrete
//! column; the scenario never hard-codes a column name.

use serde::{Deserialize, Serialize};

use crate::{ContractError, check_version};

/// The scenario format. A reader that does not recognise it fails closed.
pub const SCENARIO_VERSION: &str = "ukiel-parquet-scenario/v1";

/// Which measurement layer a scenario belongs to. L4 pruning is not implemented in
/// this plan; a scenario declares only the layers the foundation measures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Layer {
    /// L0: physical/per-column accounting. No timing.
    Census,
    /// L1: Arrow-to-Parquet writer timing only.
    Writer,
    /// L2: raw byte/range reads to a checksum sink; never parses Parquet.
    RawRead,
    /// L3: direct Arrow/Parquet decode with explicit row-group selection; no SQL.
    Scan,
    /// L5: bounded SQL guard through DataFusion.
    Sql,
}

/// A generic projection role, bound onto a concrete column by a dataset's workload
/// manifest. The direct-scan pack in Plan 49 binds exactly these five.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionRole {
    /// One fixed-width key column (physical width, dictionary/RLE, filter-key decode).
    FixedWidthKey,
    /// One high-cardinality string (dictionary fallback, byte-array decode).
    HighCardinalityString,
    /// One wide text/string column (compression and memory pressure).
    WideText,
    /// The registered hot set (a realistic narrow product query).
    HotSet,
    /// Every column (full-row scan ceiling).
    AllColumns,
}

/// An explicit row-group access plan, independent of any predicate. This measures
/// the value of reading fewer row groups separately from whether statistics or an
/// index can discover them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RowGroupSelection {
    /// Every row group (full scan).
    All,
    /// Exactly one row group.
    One,
    /// A 10% contiguous run of row groups (range-like access).
    TenPercentContiguous,
    /// A 10% scattered set of row groups (dashboard/key-like access).
    TenPercentSparse,
    /// Zero row groups: a metadata/footer-only negative control, never timed as a scan.
    Zero,
}

/// The sink that consumes decoded values so neither the compiler nor the reader can
/// discard the work and a large result array cannot dominate the timing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResultSink {
    /// A typed deterministic checksum over decoded values.
    Checksum,
    /// A row/value count. Only valid as a labelled metadata-path negative control.
    Count,
    /// A SQL aggregate (sum/count/group) that forces decode without returning rows.
    Aggregate,
}

/// Where the bytes are read from. This plan is local-only; no remote backend appears.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Backend {
    /// Bytes served from a resident in-memory artifact.
    Memory,
    /// Bytes read from the local filesystem.
    Local,
}

/// A verified local cache profile. "Cold" and "warm" are meaningless without naming
/// the cache; each profile has a residency contract the cache controller enforces.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CacheProfile {
    /// Fresh reader, bytes preloaded into memory: a codec/decode CPU floor.
    DecodeResident,
    /// Fresh process/reader, target ranges resident above the residency floor.
    LocalOsWarm,
    /// Fresh process/reader, target files evicted below the residency ceiling.
    LocalOsCold,
    /// Reused process/session, target ranges resident: repeated-query steady state.
    LocalReaderWarm,
}

impl CacheProfile {
    /// Whether this profile requires OS page-cache residency to be *high* (warm) —
    /// as opposed to *low* (cold). `decode-resident` preloads its own memory
    /// artifact and does not constrain the OS page cache.
    pub fn requires_warm(&self) -> bool {
        matches!(self, CacheProfile::LocalOsWarm | CacheProfile::LocalReaderWarm)
    }

    /// Whether this profile requires the OS page cache to be *evicted* (cold).
    pub fn requires_cold(&self) -> bool {
        matches!(self, CacheProfile::LocalOsCold)
    }
}

/// How many samples a scenario collects. The control determines the count before any
/// variant result is read; every paired arm reuses the same count.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct SamplePolicy {
    /// The warm-sample floor: `max(warm_min, ceil(warm_target_seconds / control_median))`.
    pub warm_min: u32,
    /// The measured control time a warm sample count must exceed.
    pub warm_target_seconds: f64,
    /// The pre-registered maximum warm sample count.
    pub warm_cap: u32,
    /// The minimum number of independently prepared cold samples.
    pub cold_min: u32,
}

impl SamplePolicy {
    /// The warm sample count implied by a measured control median (seconds). Frozen
    /// from the control before any variant result is accepted.
    pub fn warm_count(&self, control_median_seconds: f64) -> u32 {
        if control_median_seconds <= 0.0 {
            return self.warm_min.min(self.warm_cap);
        }
        let needed = (self.warm_target_seconds / control_median_seconds).ceil() as u32;
        self.warm_min.max(needed).min(self.warm_cap)
    }
}

/// One immutable scenario declaration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScenarioManifest {
    pub scenario_version: String,
    /// A stable id, unique within a scenario pack, e.g. `l3-scan-wide_text-10pct_sparse`.
    pub id: String,
    pub layer: Layer,
    /// The projection role, when the layer decodes columns. `None` for L1 (whole file)
    /// and L2 (raw bytes).
    #[serde(default)]
    pub projection: Option<ProjectionRole>,
    /// The row-group access plan, for L3. `None` for layers that do not select groups.
    #[serde(default)]
    pub selection: Option<RowGroupSelection>,
    /// The predicate/query this scenario consumes, for L5 (and a labelled predicate for
    /// L3 where relevant). Open shape: a registered query class name or a predicate spec.
    #[serde(default)]
    pub query: Option<serde_json::Value>,
    /// The sink that forces the work. `None` for L0/L1 (no decode) and L2 (checksum is
    /// implicit).
    #[serde(default)]
    pub sink: Option<ResultSink>,
    pub backend: Backend,
    pub cache_profile: CacheProfile,
    pub sample_policy: SamplePolicy,
}

impl ScenarioManifest {
    pub fn parse(path: &str, bytes: &[u8]) -> Result<Self, ContractError> {
        let value: serde_json::Value =
            serde_json::from_slice(bytes).map_err(|source| ContractError::Parse {
                path: path.to_string(),
                source,
            })?;
        check_version(path, &value, SCENARIO_VERSION, "scenario_version")?;
        let m: ScenarioManifest =
            serde_json::from_value(value).map_err(|source| ContractError::Parse {
                path: path.to_string(),
                source,
            })?;
        m.validate(path)?;
        Ok(m)
    }

    /// Enforce the layer/field invariants that make a scenario measurable:
    /// an L3 scan must name a projection and a selection; a `count` sink is only a
    /// metadata negative control (zero selection) and never a full scan.
    pub fn validate(&self, path: &str) -> Result<(), ContractError> {
        if self.id.trim().is_empty() {
            return Err(ContractError::EmptyScenarioField {
                context: path.to_string(),
                field: "id".to_string(),
            });
        }
        if self.layer == Layer::Scan
            && (self.projection.is_none() || self.selection.is_none())
        {
            return Err(ContractError::IncompleteScenario {
                context: path.to_string(),
                scenario: self.id.clone(),
                missing: "an L3 scan scenario must declare both a projection and a selection"
                    .to_string(),
            });
        }
        // A `count` sink may only ride the metadata path: a zero selection. Anything
        // that claims to scan rows while counting is the `count(*)`-is-a-scan anti-pattern.
        if self.sink == Some(ResultSink::Count)
            && self.selection.is_some()
            && self.selection != Some(RowGroupSelection::Zero)
        {
            return Err(ContractError::IncompleteScenario {
                context: path.to_string(),
                scenario: self.id.clone(),
                missing: "a count sink is only a metadata negative control; it cannot satisfy a \
                          scanning selection"
                    .to_string(),
            });
        }
        Ok(())
    }
}
