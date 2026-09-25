//! The causal experiment contracts: a reconstruction control, a one-variable
//! variant delta against it, and the experiment envelope that binds both controls,
//! the deltas, the scenario pack, and the planned run set.
//!
//! Plan 47/48 compared rewritten variants directly against the *product* bytes, so
//! rewrite bias (row-group boundaries, library defaults, the ZSTD-1 rewrite level)
//! rode inside every "causal" number. This module makes the two comparisons the
//! framework actually needs first-class and enforced:
//!
//! - **product control vs reconstruction control** — how much did rewriting itself
//!   move bytes and boundaries? (rewrite bias, reported separately, never a candidate)
//! - **reconstruction control vs one-variable variant** — the causal effect of the
//!   single requested physical change.
//!
//! A [`VariantDeltaManifest`] therefore carries a *delta* against the reconstruction,
//! not a second full writer configuration: an explicit `allowed_changes` allowlist
//! and only the changed values. The resolver writes a complete resolved config into
//! the manifest and structurally diffs it against the reconstruction; any physical
//! field that moved outside `allowed_changes` fails the variant closed, so a delta
//! cannot accidentally change two axes at once.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{ContractError, Fingerprint, VariantFileMap, check_version};

/// The reconstruction-control manifest format.
pub const RECONSTRUCTION_VERSION: &str = "ukiel-parquet-reconstruction/v1";
/// The variant-delta manifest format.
pub const VARIANT_DELTA_VERSION: &str = "ukiel-parquet-variant-delta/v1";
/// The experiment envelope format.
pub const EXPERIMENT_VERSION: &str = "ukiel-parquet-experiment/v1";

/// The recognised global writer-config paths. `allowed_changes` and delta change
/// paths are checked against these (plus the per-column suffixes) so an unknown or
/// mistyped path fails closed rather than silently permitting an unbounded change.
pub const KNOWN_GLOBAL_PATHS: &[&str] = &[
    "global.row_group_rows",
    "global.key_boundary_flush",
    "global.write_batch_rows",
    "global.data_page_bytes",
    "global.dictionary_page_bytes",
    "global.statistics",
    "global.offset_index",
    "global.compression",
];

/// The recognised per-column suffixes, under `columns.<name>.<suffix>`.
pub const KNOWN_COLUMN_SUFFIXES: &[&str] = &[
    "encoding",
    "dictionary",
    "compression",
    "bloom_fpp",
    "bloom_ndv",
    "physical_type",
];

/// Is `path` a recognised physical writer-config path? Global paths match exactly;
/// a `columns.<name>.<suffix>` path matches when `<suffix>` is a known column field.
pub fn is_known_config_path(path: &str) -> bool {
    if KNOWN_GLOBAL_PATHS.contains(&path) {
        return true;
    }
    if let Some(rest) = path.strip_prefix("columns.") {
        // rest is `<name>.<suffix>`; the name may itself contain dots only if the
        // column name does, which Ukiel schemas never produce, so split on the last dot.
        if let Some((_name, suffix)) = rest.rsplit_once('.') {
            return KNOWN_COLUMN_SUFFIXES.contains(&suffix);
        }
    }
    false
}

/// A complete resolved writer configuration as a canonical, sorted path→value map,
/// e.g. `"global.compression" -> "zstd(1)"`. Both a reconstruction and its variant
/// carry one so they can be structurally diffed at a single, comparable altitude.
///
/// ZSTD level is not footer-observable, so this map is authoritative for the codec
/// *level*; the footer census proves the codec *family* and other observable
/// properties separately.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ResolvedConfig {
    #[serde(flatten)]
    pub fields: BTreeMap<String, serde_json::Value>,
}

impl ResolvedConfig {
    pub fn new(fields: BTreeMap<String, serde_json::Value>) -> Self {
        ResolvedConfig { fields }
    }

    /// The set of paths whose values differ between `self` and `other` (or that are
    /// present in only one). Deterministic and sorted.
    pub fn diff(&self, other: &ResolvedConfig) -> Vec<String> {
        let mut changed = std::collections::BTreeSet::new();
        for (k, v) in &self.fields {
            if other.fields.get(k) != Some(v) {
                changed.insert(k.clone());
            }
        }
        for k in other.fields.keys() {
            if !self.fields.contains_key(k) {
                changed.insert(k.clone());
            }
        }
        changed.into_iter().collect()
    }
}

/// How far the reconstruction bytes drifted from the product bytes: the identity the
/// analyzer needs to report rewrite bias without re-opening either file. Sizes are
/// exact; the analyzer turns them into a percentage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RewriteBias {
    pub product_total_bytes: u64,
    pub reconstruction_total_bytes: u64,
    /// Per-column exact bytes, product then reconstruction, keyed by column, for the
    /// per-column rewrite-bias table. Open shape so the census payload can supply it.
    pub per_column: serde_json::Value,
}

/// The reconstruction control: the product's logical rows rewritten through the
/// laboratory writer under the resolved baseline policy (including ZSTD-1). It is the
/// causal comparison base; every variant is a child of exactly this.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReconstructionManifest {
    pub reconstruction_version: String,
    /// The product-control snapshot manifest this was rebuilt from, by digest.
    pub parent_product_digest: crate::Digest,
    /// The baseline spec (TOML) the reconstruction resolved from, by digest.
    pub baseline_spec_digest: crate::Digest,
    pub label: String,
    /// The writer configuration as requested from the baseline spec.
    pub requested_config: ResolvedConfig,
    /// The writer configuration resolved (defaults filled, footer-observable codec
    /// family/encodings cross-checked). Authoritative for codec level.
    pub resolved_config: ResolvedConfig,
    /// The declared logical types (from the product snapshot's projection), carried so a
    /// `vary` child can fold and compare the logical fingerprint without re-reading the
    /// product. Column name → logical type name.
    pub logical_projection: serde_json::Map<String, serde_json::Value>,
    /// The sort key, carried so a child stamps the same sorting columns.
    pub sort_key: Vec<String>,
    /// The physical Arrow schema of the reconstruction files.
    pub physical_schema: serde_json::Value,
    /// The logical fingerprint of the rebuilt rows. Must equal the product's.
    pub logical_fingerprint: Fingerprint,
    pub files: Vec<VariantFileMap>,
    pub input_census: serde_json::Value,
    pub output_census: serde_json::Value,
    /// Product-vs-reconstruction byte identity, so rewrite bias is never silently
    /// folded into a variant comparison.
    pub rewrite_bias: RewriteBias,
}

impl ReconstructionManifest {
    pub fn parse(path: &str, bytes: &[u8]) -> Result<Self, ContractError> {
        let value: serde_json::Value =
            serde_json::from_slice(bytes).map_err(|source| ContractError::Parse {
                path: path.to_string(),
                source,
            })?;
        check_version(
            path,
            &value,
            RECONSTRUCTION_VERSION,
            "reconstruction_version",
        )?;
        let m: ReconstructionManifest =
            serde_json::from_value(value).map_err(|source| ContractError::Parse {
                path: path.to_string(),
                source,
            })?;
        m.validate(path)?;
        Ok(m)
    }

    pub fn validate(&self, path: &str) -> Result<(), ContractError> {
        let mut seen = std::collections::HashSet::new();
        for f in &self.files {
            crate::check_relative_path(path, &f.output.path)?;
            if !seen.insert(f.output.path.as_str()) {
                return Err(ContractError::DuplicateFile {
                    context: path.to_string(),
                    duplicate: f.output.path.clone(),
                });
            }
        }
        Ok(())
    }

    /// Prove this reconstruction descends from the given product snapshot digest.
    pub fn check_parent(&self, product_digest: &str) -> Result<(), ContractError> {
        if self.parent_product_digest != product_digest {
            return Err(ContractError::ParentMismatch {
                context: format!("reconstruction '{}'", self.label),
                expected: product_digest.to_string(),
                actual: self.parent_product_digest.clone(),
            });
        }
        Ok(())
    }
}

/// A one-variable variant: a child of the reconstruction control changing only the
/// values named in `allowed_changes`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VariantDeltaManifest {
    pub delta_version: String,
    /// The reconstruction control this is a child of, by digest.
    pub parent_reconstruction_digest: crate::Digest,
    pub label: String,
    /// The exact physical paths this variant is permitted to change. Every changed
    /// path must be listed here, and every listed path must be a known config field.
    pub allowed_changes: Vec<String>,
    /// Only the changed values, as a flat path→value map (the delta).
    pub changes: BTreeMap<String, serde_json::Value>,
    /// The full resolved child configuration, for the structural diff against the
    /// reconstruction and for the census to prove the observable change.
    pub resolved_config: ResolvedConfig,
    pub logical_fingerprint: Fingerprint,
    pub files: Vec<VariantFileMap>,
    pub input_census: serde_json::Value,
    pub output_census: serde_json::Value,
}

impl VariantDeltaManifest {
    pub fn parse(path: &str, bytes: &[u8]) -> Result<Self, ContractError> {
        let value: serde_json::Value =
            serde_json::from_slice(bytes).map_err(|source| ContractError::Parse {
                path: path.to_string(),
                source,
            })?;
        check_version(path, &value, VARIANT_DELTA_VERSION, "delta_version")?;
        let m: VariantDeltaManifest =
            serde_json::from_value(value).map_err(|source| ContractError::Parse {
                path: path.to_string(),
                source,
            })?;
        m.validate(path)?;
        Ok(m)
    }

    /// Enforce the delta invariants that keep a variant one-variable:
    ///
    /// - every path in `allowed_changes` is a known physical config field (an unknown
    ///   allowlist entry fails closed — it must never be a silent escape hatch);
    /// - every changed value's path is listed in `allowed_changes`; and
    /// - output paths are relative and unique.
    ///
    /// The reconstruction-vs-child structural diff is enforced by
    /// [`Self::check_against_reconstruction`], which needs the parent's config.
    pub fn validate(&self, path: &str) -> Result<(), ContractError> {
        for allowed in &self.allowed_changes {
            if !crate::experiment::is_known_config_path(allowed) {
                return Err(ContractError::UnknownAllowlistPath {
                    context: path.to_string(),
                    path: allowed.clone(),
                });
            }
        }
        for changed in self.changes.keys() {
            if !self.allowed_changes.iter().any(|a| a == changed) {
                return Err(ContractError::AllowlistViolation {
                    context: path.to_string(),
                    path: changed.clone(),
                });
            }
        }
        let mut seen = std::collections::HashSet::new();
        for f in &self.files {
            crate::check_relative_path(path, &f.output.path)?;
            if !seen.insert(f.output.path.as_str()) {
                return Err(ContractError::DuplicateFile {
                    context: path.to_string(),
                    duplicate: f.output.path.clone(),
                });
            }
        }
        Ok(())
    }

    /// Prove this variant is a child of the given reconstruction and that its resolved
    /// config differs from the reconstruction's *only* on `allowed_changes` paths. This
    /// is the structural-diff gate: a field that moved outside the allowlist (a writer
    /// default the delta tripped, a second axis a careless delta changed) fails closed.
    pub fn check_against_reconstruction(
        &self,
        reconstruction_digest: &str,
        reconstruction_config: &ResolvedConfig,
    ) -> Result<(), ContractError> {
        if self.parent_reconstruction_digest != reconstruction_digest {
            return Err(ContractError::ParentMismatch {
                context: format!("variant '{}'", self.label),
                expected: reconstruction_digest.to_string(),
                actual: self.parent_reconstruction_digest.clone(),
            });
        }
        for changed in reconstruction_config.diff(&self.resolved_config) {
            if !self.allowed_changes.iter().any(|a| a == &changed) {
                return Err(ContractError::AllowlistViolation {
                    context: format!("variant '{}' resolved diff", self.label),
                    path: changed,
                });
            }
        }
        Ok(())
    }
}

/// The workload binding: dataset id plus the generic role → concrete column map that
/// lets one scenario pack bind to any dataset's real columns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkloadBinding {
    /// The dataset id, e.g. `prod-synth-30m` or `clickbench-10m`.
    pub dataset_id: String,
    /// Generic role → concrete column, e.g. `{"fixed_width_key": "team_id"}`.
    pub roles: serde_json::Map<String, serde_json::Value>,
    /// The hot column set, ordered, bound to the `hot_set` projection role.
    pub hot_columns: Vec<String>,
}

/// The experiment envelope: both controls, the variant deltas, the scenario pack, and
/// the planned run set — all by digest — for one dataset/workload binding.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExperimentManifest {
    pub experiment_version: String,
    pub experiment_id: String,
    pub workload: WorkloadBinding,
    /// The product control (immutable snapshot) manifest, by digest.
    pub product_digest: crate::Digest,
    /// The reconstruction control manifest, by digest.
    pub reconstruction_digest: crate::Digest,
    /// The variant-delta manifests, by digest.
    pub variant_delta_digests: Vec<crate::Digest>,
    /// The scenario manifests, by digest.
    pub scenario_digests: Vec<crate::Digest>,
    /// The planned run set, by digest. `None` until the schedule is frozen.
    #[serde(default)]
    pub run_set_digest: Option<crate::Digest>,
}

impl ExperimentManifest {
    pub fn parse(path: &str, bytes: &[u8]) -> Result<Self, ContractError> {
        let value: serde_json::Value =
            serde_json::from_slice(bytes).map_err(|source| ContractError::Parse {
                path: path.to_string(),
                source,
            })?;
        check_version(path, &value, EXPERIMENT_VERSION, "experiment_version")?;
        let m: ExperimentManifest =
            serde_json::from_value(value).map_err(|source| ContractError::Parse {
                path: path.to_string(),
                source,
            })?;
        m.validate(path)?;
        Ok(m)
    }

    pub fn validate(&self, path: &str) -> Result<(), ContractError> {
        if self.product_digest == self.reconstruction_digest {
            return Err(ContractError::DegenerateExperiment {
                context: path.to_string(),
                reason: "the product and reconstruction controls cannot be the same artifact"
                    .to_string(),
            });
        }
        if self.scenario_digests.is_empty() {
            return Err(ContractError::DegenerateExperiment {
                context: path.to_string(),
                reason: "an experiment must bind at least one scenario".to_string(),
            });
        }
        Ok(())
    }
}
