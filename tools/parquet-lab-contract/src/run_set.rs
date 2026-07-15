//! `ukiel-parquet-run-set/v1` — the schedule that makes a matrix run analyzable.
//!
//! A run set binds the suite, control, block, backend, reader configuration, host,
//! repetition count, seed, and the *complete scheduled order* of every report a matrix run
//! will produce — before any of them run. Only a `Complete` run set, in which every
//! scheduled entry is bound to a produced report digest, is analyzable; the analyzer refuses
//! a glob of reports precisely because a glob cannot prove it saw the whole interleaved
//! schedule, in the recorded order, with nothing missing and nothing extra.

use serde::{Deserialize, Serialize};

use crate::{ContractError, check_version};

/// The run-set format. A reader that does not recognise it fails closed.
pub const RUN_SET_VERSION: &str = "ukiel-parquet-run-set/v1";

/// A run set's lifecycle. Only `Complete` is analyzable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunSetState {
    /// The schedule is recorded but not all reports exist yet.
    Planned,
    /// A scheduled run failed; the set is not analyzable.
    Failed,
    /// Every scheduled entry is bound to a produced report digest.
    Complete,
}

/// One scheduled measurement: a (repetition, order) slot for one artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunSetEntry {
    pub repetition: u32,
    /// Position within the whole interleaved schedule.
    pub order_index: u32,
    /// `control` or `variant`.
    pub artifact_kind: String,
    pub label: String,
    /// The artifact (snapshot or variant) measured, by digest.
    pub artifact_digest: crate::Digest,
    /// A stable id for the report this slot expects, e.g. `rep0/order3/control`.
    pub expected_report_id: String,
    /// The produced report's digest — present only once the report exists. A `Complete` run
    /// set requires every entry to carry one.
    pub report_digest: Option<crate::Digest>,
}

/// The run set.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunSet {
    pub run_set_version: String,
    pub state: RunSetState,
    pub suite_digest: crate::Digest,
    /// The control artifact (the immutable snapshot) every block reruns.
    pub control_digest: crate::Digest,
    pub block: String,
    /// The backend identity (`memory`, `local`, or an object-store endpoint identity).
    pub backend: String,
    pub reader_config: serde_json::Value,
    pub host: serde_json::Value,
    pub repetitions: u32,
    pub seed: u64,
    /// The complete interleaved schedule, in order.
    pub schedule: Vec<RunSetEntry>,
}

impl RunSet {
    pub fn parse(path: &str, bytes: &[u8]) -> Result<Self, ContractError> {
        let value: serde_json::Value =
            serde_json::from_slice(bytes).map_err(|source| ContractError::Parse {
                path: path.to_string(),
                source,
            })?;
        check_version(path, &value, RUN_SET_VERSION, "run_set_version")?;
        let set: RunSet = serde_json::from_value(value).map_err(|source| ContractError::Parse {
            path: path.to_string(),
            source,
        })?;
        set.validate(path)?;
        Ok(set)
    }

    /// Enforce structural invariants: a non-empty schedule, unique (repetition, order) slots
    /// and expected ids, and — for a `Complete` set — every entry bound to a unique report.
    pub fn validate(&self, path: &str) -> Result<(), ContractError> {
        if self.schedule.is_empty() {
            return Err(ContractError::EmptySchedule {
                context: path.to_string(),
            });
        }
        let mut slots = std::collections::HashSet::new();
        let mut ids = std::collections::HashSet::new();
        for e in &self.schedule {
            if !slots.insert((e.repetition, e.order_index)) {
                return Err(ContractError::DuplicateLabel {
                    context: path.to_string(),
                    duplicate: format!("rep{}/order{}", e.repetition, e.order_index),
                });
            }
            if !ids.insert(e.expected_report_id.as_str()) {
                return Err(ContractError::DuplicateLabel {
                    context: path.to_string(),
                    duplicate: e.expected_report_id.clone(),
                });
            }
        }
        if self.state == RunSetState::Complete {
            let mut report_digests = std::collections::HashSet::new();
            for e in &self.schedule {
                let Some(d) = &e.report_digest else {
                    return Err(ContractError::MissingReport {
                        context: path.to_string(),
                        expected: e.expected_report_id.clone(),
                    });
                };
                if !report_digests.insert(d.as_str()) {
                    return Err(ContractError::DuplicateReport {
                        context: path.to_string(),
                        digest: d.clone(),
                    });
                }
            }
        }
        Ok(())
    }

    /// Only a complete run set may be analyzed.
    pub fn is_analyzable(&self) -> bool {
        self.state == RunSetState::Complete
    }
}
