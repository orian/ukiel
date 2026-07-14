//! `prod-synth` — compile an anonymized workload profile into a portable,
//! synthetic, production-shaped Parquet fixture.
//!
//! Offline, deterministic, and installable with nothing but a Rust toolchain. It
//! needs no PostgreSQL, no Kafka, no object store, and no running Ukiel: the whole
//! point is that anyone can reproduce the fixture from the committed profile and
//! check that they got the same bytes.
//!
//! See `tools/prod-synth/README.md` for the runbook, and
//! `docs/superpowers/plans/2026-07-14-ukiel-prod-synth.md` for why the fixture is
//! shaped the way it is.

pub mod fidelity;
pub mod generate;
pub mod profile;
pub mod rng;
pub mod schema;
pub mod topology;
pub mod value_model;
pub mod verify;

pub use profile::{ProductionProfile, ProfileError};
pub use topology::{SyntheticTopology, TopologyConfig, compile};
