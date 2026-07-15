//! The prod-synth event suite label. The six query classes live in
//! `bench/queries/prod-synth/queries.sql` and are imported as data by the `compile`
//! command; this module exists so the suite kind has a home and a docstring.

/// The suite kind label used in reports.
pub const KIND: &str = "prod_synth";
