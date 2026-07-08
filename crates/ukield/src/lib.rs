//! ukield: the all-in-one Ukiel server binary. Library side holds the
//! testable pieces (config, bootstrap, run); main.rs is a thin shell.

pub mod bootstrap;
pub mod collector;
pub mod config;
pub mod metrics;
pub mod run;
