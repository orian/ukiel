//! Process-wide Prometheus recorder (metrics P1). Only the binary installs an
//! exporter; library crates emit through the `metrics` facade and pay nothing
//! without a recorder.

use std::sync::OnceLock;

use axum::Router;
use axum::routing::get;
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};

static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

/// Installs the Prometheus recorder once per process; returns the render
/// handle. Idempotent so repeated `run_with_bound_addr` (integration tests)
/// is safe. `*_duration_seconds` families get explicit buckets — the
/// exporter's default quantile summaries can't be aggregated across the
/// stateless query nodes.
pub fn handle() -> PrometheusHandle {
    HANDLE
        .get_or_init(|| {
            PrometheusBuilder::new()
                .set_buckets_for_metric(
                    Matcher::Suffix("_duration_seconds".to_string()),
                    &[
                        0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
                        10.0, 30.0, 60.0, 120.0, 300.0,
                    ],
                )
                .expect("duration buckets")
                .install_recorder()
                .expect("install prometheus recorder")
        })
        .clone()
}

/// Adds `GET /metrics` (Prometheus text) to `router`, rendering from `handle`.
pub fn route(router: Router, handle: PrometheusHandle) -> Router {
    router.route(
        "/metrics",
        get(move || {
            let handle = handle.clone();
            async move { handle.render() }
        }),
    )
}
