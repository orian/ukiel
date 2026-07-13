//! Toxiproxy control client for the S10 catalog-outage suite (plan 42).
//!
//! Cutting the *network* to PostgreSQL is the only honest way to test this.
//! Stopping the database would also destroy the state recovery is supposed to
//! reload from, and closing a pool from inside the process tests our own code
//! against itself. A managed failover looks like this: connections dropped,
//! nothing answering, then everything back — with the data intact.
//!
//! Uses the `reqwest` already in the workspace; no new dependency and no CLI.

use std::time::Duration;

use serde_json::json;

/// The compose `ha` profile's Toxiproxy. Idempotent: a proxy left behind by a
/// previous (failed) run is reset rather than duplicated, because a toxic proxy
/// surviving into the next run would fail tests that have nothing to do with HA.
pub struct FaultProxy {
    client: reqwest::Client,
    api: String,
    name: String,
    /// Address ukield should use as its catalog: the proxy's listen side.
    pub listen: String,
}

impl FaultProxy {
    /// Creates (or resets) a proxy named `name` forwarding `listen` → `upstream`.
    /// Both addresses are as seen *from the Toxiproxy container*, except
    /// `listen`, whose port is also published on the host.
    pub async fn create(api: &str, name: &str, listen: &str, upstream: &str) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .expect("http client");
        let proxy = FaultProxy {
            client,
            api: api.to_string(),
            name: name.to_string(),
            listen: listen.to_string(),
        };

        // Delete-then-create: the only way to be sure we start from a clean
        // proxy whatever the previous run did to it.
        let _ = proxy
            .client
            .delete(format!("{api}/proxies/{name}"))
            .send()
            .await;
        let resp = proxy
            .client
            .post(format!("{api}/proxies"))
            .json(&json!({
                "name": name,
                "listen": listen,
                "upstream": upstream,
                "enabled": true,
            }))
            .send()
            .await
            .expect("toxiproxy is not reachable — start it with `--profile ha`");
        assert!(
            resp.status().is_success(),
            "creating proxy: {}",
            resp.status()
        );
        proxy
    }

    /// Cuts catalog traffic: existing connections are dropped and new ones are
    /// refused. This is the failover signature.
    pub async fn cut(&self) {
        self.set_enabled(false).await;
    }

    /// Restores catalog traffic. The database never went anywhere — which is the
    /// point: everything ukield reloads afterwards is the state it left behind.
    pub async fn restore(&self) {
        self.set_enabled(true).await;
    }

    async fn set_enabled(&self, enabled: bool) {
        let resp = self
            .client
            .post(format!("{}/proxies/{}", self.api, self.name))
            .json(&json!({ "enabled": enabled }))
            .send()
            .await
            .expect("toxiproxy control API");
        assert!(
            resp.status().is_success(),
            "setting enabled={enabled}: {}",
            resp.status()
        );
    }
}
