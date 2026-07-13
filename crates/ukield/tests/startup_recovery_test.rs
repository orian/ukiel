//! Plan 42, Task 3: a process that starts while the catalog is unreachable must
//! come up anyway, stay alive, and finish initializing when the catalog appears.
//!
//! Before this, `run` connected eagerly and ran migrations before binding
//! anything — so a database failover during a rolling deploy killed each new
//! process before it had an endpoint to report the problem on, and the restarts
//! hammered the primary exactly while it was being promoted.

use std::time::Duration;

use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;
use ukield::config::UkieldConfig;

/// A TCP forwarder that does not exist until we start it — so the catalog is
/// genuinely unreachable (connection refused) and then genuinely reachable at
/// the same address, without moving a container. The compose-level fault proxy
/// for the full outage/recovery scenario is Task 7.
async fn forward(listen: TcpListener, target: String, shutdown: CancellationToken) {
    loop {
        tokio::select! {
            () = shutdown.cancelled() => return,
            accepted = listen.accept() => {
                let Ok((mut inbound, _)) = accepted else { return };
                let target = target.clone();
                tokio::spawn(async move {
                    if let Ok(mut outbound) = TcpStream::connect(&target).await {
                        let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
                    }
                });
            }
        }
    }
}

async fn reserve_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

#[tokio::test(flavor = "multi_thread")]
async fn ukield_starts_and_stays_alive_while_the_catalog_is_unreachable() {
    let pg = Postgres::default().start().await.expect("postgres");
    let pg_port = pg.get_host_port_ipv4(5432).await.unwrap();

    // ukield is pointed at a port where nothing is listening yet.
    let proxy_port = reserve_port().await;
    let cfg: UkieldConfig = toml::from_str(&format!(
        r#"
        roles = ["query"]
        [catalog]
        url = "postgres://postgres:postgres@127.0.0.1:{proxy_port}/postgres"
        retry_base_ms = 50
        retry_max_ms = 200
        acquire_timeout_ms = 250
        recovery_probe_timeout_ms = 500
        [object_store]
        kind = "memory"
        [query]
        listen = "127.0.0.1:0"
        "#
    ))
    .unwrap();

    let shutdown = CancellationToken::new();
    let (bound_tx, bound_rx) = tokio::sync::oneshot::channel();
    let server = {
        let (cfg, token) = (cfg, shutdown.clone());
        tokio::spawn(
            async move { ukield::run::run_with_bound_addr(cfg, token, Some(bound_tx)).await },
        )
    };

    // The listener binds even though the catalog has never answered: this is the
    // property that keeps a failover from becoming a crash loop.
    let addr = tokio::time::timeout(Duration::from_secs(30), bound_rx)
        .await
        .expect("the query listener must bind before the catalog is reachable")
        .expect("server died before binding");
    let base = format!("http://{addr}");
    let client = reqwest::Client::new();

    // Liveness must NOT depend on PostgreSQL. If it did, a managed failover
    // would fail every liveness probe in the fleet at once and the orchestrator
    // would restart every process — the exact stampede this plan prevents.
    for _ in 0..3 {
        let resp = client.get(format!("{base}/healthz")).send().await.unwrap();
        assert_eq!(
            resp.status(),
            200,
            "liveness must not depend on the catalog"
        );
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    assert!(!server.is_finished(), "the process must still be running");

    // The catalog appears at the address ukield has been retrying all along.
    let listener = TcpListener::bind(("127.0.0.1", proxy_port)).await.unwrap();
    let proxy = tokio::spawn(forward(
        listener,
        format!("127.0.0.1:{pg_port}"),
        shutdown.clone(),
    ));

    // Initialization advances: migrations run, and the process becomes able to
    // serve queries — all inside the same process that started during the outage.
    let mut ready = false;
    for _ in 0..100 {
        if let Ok(resp) = client.get(format!("{base}/readyz")).send().await
            && resp.status() == 200
        {
            ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(
        ready,
        "the process must finish initializing once the catalog appears"
    );

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(30), server).await;
    proxy.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_during_a_catalog_outage_exits_promptly() {
    // A SIGTERM while the catalog is down must drain now, not after the backoff.
    let proxy_port = reserve_port().await;
    let cfg: UkieldConfig = toml::from_str(&format!(
        r#"
        roles = ["query"]
        [catalog]
        url = "postgres://postgres:postgres@127.0.0.1:{proxy_port}/postgres"
        retry_base_ms = 5000
        retry_max_ms = 5000
        acquire_timeout_ms = 250
        recovery_probe_timeout_ms = 500
        [object_store]
        kind = "memory"
        [query]
        listen = "127.0.0.1:0"
        "#
    ))
    .unwrap();

    let shutdown = CancellationToken::new();
    let (bound_tx, bound_rx) = tokio::sync::oneshot::channel();
    let server = {
        let (cfg, token) = (cfg, shutdown.clone());
        tokio::spawn(
            async move { ukield::run::run_with_bound_addr(cfg, token, Some(bound_tx)).await },
        )
    };
    tokio::time::timeout(Duration::from_secs(30), bound_rx)
        .await
        .expect("bound")
        .expect("server up");

    let started = std::time::Instant::now();
    shutdown.cancel();
    let result = tokio::time::timeout(Duration::from_secs(20), server)
        .await
        .expect("shutdown must not wait out the backoff")
        .expect("task joined");

    assert!(
        started.elapsed() < Duration::from_secs(15),
        "took {:?} to exit",
        started.elapsed()
    );
    // Exiting because we were told to, while the catalog was never reachable, is
    // reported as an error — but a *bounded* one, not a hang.
    assert!(
        result.is_err(),
        "startup never completed, so run reports why"
    );
}
