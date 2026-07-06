use tokio_util::sync::CancellationToken;
use ukield::config::UkieldConfig;

fn parse_args() -> (String, bool) {
    let mut config_path = "ukield.toml".to_string();
    let mut bootstrap_only = false;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config_path = args.next().unwrap_or_else(|| {
                    eprintln!("--config requires a path");
                    std::process::exit(2);
                });
            }
            "--bootstrap-only" => bootstrap_only = true,
            other => {
                eprintln!(
                    "unknown argument '{other}'\nusage: ukield [--config <path>] [--bootstrap-only]"
                );
                std::process::exit(2);
            }
        }
    }
    (config_path, bootstrap_only)
}

async fn shutdown_signal(token: CancellationToken) {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => {},
            _ = term.recv() => {},
        }
    }
    #[cfg(not(unix))]
    {
        let _ = ctrl_c.await;
    }
    tracing::info!("shutdown signal received");
    token.cancel();
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let (config_path, bootstrap_only) = parse_args();
    let cfg = UkieldConfig::load(&config_path)?;

    if bootstrap_only {
        let catalog = ukiel_catalog::PostgresCatalog::connect(&cfg.catalog.url).await?;
        catalog.migrate().await?;
        ukield::bootstrap::apply(&catalog, &cfg.tables).await?;
        tracing::info!("bootstrap complete");
        return Ok(());
    }

    let shutdown = CancellationToken::new();
    tokio::spawn(shutdown_signal(shutdown.clone()));
    ukield::run::run(cfg, shutdown).await
}
