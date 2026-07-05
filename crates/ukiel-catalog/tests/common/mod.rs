use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use ukiel_catalog::PostgresCatalog;

/// Starts a throwaway Postgres and returns a migrated catalog.
/// Keep the returned container binding alive for the whole test.
pub async fn setup() -> (ContainerAsync<Postgres>, PostgresCatalog) {
    let container = Postgres::default()
        .start()
        .await
        .expect("start postgres container");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("mapped port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    let catalog = PostgresCatalog::connect(&url).await.expect("connect");
    catalog.migrate().await.expect("migrate");
    (container, catalog)
}
