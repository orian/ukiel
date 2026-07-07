use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use ukiel_catalog::PostgresCatalog;
use ukiel_core::{NamespaceId, Placement};
use ukield::bootstrap;
use ukield::config::UkieldConfig;

const CONFIG: &str = r#"
    [catalog]
    url = "unused"
    [object_store]
    [[tables]]
    name = "events"
    topic = "events"
    ts_column = "ts"
    packing_key = "tenant_id"
    sort_key = ["tenant_id", "ts"]
    partition_columns = ["day"]
    placement = "separated"
    namespaces = [1, 2]
    [[tables.columns]]
    name = "tenant_id"
    type = "int64"
    [[tables.columns]]
    name = "ts"
    type = "timestamp_ms"
    [[tables.columns]]
    name = "payload"
    type = "utf8"
"#;

/// A single-table config with the given extra `[[tables]]` line(s) spliced in.
fn table_config(name: &str, extra: &str) -> UkieldConfig {
    let cfg = format!(
        r#"
        [catalog]
        url = "unused"
        [object_store]
        [[tables]]
        name = "{name}"
        ts_column = "ts"
        packing_key = "tenant_id"
        sort_key = ["tenant_id", "ts"]
        partition_columns = ["day"]
        {extra}
        namespaces = [1]
        [[tables.columns]]
        name = "tenant_id"
        type = "int64"
        [[tables.columns]]
        name = "ts"
        type = "timestamp_ms"
        [[tables.columns]]
        name = "payload"
        type = "utf8"
    "#
    );
    toml::from_str(&cfg).unwrap()
}

async fn fresh_catalog() -> (
    testcontainers_modules::testcontainers::ContainerAsync<Postgres>,
    PostgresCatalog,
) {
    let pg = Postgres::default().start().await.expect("postgres");
    let port = pg.get_host_port_ipv4(5432).await.unwrap();
    let catalog = PostgresCatalog::connect(&format!(
        "postgres://postgres:postgres@127.0.0.1:{port}/postgres"
    ))
    .await
    .unwrap();
    catalog.migrate().await.unwrap();
    (pg, catalog)
}

#[tokio::test]
async fn bootstrap_sets_size_targeted_placement_from_target_file_mb() {
    let (_pg, catalog) = fresh_catalog().await;
    let cfg = table_config("events_sized", "target_file_mb = 256");

    bootstrap::apply(&catalog, &cfg.tables).await.unwrap();

    let ht = catalog.get_hypertable("events_sized").await.unwrap();
    assert_eq!(ht.placement, Placement::SizeTargeted(256 * 1024 * 1024));
}

#[tokio::test]
async fn bootstrap_rejects_placement_and_target_file_mb_together() {
    let (_pg, catalog) = fresh_catalog().await;
    let cfg = table_config(
        "events_conflict",
        "placement = \"separated\"\n        target_file_mb = 256",
    );

    let err = bootstrap::apply(&catalog, &cfg.tables).await.unwrap_err();
    assert!(err.to_string().contains("mutually exclusive"), "{err}");
}

#[tokio::test]
async fn bootstrap_is_idempotent() {
    let pg = Postgres::default().start().await.expect("postgres");
    let port = pg.get_host_port_ipv4(5432).await.unwrap();
    let catalog = PostgresCatalog::connect(&format!(
        "postgres://postgres:postgres@127.0.0.1:{port}/postgres"
    ))
    .await
    .unwrap();
    catalog.migrate().await.unwrap();

    let cfg: UkieldConfig = toml::from_str(CONFIG).unwrap();

    // First boot creates everything.
    bootstrap::apply(&catalog, &cfg.tables).await.unwrap();
    let ht = catalog.get_hypertable("events").await.unwrap();
    assert_eq!(ht.packing_key, "tenant_id");
    assert_eq!(ht.placement, Placement::Separated);
    for ns in [1, 2] {
        catalog
            .get_logical_table(NamespaceId(ns), "events")
            .await
            .unwrap();
    }

    // Second boot is a no-op (same hypertable id, no duplicates, no error).
    bootstrap::apply(&catalog, &cfg.tables).await.unwrap();
    assert_eq!(catalog.get_hypertable("events").await.unwrap().id, ht.id);
    assert_eq!(catalog.list_hypertables().await.unwrap().len(), 1);
    assert_eq!(
        catalog
            .list_logical_tables(NamespaceId(1))
            .await
            .unwrap()
            .len(),
        1
    );

    // A bad expression is rejected up front.
    let mut bad: UkieldConfig = toml::from_str(CONFIG).unwrap();
    bad.tables[0].name = "bad_events".to_string();
    bad.tables[0].columns.push(ukield::config::ColumnConfig {
        name: "boom".to_string(),
        type_name: "float64".to_string(),
        nullable: None,
        default: Some("random()".to_string()),
        materialized: None,
        alias: None,
    });
    assert!(
        bootstrap::apply(&catalog, &bad.tables).await.is_err(),
        "volatile default must fail"
    );
}
