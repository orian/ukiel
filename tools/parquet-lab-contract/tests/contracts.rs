//! Plan-47 contract tests: version gating, digest binding, the relative-path rule,
//! duplicate detection, resolved-property completeness, unknown index kinds, and the
//! atomic-report identity envelope.

use parquet_lab_contract::{
    ColumnProperties, ContractError, FileDigest, Fingerprint, HostInfo, IndexKind, IndexedColumn,
    LOGICAL_ROW_MULTISET_VERSION, LogicalProjection, Probe, Query, REPORT_VERSION, ReportIdentity,
    RowGroupIndex, RunReport, SKIP_MANIFEST_VERSION, SNAPSHOT_MANIFEST_VERSION, SUITE_VERSION,
    SkipManifest, SnapshotFile, SnapshotManifest, SourceKind, Suite, SuiteKind, ToolVersions,
    VARIANT_MANIFEST_VERSION, VariantFileMap, VariantManifest, WriterProperties,
};

fn tool_versions() -> ToolVersions {
    ToolVersions {
        git_sha: "deadbeef".into(),
        arrow: "58.3".into(),
        parquet: "58.3".into(),
        datafusion: "54".into(),
    }
}

fn logical_fp() -> Fingerprint {
    Fingerprint {
        version: LOGICAL_ROW_MULTISET_VERSION.into(),
        count: 100,
        xor: "aa".repeat(32),
        sum: [1, 2, 3, 4],
        digest: "bb".repeat(32),
    }
}

fn snapshot() -> SnapshotManifest {
    SnapshotManifest {
        manifest_version: SNAPSHOT_MANIFEST_VERSION.into(),
        source_kind: SourceKind::Plan46Receipt,
        source_digests: vec![FileDigest::of("receipt.json", b"receipt")],
        tool_versions: tool_versions(),
        creation_command: "parquet-lab-snapshot from-ukiel ...".into(),
        logical_schema: serde_json::json!({"fields": [{"name": "team_id", "type": "int64"}]}),
        physical_schema: serde_json::json!({"fields": [{"name": "team_id", "type": "Int64"}]}),
        packing_key: "team_id".into(),
        sort_key: vec!["team_id".into(), "timestamp".into()],
        logical_projection: Some(LogicalProjection {
            logical_types: serde_json::Map::new(),
        }),
        files: vec![SnapshotFile {
            path: "parquet/part-0001.parquet".into(),
            object_key: Some("prod/table/part-0001.parquet".into()),
            digest: "cc".repeat(32),
            bytes: 4096,
            rows: 100,
            row_groups: 1,
            source_part: Some("42".into()),
        }],
        total_rows: 100,
        total_bytes: 4096,
        physical_fingerprint: None,
        logical_fingerprint: logical_fp(),
        disclaimer: Some("Synthetic fixture.".into()),
    }
}

fn variant() -> VariantManifest {
    VariantManifest {
        manifest_version: VARIANT_MANIFEST_VERSION.into(),
        parent_snapshot_digest: "5nap".repeat(16),
        spec_digest: "5pec".repeat(16),
        label: "pages-rowgroup-32k".into(),
        properties: WriterProperties {
            row_group_rows: 32_768,
            key_boundary_flush: true,
            write_batch_rows: 8192,
            data_page_bytes: 1 << 20,
            dictionary_page_bytes: 1 << 20,
            statistics: "page".into(),
            offset_index: true,
            compression: "zstd(3)".into(),
        },
        columns: vec![ColumnProperties {
            column: "team_id".into(),
            encoding: Some("delta_binary_packed".into()),
            dictionary: Some(false),
            compression: None,
            bloom_fpp: None,
            bloom_ndv: None,
            physical_type: None,
            resolved_encodings: vec!["DELTA_BINARY_PACKED".into()],
            resolved_dictionary: Some(false),
            resolved_compression: Some("zstd".into()),
        }],
        input_census: serde_json::json!({}),
        output_census: serde_json::json!({}),
        logical_fingerprint: logical_fp(),
        files: vec![VariantFileMap {
            input: FileDigest::of("parquet/part-0001.parquet", b"in"),
            output: FileDigest::of("out/part-0001.parquet", b"out"),
            input_rows: 100,
            output_rows: 100,
            footer_summary: serde_json::json!({"row_groups": 1}),
        }],
    }
}

fn skip() -> SkipManifest {
    SkipManifest {
        manifest_version: SKIP_MANIFEST_VERSION.into(),
        parent_variant_digest: "var1".repeat(16),
        file_digests: vec![FileDigest::of("out/part-0001.parquet", b"out")],
        payload_path: "skip/payload.bin".into(),
        columns: vec![IndexedColumn {
            column: "team_id".into(),
            kind: IndexKind::ZoneMap,
            kind_version: "zone-map/v1".into(),
            file: "out/part-0001.parquet".into(),
            row_groups: vec![RowGroupIndex {
                row_group: 0,
                kind: IndexKind::ZoneMap,
                parameters: serde_json::json!({"min": 1, "max": 900}),
                payload_offset: 0,
                payload_length: 16,
                payload_digest: "ee".repeat(32),
            }],
        }],
        build_wall_ms: 12,
        payload_bytes: 16,
    }
}

fn suite() -> Suite {
    Suite {
        suite_version: SUITE_VERSION.into(),
        kind: SuiteKind::ProdSynth,
        view_sql: "CREATE VIEW events AS SELECT * FROM events_physical".into(),
        queries: vec![Query {
            name: "q_heavy_tenant".into(),
            sql: "SELECT count(*) FROM events WHERE team_id = 900".into(),
            expected_result_digest: "ab".repeat(32),
        }],
        probes: vec![Probe {
            name: "eq_0.001".into(),
            family: "equality".into(),
            column: "team_id".into(),
            sql: "SELECT * FROM events WHERE team_id = 5".into(),
            target_selectivity: 0.001,
            observed_count: Some(42),
            expected_result_digest: Some("cd".repeat(32)),
        }],
    }
}

fn report() -> RunReport {
    RunReport {
        identity: ReportIdentity {
            report_version: REPORT_VERSION.into(),
            tool_versions: tool_versions(),
            snapshot_digest: "5nap".repeat(16),
            variant_digest: Some("var1".repeat(16)),
            suite_digest: Some("su1t".repeat(16)),
            skip_digest: None,
            run_order: 3,
        },
        host: HostInfo {
            target_cpu: Some("x86-64-v3".into()),
            host_cpu: None,
            host_ram_bytes: None,
            kernel: Some("6.17.0".into()),
            storage_kind: "local".into(),
            page_cache_dropped: None,
        },
        body: serde_json::json!({"queries": []}),
    }
}

// -- round-trips ------------------------------------------------------------

#[test]
fn every_manifest_round_trips() {
    let s = snapshot();
    assert_eq!(
        s,
        SnapshotManifest::parse("s", &serde_json::to_vec_pretty(&s).unwrap()).unwrap()
    );
    let v = variant();
    assert_eq!(
        v,
        VariantManifest::parse("v", &serde_json::to_vec_pretty(&v).unwrap()).unwrap()
    );
    let k = skip();
    assert_eq!(
        k,
        SkipManifest::parse("k", &serde_json::to_vec_pretty(&k).unwrap()).unwrap()
    );
    let su = suite();
    assert_eq!(
        su,
        Suite::parse("su", &serde_json::to_vec_pretty(&su).unwrap()).unwrap()
    );
    let r = report();
    assert_eq!(
        r,
        RunReport::parse("r", &serde_json::to_vec_pretty(&r).unwrap()).unwrap()
    );
}

// -- version gating: every version fails closed -----------------------------

#[test]
fn every_unknown_version_fails_closed() {
    let mut v = serde_json::to_value(snapshot()).unwrap();
    v["manifest_version"] = serde_json::json!("ukiel-parquet-snapshot/v99");
    assert!(matches!(
        SnapshotManifest::parse("s", &serde_json::to_vec(&v).unwrap()).unwrap_err(),
        ContractError::Version { .. }
    ));

    let mut v = serde_json::to_value(variant()).unwrap();
    v["manifest_version"] = serde_json::json!("x");
    assert!(matches!(
        VariantManifest::parse("v", &serde_json::to_vec(&v).unwrap()).unwrap_err(),
        ContractError::Version { .. }
    ));

    let mut v = serde_json::to_value(skip()).unwrap();
    v["manifest_version"] = serde_json::json!("x");
    assert!(matches!(
        SkipManifest::parse("k", &serde_json::to_vec(&v).unwrap()).unwrap_err(),
        ContractError::Version { .. }
    ));

    let mut v = serde_json::to_value(suite()).unwrap();
    v["suite_version"] = serde_json::json!("x");
    assert!(matches!(
        Suite::parse("su", &serde_json::to_vec(&v).unwrap()).unwrap_err(),
        ContractError::Version { .. }
    ));

    let mut v = serde_json::to_value(report()).unwrap();
    v["identity"]["report_version"] = serde_json::json!("x");
    assert!(matches!(
        RunReport::parse("r", &serde_json::to_vec(&v).unwrap()).unwrap_err(),
        ContractError::Version { .. }
    ));
}

// -- digest binding ---------------------------------------------------------

#[test]
fn variant_binds_to_its_parent_snapshot_by_digest() {
    let v = variant();
    assert!(v.check_parent(&"5nap".repeat(16)).is_ok());
    assert!(matches!(
        v.check_parent("wrong").unwrap_err(),
        ContractError::ParentMismatch { .. }
    ));
}

#[test]
fn a_sidecar_is_bound_to_its_variant_and_every_file() {
    let k = skip();
    let files = vec![FileDigest::of("out/part-0001.parquet", b"out")];
    assert!(k.is_bound_to(&"var1".repeat(16), &files));
    // Wrong variant digest: not bound.
    assert!(!k.is_bound_to("other-variant", &files));
    // A file that changed under it: not bound.
    let tampered = vec![FileDigest::of("out/part-0001.parquet", b"tampered")];
    assert!(!k.is_bound_to(&"var1".repeat(16), &tampered));
}

#[test]
fn a_file_digest_detects_tampering() {
    let fd = FileDigest::of("f", b"original");
    assert!(fd.verify("f", b"original").is_ok());
    assert!(matches!(
        fd.verify("f", b"tampered").unwrap_err(),
        ContractError::Digest { .. }
    ));
}

// -- relative-path rule -----------------------------------------------------

#[test]
fn absolute_and_traversing_paths_are_refused() {
    for bad in [
        "/abs/part.parquet",
        "../escape.parquet",
        "a/../../b.parquet",
    ] {
        let mut s = snapshot();
        s.files[0].path = bad.into();
        assert!(
            matches!(
                s.validate("s").unwrap_err(),
                ContractError::NonRelativePath { .. }
            ),
            "expected refusal for {bad}"
        );
    }
}

// -- duplicate detection ----------------------------------------------------

#[test]
fn duplicate_snapshot_files_are_refused() {
    let mut s = snapshot();
    let dup = s.files[0].clone();
    s.files.push(dup);
    assert!(matches!(
        s.validate("s").unwrap_err(),
        ContractError::DuplicateFile { .. }
    ));
}

#[test]
fn duplicate_variant_output_files_are_refused() {
    let mut v = variant();
    let dup = v.files[0].clone();
    v.files.push(dup);
    assert!(matches!(
        v.validate("v").unwrap_err(),
        ContractError::DuplicateFile { .. }
    ));
}

#[test]
fn duplicate_suite_labels_are_refused() {
    let mut su = suite();
    su.probes[0].name = su.queries[0].name.clone();
    assert!(matches!(
        su.validate("su").unwrap_err(),
        ContractError::DuplicateLabel { .. }
    ));
}

// -- incomplete resolved properties -----------------------------------------

#[test]
fn a_requested_property_without_a_resolved_one_is_refused() {
    let mut v = variant();
    // Requested an encoding but read nothing back from the footer.
    v.columns[0].resolved_encodings.clear();
    v.columns[0].resolved_dictionary = None;
    v.columns[0].resolved_compression = None;
    assert!(matches!(
        v.validate("v").unwrap_err(),
        ContractError::IncompleteProperties { .. }
    ));
}

// -- unknown index kinds ----------------------------------------------------

#[test]
fn an_unknown_index_kind_fails_to_parse() {
    let mut v = serde_json::to_value(skip()).unwrap();
    v["columns"][0]["kind"] = serde_json::json!("bloom_ngram_v2");
    let err = SkipManifest::parse("k", &serde_json::to_vec(&v).unwrap()).unwrap_err();
    assert!(matches!(err, ContractError::Parse { .. }), "{err}");
}

// -- logical-fingerprint version binding ------------------------------------

#[test]
fn a_snapshot_with_a_physical_fingerprint_in_the_logical_slot_is_refused() {
    let mut s = snapshot();
    s.logical_fingerprint.version = "row-multiset/v1".into();
    assert!(matches!(
        s.validate("s").unwrap_err(),
        ContractError::FingerprintVersion { .. }
    ));
}

// -- atomic-report identity -------------------------------------------------

#[test]
fn a_report_missing_its_identity_version_is_refused() {
    let mut v = serde_json::to_value(report()).unwrap();
    v["identity"]
        .as_object_mut()
        .unwrap()
        .remove("report_version");
    assert!(matches!(
        RunReport::parse("r", &serde_json::to_vec(&v).unwrap()).unwrap_err(),
        ContractError::Version { .. }
    ));
}

#[test]
fn fingerprints_agree_only_within_a_version() {
    let a = logical_fp();
    assert!(a.agrees_with(&a));
    let mut count = a.clone();
    count.count += 1;
    assert!(!a.agrees_with(&count));
    let mut ver = a.clone();
    ver.version = "logical-row-multiset/v2".into();
    assert!(!a.agrees_with(&ver));
}
