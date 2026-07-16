//! Plan-47 contract tests: version gating, digest binding, the relative-path rule,
//! duplicate detection, resolved-property completeness, unknown index kinds, and the
//! atomic-report identity envelope.

use parquet_lab_contract::{
    ColumnProperties, ContractError, FileDigest, Fingerprint, HostInfo, IndexKind, IndexedColumn,
    LOGICAL_ROW_MULTISET_VERSION, LogicalProjection, Probe, ProbeFamily, Query, REPORT_VERSION,
    RUN_SET_VERSION, ReportIdentity, ResultSemantics, RowGroupIndex, RunReport, RunSet,
    RunSetEntry, RunSetState, SKIP_MANIFEST_VERSION, SNAPSHOT_MANIFEST_VERSION,
    STORE_RECEIPT_VERSION, SUITE_VERSION, SkipManifest, SnapshotFile, SnapshotManifest, SourceKind,
    StoreObject, StoreReceipt, Suite, SuiteKind, ToolVersions, TypedLiteral,
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
            result_semantics: ResultSemantics::Ordered,
        }],
        probes: vec![Probe {
            name: "eq_0.001".into(),
            family: ProbeFamily::Equality,
            column: "team_id".into(),
            literals: vec![TypedLiteral::Int(5)],
            sql: "SELECT * FROM events WHERE team_id = 5".into(),
            expected_result_digest: "cd".repeat(32),
            result_semantics: ResultSemantics::Multiset,
            control_row_count: 8000,
            match_count: 42,
            observed_selectivity: 42.0 / 8000.0,
        }],
        skipped_probes: vec![parquet_lab_contract::SkippedProbe {
            name: "eq_missing".into(),
            family: ProbeFamily::Equality,
            column: "does_not_exist".into(),
            reason: parquet_lab_contract::ProbeSkipReason::ColumnMissing,
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
            repetition: Some(0),
            seed: Some(7),
            order_digest: Some("0rd".repeat(16) + "0"),
            backend: Some("local".into()),
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

// -- Task 47A: store receipt, run set, credentials, extended report identity ----------

fn store_receipt() -> StoreReceipt {
    StoreReceipt {
        receipt_version: STORE_RECEIPT_VERSION.into(),
        artifact_digest: "5nap".repeat(16),
        store_kind: "minio".into(),
        endpoint_identity: "http://127.0.0.1:9000".into(),
        bucket: "parquet-lab".into(),
        prefix: "disposable/run-1".into(),
        objects: vec![
            StoreObject {
                path: "parquet/part-0.parquet".into(),
                size: 100,
                sha256: "aa".repeat(32),
            },
            StoreObject {
                path: "parquet/part-1.parquet".into(),
                size: 200,
                sha256: "bb".repeat(32),
            },
        ],
    }
}

fn run_set(state: RunSetState, with_reports: bool) -> RunSet {
    let entry = |rep: u32, ord: u32, id: &str, rep_digest: Option<String>| RunSetEntry {
        repetition: rep,
        order_index: ord,
        artifact_kind: if id.contains("control") {
            "control"
        } else {
            "variant"
        }
        .into(),
        label: id.into(),
        artifact_digest: "5nap".repeat(16),
        expected_report_id: id.into(),
        report_digest: rep_digest,
    };
    let d = |s: &str| {
        if with_reports {
            Some(format!("{s:0<64}"))
        } else {
            None
        }
    };
    RunSet {
        run_set_version: RUN_SET_VERSION.into(),
        state,
        suite_digest: "su1t".repeat(16),
        control_digest: "5nap".repeat(16),
        block: "pages".into(),
        backend: "local".into(),
        reader_config: serde_json::json!({"pruning": true}),
        host: serde_json::json!({"cpu": "x86-64"}),
        repetitions: 2,
        seed: 7,
        schedule: vec![
            entry(0, 0, "rep0/order0/control", d("r0c0")),
            entry(0, 1, "rep0/order1/pages-32k", d("r0v1")),
            entry(1, 0, "rep1/order0/control", d("r1c0")),
        ],
    }
}

#[test]
fn a_store_receipt_round_trips_and_refuses_bad_keys() {
    let r = store_receipt();
    assert_eq!(
        r,
        StoreReceipt::parse("s", &serde_json::to_vec_pretty(&r).unwrap()).unwrap()
    );

    // Absolute / traversing object keys are refused.
    let mut bad = store_receipt();
    bad.objects[0].path = "/etc/passwd".into();
    assert!(matches!(
        bad.validate("s").unwrap_err(),
        ContractError::NonRelativePath { .. }
    ));

    // Duplicate objects are refused.
    let mut dup = store_receipt();
    dup.objects[1].path = dup.objects[0].path.clone();
    assert!(matches!(
        dup.validate("s").unwrap_err(),
        ContractError::DuplicateFile { .. }
    ));

    // Out-of-order objects are refused (the receipt must be sorted for a stable digest).
    let mut unsorted = store_receipt();
    unsorted.objects.reverse();
    assert!(matches!(
        unsorted.validate("s").unwrap_err(),
        ContractError::UnsortedObjects { .. }
    ));
}

#[test]
fn a_store_receipt_cannot_serialize_a_credential() {
    // The receipt has no field for a secret; prove a serialized one contains none of the
    // words a leaked credential would carry.
    let json = serde_json::to_string(&store_receipt())
        .unwrap()
        .to_lowercase();
    for forbidden in [
        "access_key",
        "secret",
        "session_token",
        "password",
        "aws_secret",
    ] {
        assert!(
            !json.contains(forbidden),
            "a store receipt must never carry '{forbidden}'"
        );
    }
}

#[test]
fn a_report_cannot_serialize_a_credential() {
    let json = serde_json::to_string(&report()).unwrap().to_lowercase();
    for forbidden in ["access_key", "secret_key", "session_token", "aws_secret"] {
        assert!(
            !json.contains(forbidden),
            "a report must never carry '{forbidden}'"
        );
    }
}

#[test]
fn a_run_set_round_trips_and_only_complete_is_analyzable() {
    let planned = run_set(RunSetState::Planned, false);
    assert_eq!(
        planned,
        RunSet::parse("rs", &serde_json::to_vec_pretty(&planned).unwrap()).unwrap()
    );
    assert!(
        !planned.is_analyzable(),
        "a planned run set is not analyzable"
    );

    let complete = run_set(RunSetState::Complete, true);
    assert!(complete.is_analyzable());
    assert_eq!(
        complete,
        RunSet::parse("rs", &serde_json::to_vec_pretty(&complete).unwrap()).unwrap()
    );
}

#[test]
fn a_complete_run_set_missing_a_report_is_refused() {
    let mut rs = run_set(RunSetState::Complete, true);
    rs.schedule[1].report_digest = None;
    assert!(matches!(
        rs.validate("rs").unwrap_err(),
        ContractError::MissingReport { .. }
    ));
}

#[test]
fn a_run_set_with_a_duplicate_slot_or_report_is_refused() {
    // Duplicate (repetition, order_index).
    let mut dup_slot = run_set(RunSetState::Planned, false);
    dup_slot.schedule[1].order_index = 0;
    dup_slot.schedule[1].repetition = 0;
    assert!(matches!(
        dup_slot.validate("rs").unwrap_err(),
        ContractError::DuplicateLabel { .. }
    ));

    // Same report digest bound to two entries.
    let mut dup_report = run_set(RunSetState::Complete, true);
    dup_report.schedule[1].report_digest = dup_report.schedule[0].report_digest.clone();
    assert!(matches!(
        dup_report.validate("rs").unwrap_err(),
        ContractError::DuplicateReport { .. }
    ));
}

#[test]
fn an_empty_run_set_schedule_is_refused() {
    let mut rs = run_set(RunSetState::Planned, false);
    rs.schedule.clear();
    assert!(matches!(
        rs.validate("rs").unwrap_err(),
        ContractError::EmptySchedule { .. }
    ));
}

#[test]
fn unknown_store_and_run_set_versions_fail_closed() {
    let mut v = serde_json::to_value(store_receipt()).unwrap();
    v["receipt_version"] = serde_json::json!("x");
    assert!(matches!(
        StoreReceipt::parse("s", &serde_json::to_vec(&v).unwrap()).unwrap_err(),
        ContractError::Version { .. }
    ));
    let mut v = serde_json::to_value(run_set(RunSetState::Planned, false)).unwrap();
    v["run_set_version"] = serde_json::json!("x");
    assert!(matches!(
        RunSet::parse("rs", &serde_json::to_vec(&v).unwrap()).unwrap_err(),
        ContractError::Version { .. }
    ));
}

#[test]
fn the_extended_report_identity_round_trips() {
    let r = report();
    let back = RunReport::parse("r", &serde_json::to_vec_pretty(&r).unwrap()).unwrap();
    assert_eq!(back.identity.repetition, Some(0));
    assert_eq!(back.identity.seed, Some(7));
    assert_eq!(back.identity.backend.as_deref(), Some("local"));
}

// -- Plan 49: causal experiment, scenario, and cache contracts ---------------

use parquet_lab_contract::{
    Backend, CacheProfile, CacheReceipt, ExperimentManifest, Layer, ProjectionRole,
    ReconstructionManifest, Residency, ResolvedConfig, ResultSink, RewriteBias, RowGroupSelection,
    SamplePolicy, ScenarioManifest, VariantDeltaManifest, WorkloadBinding, is_known_config_path,
};

fn resolved(compression: &str) -> ResolvedConfig {
    let mut m = std::collections::BTreeMap::new();
    m.insert("global.row_group_rows".into(), serde_json::json!(1_000_000));
    m.insert("global.data_page_bytes".into(), serde_json::json!(1 << 20));
    m.insert("global.statistics".into(), serde_json::json!("page"));
    m.insert("global.compression".into(), serde_json::json!(compression));
    ResolvedConfig::new(m)
}

fn file_map() -> VariantFileMap {
    VariantFileMap {
        input: FileDigest::of("product/part-00000.parquet", b"in"),
        output: FileDigest::of("parquet/part-00000.parquet", b"out"),
        input_rows: 100,
        output_rows: 100,
        footer_summary: serde_json::json!({"row_groups": 1}),
    }
}

fn reconstruction() -> ReconstructionManifest {
    ReconstructionManifest {
        reconstruction_version: parquet_lab_contract::RECONSTRUCTION_VERSION.into(),
        parent_product_digest: "prod".repeat(16),
        baseline_spec_digest: "ba5e".repeat(16),
        label: "reconstruction".into(),
        requested_config: resolved("zstd(1)"),
        resolved_config: resolved("zstd(1)"),
        logical_projection: {
            let mut m = serde_json::Map::new();
            m.insert("team_id".into(), serde_json::json!("int64"));
            m
        },
        sort_key: vec!["team_id".into()],
        physical_schema: serde_json::json!({"fields": []}),
        logical_fingerprint: logical_fp(),
        files: vec![file_map()],
        input_census: serde_json::json!({"total_bytes": 5000}),
        output_census: serde_json::json!({"total_bytes": 4800}),
        rewrite_bias: RewriteBias {
            product_total_bytes: 5000,
            reconstruction_total_bytes: 4800,
            per_column: serde_json::json!({}),
        },
    }
}

fn zstd6_delta() -> VariantDeltaManifest {
    let mut changes = std::collections::BTreeMap::new();
    changes.insert("global.compression".into(), serde_json::json!("zstd(6)"));
    VariantDeltaManifest {
        delta_version: parquet_lab_contract::VARIANT_DELTA_VERSION.into(),
        parent_reconstruction_digest: "reco".repeat(16),
        label: "compression-zstd-6".into(),
        allowed_changes: vec!["global.compression".into()],
        changes,
        resolved_config: resolved("zstd(6)"),
        logical_fingerprint: logical_fp(),
        files: vec![file_map()],
        input_census: serde_json::json!({}),
        output_census: serde_json::json!({}),
    }
}

fn scenario() -> ScenarioManifest {
    ScenarioManifest {
        scenario_version: parquet_lab_contract::SCENARIO_VERSION.into(),
        id: "l3-scan-wide_text-10pct_sparse".into(),
        layer: Layer::Scan,
        projection: Some(ProjectionRole::WideText),
        selection: Some(RowGroupSelection::TenPercentSparse),
        query: None,
        sink: Some(ResultSink::Checksum),
        backend: Backend::Local,
        cache_profile: CacheProfile::LocalOsWarm,
        sample_policy: SamplePolicy {
            warm_min: 7,
            warm_target_seconds: 3.0,
            warm_cap: 50,
            cold_min: 3,
        },
    }
}

fn cache_receipt(profile: CacheProfile, resident_after: f64, valid: bool) -> CacheReceipt {
    CacheReceipt {
        receipt_version: parquet_lab_contract::CACHE_RECEIPT_VERSION.into(),
        target_manifest_digest: "reco".repeat(16),
        target_files: vec![FileDigest::of("parquet/part-00000.parquet", b"out")],
        requested_profile: profile,
        preparation_method: "posix_fadvise(DONTNEED)+mincore".into(),
        residency_before: Residency { resident_fraction: 1.0, pages_probed: 1000 },
        residency_after: Residency { resident_fraction: resident_after, pages_probed: 1000 },
        warm_floor: Some(0.90),
        cold_ceiling: Some(0.10),
        valid,
    }
}

fn experiment() -> ExperimentManifest {
    ExperimentManifest {
        experiment_version: parquet_lab_contract::EXPERIMENT_VERSION.into(),
        experiment_id: "plan49".into(),
        workload: WorkloadBinding {
            dataset_id: "prod-synth-30m".into(),
            roles: {
                let mut m = serde_json::Map::new();
                m.insert("fixed_width_key".into(), serde_json::json!("team_id"));
                m
            },
            hot_columns: vec!["team_id".into(), "timestamp".into()],
        },
        product_digest: "prod".repeat(16),
        reconstruction_digest: "reco".repeat(16),
        variant_delta_digests: vec!["z6de".repeat(16)],
        scenario_digests: vec!["5cen".repeat(16)],
        run_set_digest: None,
    }
}

#[test]
fn every_causal_contract_round_trips() {
    let r = reconstruction();
    assert_eq!(r, ReconstructionManifest::parse("r", &serde_json::to_vec_pretty(&r).unwrap()).unwrap());
    let d = zstd6_delta();
    assert_eq!(d, VariantDeltaManifest::parse("d", &serde_json::to_vec_pretty(&d).unwrap()).unwrap());
    let s = scenario();
    assert_eq!(s, ScenarioManifest::parse("s", &serde_json::to_vec_pretty(&s).unwrap()).unwrap());
    let c = cache_receipt(CacheProfile::LocalOsCold, 0.05, true);
    assert_eq!(c, CacheReceipt::parse("c", &serde_json::to_vec_pretty(&c).unwrap()).unwrap());
    let e = experiment();
    assert_eq!(e, ExperimentManifest::parse("e", &serde_json::to_vec_pretty(&e).unwrap()).unwrap());
}

#[test]
fn causal_contract_versions_fail_closed() {
    let mut v = serde_json::to_value(reconstruction()).unwrap();
    v["reconstruction_version"] = serde_json::json!("x");
    assert!(matches!(ReconstructionManifest::parse("r", &serde_json::to_vec(&v).unwrap()).unwrap_err(), ContractError::Version { .. }));
    let mut v = serde_json::to_value(zstd6_delta()).unwrap();
    v["delta_version"] = serde_json::json!("x");
    assert!(matches!(VariantDeltaManifest::parse("d", &serde_json::to_vec(&v).unwrap()).unwrap_err(), ContractError::Version { .. }));
    let mut v = serde_json::to_value(scenario()).unwrap();
    v["scenario_version"] = serde_json::json!("x");
    assert!(matches!(ScenarioManifest::parse("s", &serde_json::to_vec(&v).unwrap()).unwrap_err(), ContractError::Version { .. }));
    let mut v = serde_json::to_value(cache_receipt(CacheProfile::LocalOsCold, 0.05, true)).unwrap();
    v["receipt_version"] = serde_json::json!("x");
    assert!(matches!(CacheReceipt::parse("c", &serde_json::to_vec(&v).unwrap()).unwrap_err(), ContractError::Version { .. }));
    let mut v = serde_json::to_value(experiment()).unwrap();
    v["experiment_version"] = serde_json::json!("x");
    assert!(matches!(ExperimentManifest::parse("e", &serde_json::to_vec(&v).unwrap()).unwrap_err(), ContractError::Version { .. }));
}

#[test]
fn reconstruction_binds_its_exact_product_parent() {
    let r = reconstruction();
    assert!(r.check_parent(&"prod".repeat(16)).is_ok());
    assert!(matches!(r.check_parent("wrong").unwrap_err(), ContractError::ParentMismatch { .. }));
}

#[test]
fn a_zstd6_delta_changes_only_compression_level() {
    let d = zstd6_delta();
    // Structurally diff against the reconstruction: only global.compression moved.
    assert!(d.check_against_reconstruction(&"reco".repeat(16), &resolved("zstd(1)")).is_ok());
    // Wrong parent is refused.
    assert!(matches!(
        d.check_against_reconstruction("other", &resolved("zstd(1)")).unwrap_err(),
        ContractError::ParentMismatch { .. }
    ));
}

#[test]
fn a_delta_that_also_changes_row_group_size_is_refused() {
    let mut d = zstd6_delta();
    // The child's resolved config quietly moved a second axis the allowlist never permitted.
    let mut cfg = resolved("zstd(6)");
    cfg.fields.insert("global.row_group_rows".into(), serde_json::json!(500_000));
    d.resolved_config = cfg;
    let err = d
        .check_against_reconstruction(&"reco".repeat(16), &resolved("zstd(1)"))
        .unwrap_err();
    assert!(matches!(err, ContractError::AllowlistViolation { .. }), "{err}");
}

#[test]
fn a_delta_declaring_a_change_outside_its_allowlist_is_refused() {
    let mut d = zstd6_delta();
    // The delta changes a path it never allowed.
    d.changes.insert("global.row_group_rows".into(), serde_json::json!(500_000));
    assert!(matches!(d.validate("d").unwrap_err(), ContractError::AllowlistViolation { .. }));
}

#[test]
fn an_unknown_allowlist_path_fails_closed() {
    let mut d = zstd6_delta();
    d.allowed_changes = vec!["global.magic_unicorn".into()];
    d.changes.clear();
    assert!(matches!(d.validate("d").unwrap_err(), ContractError::UnknownAllowlistPath { .. }));
    // And per-column paths are recognised by suffix.
    assert!(is_known_config_path("columns.team_id.physical_type"));
    assert!(is_known_config_path("global.compression"));
    assert!(!is_known_config_path("columns.team_id.nonsense"));
    assert!(!is_known_config_path("global.nonsense"));
}

#[test]
fn a_scan_scenario_must_name_a_projection_and_selection() {
    let mut s = scenario();
    s.selection = None;
    assert!(matches!(s.validate("s").unwrap_err(), ContractError::IncompleteScenario { .. }));
}

#[test]
fn a_count_sink_cannot_ride_a_scanning_selection() {
    let mut s = scenario();
    s.sink = Some(ResultSink::Count);
    // Count with an `all` selection is the count(*)-is-a-scan anti-pattern.
    s.selection = Some(RowGroupSelection::All);
    assert!(matches!(s.validate("s").unwrap_err(), ContractError::IncompleteScenario { .. }));
    // Count as a zero-selection metadata negative control is allowed.
    s.selection = Some(RowGroupSelection::Zero);
    s.projection = None;
    s.layer = Layer::Scan;
    // Zero selection still needs a projection under the scan layer, so use census layer here.
    s.layer = Layer::Census;
    assert!(s.validate("s").is_ok());
}

#[test]
fn a_cache_receipt_cannot_launder_an_ineffective_eviction() {
    // A cold receipt claiming validity while 60% resident is refused.
    let bad = cache_receipt(CacheProfile::LocalOsCold, 0.60, true);
    assert!(matches!(bad.validate("c").unwrap_err(), ContractError::CacheReceiptInconsistent { .. }));
    // The same numbers recorded as invalid is honest and accepted.
    let honest = cache_receipt(CacheProfile::LocalOsCold, 0.60, false);
    assert!(honest.validate("c").is_ok());
    assert!(!honest.is_usable());
    // A warm receipt below its floor while claiming validity is refused.
    let bad_warm = cache_receipt(CacheProfile::LocalOsWarm, 0.50, true);
    assert!(matches!(bad_warm.validate("c").unwrap_err(), ContractError::CacheReceiptInconsistent { .. }));
}

#[test]
fn sample_policy_freezes_warm_count_from_the_control() {
    let p = SamplePolicy { warm_min: 7, warm_target_seconds: 3.0, warm_cap: 50, cold_min: 3 };
    // Fast control -> the 3-second floor dominates.
    assert_eq!(p.warm_count(0.05), 50.min((3.0f64 / 0.05).ceil() as u32));
    // Slow control -> the 7-sample floor dominates.
    assert_eq!(p.warm_count(1.0), 7);
    // The cap bounds a pathologically fast control.
    assert_eq!(p.warm_count(0.0001), 50);
}

#[test]
fn an_experiment_refuses_identical_controls() {
    let mut e = experiment();
    e.reconstruction_digest = e.product_digest.clone();
    assert!(matches!(e.validate("e").unwrap_err(), ContractError::DegenerateExperiment { .. }));
    let mut e = experiment();
    e.scenario_digests.clear();
    assert!(matches!(e.validate("e").unwrap_err(), ContractError::DegenerateExperiment { .. }));
}

#[test]
fn old_variant_manifests_are_never_promoted_to_the_causal_contract() {
    // A plan-47/48 variant manifest still parses with its own reader.
    let v = variant();
    let bytes = serde_json::to_vec_pretty(&v).unwrap();
    assert!(VariantManifest::parse("v", &bytes).is_ok());
    // But it is NOT silently accepted as a causal variant-delta: the version gate refuses it.
    assert!(matches!(
        VariantDeltaManifest::parse("v", &bytes).unwrap_err(),
        ContractError::Version { .. }
    ));
    // And a causal delta is not accepted by the old variant reader.
    let d = serde_json::to_vec_pretty(&zstd6_delta()).unwrap();
    assert!(matches!(
        VariantManifest::parse("d", &d).unwrap_err(),
        ContractError::Version { .. }
    ));
}
