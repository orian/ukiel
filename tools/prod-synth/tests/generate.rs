//! Task 3: the streaming Parquet generator, the manifest, and offline verification.

use std::path::{Path, PathBuf};

use arrow::array::{Array, Int64Array, StringArray};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use prod_synth::profile::ProductionProfile;
use prod_synth::topology::TopologyConfig;
use prod_synth::{generate, topology, verify};
use prod_synth_contract::{Manifest, Tier, Topology};

fn profile_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/prod-info")
}

/// A tiny fixture: 200 tenants, enough rows to clear the membership floor. Small
/// enough to read every row back and check it.
fn tiny(dir: &Path, seed: u64) -> generate::Generated {
    let profile = ProductionProfile::load(&profile_dir()).expect("profile");
    let config = TopologyConfig {
        tier: Tier::Smoke,
        tenants: 200,
        rows_per_shard: 20_000,
        shards: 1,
        seed,
        overrides: vec![],
    };
    let topo = topology::compile(&profile, &config).expect("compiles");
    generate::generate(&profile, &topo, dir, false, false).expect("generates")
}

/// Read every row of every part back. The fixture's promises are row-level, so the
/// check has to be too.
#[test]
fn generated_rows_are_sorted_valid_and_internally_consistent() {
    let tmp = tempfile::tempdir().unwrap();
    let out = tmp.path().join("fx");
    let g = tiny(&out, 0);

    let mut total = 0u64;
    for part in &g.manifest.parts {
        let file = std::fs::File::open(out.join(&part.path)).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();

        let mut prev: Option<(i64, i64, String, String, String)> = None;
        let mut seen_keys = std::collections::BTreeSet::new();
        let mut persons =
            std::collections::BTreeMap::<i64, std::collections::BTreeSet<String>>::new();
        let mut rows_per_tenant = std::collections::BTreeMap::<i64, u64>::new();
        let mut rows_here = 0u64;

        for batch in reader {
            let batch = batch.unwrap();
            let col = |n: &str| batch.column(batch.schema().index_of(n).unwrap()).clone();
            let team = col("team_id");
            let team = team.as_any().downcast_ref::<Int64Array>().unwrap();
            let ts = col("timestamp");
            let ts = ts.as_any().downcast_ref::<Int64Array>().unwrap();
            let strs = |n: &str| {
                let a = col(n);
                let a = a.as_any().downcast_ref::<StringArray>().unwrap();
                (0..a.len())
                    .map(|i| a.value(i).to_string())
                    .collect::<Vec<_>>()
            };
            let event = strs("event");
            let did = strs("distinct_id");
            let uuid = strs("uuid");
            let props = strs("properties");
            let url = strs("mat_$current_url");
            let host = strs("mat_$host");
            let lib = strs("mat_$lib");

            for i in 0..batch.num_rows() {
                let k = team.value(i);
                let t = ts.value(i);
                seen_keys.insert(k);
                persons.entry(k).or_default().insert(did[i].clone());
                *rows_per_tenant.entry(k).or_default() += 1;
                rows_here += 1;

                // Sorted by the declared sort key: (team_id, timestamp, event,
                // distinct_id, uuid). The catalog and the query provider both assume
                // this ordering; a file that is not in it makes DataFusion's elided
                // sort a lie.
                let key = (k, t, event[i].clone(), did[i].clone(), uuid[i].clone());
                if let Some(p) = &prev {
                    assert!(
                        *p <= key,
                        "part {} row {i} is out of sort order",
                        part.index
                    );
                }
                prev = Some(key);

                // Timestamps inside the declared span, which is inside the window.
                assert!(t >= part.ts_min && t <= part.ts_max, "part {}", part.index);
                assert!(t >= prod_synth_contract::FIXTURE_WINDOW_START_MS);
                assert!(t < prod_synth_contract::FIXTURE_WINDOW_END_MS);

                // `properties` is valid compact JSON, and the three promoted columns
                // hold exactly what it holds. A query reading `mat_$lib` and a query
                // digging `$lib` out of the JSON must agree, or the fixture cannot be
                // used to compare them.
                let v: serde_json::Value =
                    serde_json::from_str(&props[i]).expect("properties must be valid JSON");
                assert_eq!(v["$current_url"].as_str().unwrap(), url[i], "promoted url");
                assert_eq!(v["$host"].as_str().unwrap(), host[i], "promoted host");
                assert_eq!(v["$lib"].as_str().unwrap(), lib[i], "promoted lib");
            }
        }

        assert_eq!(rows_here, part.rows, "part {} row count", part.index);
        total += rows_here;

        // The file holds exactly the tenants the graph says it holds — no more, no
        // fewer. This is the promise the catalog's Bloom filter is built on.
        let declared: std::collections::BTreeSet<i64> = topology_members(&out, part.index);
        assert_eq!(
            seen_keys, declared,
            "part {}: the file's tenants are not the topology's",
            part.index
        );
        assert_eq!(
            *seen_keys.first().unwrap(),
            part.key_min,
            "part {} key_min",
            part.index
        );
        assert_eq!(
            *seen_keys.last().unwrap(),
            part.key_max,
            "part {} key_max",
            part.index
        );

        // `distinct_id` repeats within a tenant — otherwise "count distinct persons"
        // is a row count in disguise and measures nothing.
        for (tenant, ppl) in &persons {
            let rows_for = *rows_per_tenant.get(tenant).unwrap_or(&0);
            if rows_for > 20 {
                assert!(
                    (ppl.len() as u64) < rows_for,
                    "tenant {tenant} has {} distinct persons across {rows_for} rows — persons must \
                     repeat, or `count distinct persons` is just a row count",
                    ppl.len()
                );
            }
        }

        // The topology's per-member row counts are the file's per-tenant row counts.
        let topo_rows = topology_member_rows(&out, part.index);
        assert_eq!(
            topo_rows, rows_per_tenant,
            "part {}: the graph's rows are not the file's",
            part.index
        );
    }

    assert_eq!(total, g.manifest.generated.rows);
}

/// Helpers that read the topology back rather than trusting an in-memory copy — the
/// artifact is what a loader will see, so the artifact is what the test checks.
fn topology_of(dir: &Path) -> Topology {
    let bytes = std::fs::read(dir.join("topology.json")).unwrap();
    Topology::parse("topology.json", &bytes).unwrap()
}

/// The topology's declared rows per tenant for one part.
fn topology_member_rows(dir: &Path, part: u32) -> std::collections::BTreeMap<i64, u64> {
    let t = topology_of(dir);
    let m = t
        .shards
        .iter()
        .flat_map(|s| s.memberships.iter())
        .find(|m| m.part == part)
        .expect("part in topology");
    m.tenants
        .iter()
        .copied()
        .zip(m.rows.iter().copied())
        .collect()
}

fn topology_members(dir: &Path, part: u32) -> std::collections::BTreeSet<i64> {
    topology_of(dir)
        .shards
        .iter()
        .flat_map(|s| s.memberships.iter())
        .find(|m| m.part == part)
        .expect("part in topology")
        .tenants
        .iter()
        .copied()
        .collect()
}

/// Byte-for-byte determinism. The whole promise of `--seed`.
#[test]
fn the_same_seed_produces_the_same_bytes() {
    let tmp = tempfile::tempdir().unwrap();
    let a = tiny(&tmp.path().join("a"), 4);
    let b = tiny(&tmp.path().join("b"), 4);

    assert_eq!(a.manifest.topology.digest, b.manifest.topology.digest);
    assert_eq!(a.manifest.parts.len(), b.manifest.parts.len());
    for (x, y) in a.manifest.parts.iter().zip(&b.manifest.parts) {
        assert_eq!(
            x.digest, y.digest,
            "part {} bytes differ across runs",
            x.index
        );
        assert_eq!(x.bytes, y.bytes);
        assert_eq!(x.rows, y.rows);
    }

    // And a different seed is a different fixture — the seed is wired in, not decorative.
    let c = tiny(&tmp.path().join("c"), 5);
    assert_ne!(a.manifest.topology.digest, c.manifest.topology.digest);
}

/// The manifest is the contract. It must round-trip, and it must refuse a version it
/// does not know rather than half-reading it.
#[test]
fn the_manifest_round_trips_and_unknown_versions_fail_closed() {
    let tmp = tempfile::tempdir().unwrap();
    let out = tmp.path().join("fx");
    let g = tiny(&out, 0);

    let bytes = std::fs::read(out.join("manifest.json")).unwrap();
    let parsed = Manifest::parse("manifest.json", &bytes).expect("round trips");
    assert_eq!(parsed.config, g.manifest.config);
    assert_eq!(
        parsed.generator.rng, "splitmix64/v1",
        "the RNG is named in the artifact"
    );
    assert_eq!(
        parsed.manifest_version,
        prod_synth_contract::MANIFEST_VERSION
    );
    assert_eq!(parsed.disclaimer, prod_synth_contract::SYNTHETIC_DISCLAIMER);

    // A future version is refused *as a version error*, not as whatever field happens
    // to fail to deserialize first.
    let mut v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    v["manifest_version"] = serde_json::json!("ukiel-prod-synth/v99");
    let e = Manifest::parse("m", &serde_json::to_vec(&v).unwrap()).unwrap_err();
    assert!(
        matches!(e, prod_synth_contract::ContractError::Version { .. }),
        "{e}"
    );

    // And an artifact that will not admit to being synthetic is not treated as one.
    let mut v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    v["disclaimer"] = serde_json::json!("definitely real production data");
    let e = Manifest::parse("m", &serde_json::to_vec(&v).unwrap()).unwrap_err();
    assert!(
        matches!(
            e,
            prod_synth_contract::ContractError::MissingDisclaimer { .. }
        ),
        "{e}"
    );
}

/// The value model is stored verbatim, so a reader can see exactly which columns are
/// evidence and which are invention.
#[test]
fn the_value_model_is_recorded_in_the_manifest() {
    let tmp = tempfile::tempdir().unwrap();
    let g = tiny(&tmp.path().join("fx"), 0);
    let m = &g.manifest.value_model;

    assert_eq!(m.version, "prod-synth-values/v1");
    assert!(m.events.iter().any(|e| e.value == "$pageview"));
    assert!(m.events.iter().any(|e| e.value == "$autocapture"));
    // Skewed, not uniform: a uniform vocabulary makes `top events` a tie.
    let top = m.events.iter().map(|e| e.weight).fold(0.0f64, f64::max);
    let bottom = m.events.iter().map(|e| e.weight).fold(f64::MAX, f64::min);
    assert!(top / bottom > 10.0, "the event vocabulary must be skewed");
}

/// Provenance is recorded and *not* reused. ClickHouse bytes and merge levels
/// describe a different engine; copying them across would give a real number a false
/// meaning.
#[test]
fn clickhouse_bytes_and_levels_are_provenance_never_generated_values() {
    let tmp = tempfile::tempdir().unwrap();
    let g = tiny(&tmp.path().join("fx"), 0);

    // The generated totals come from the row budget and the closed files. They are
    // nowhere near the source's counters, which describe a different engine over a
    // different amount of data.
    let rows: u64 = g.manifest.parts.iter().map(|p| p.rows).sum();
    let src_rows: i64 = g
        .manifest
        .parts
        .iter()
        .map(|p| p.provenance.source_observed_rows)
        .sum();
    assert_eq!(rows, 20_000, "the generated rows are the configured budget");
    assert!(
        src_rows > 4_000_000_000,
        "the source's own counters are four orders of magnitude larger and are NOT what was written"
    );

    // Every part large enough for the distinction to be visible carries its own,
    // measured size and row count — never ClickHouse's.
    for p in &g.manifest.parts {
        if p.provenance.source_observed_rows > 1_000 {
            assert_ne!(
                p.rows, p.provenance.source_observed_rows as u64,
                "part {}: the row count must be the file's own",
                p.index
            );
            assert_ne!(
                p.bytes, p.provenance.source_bytes_on_disk as u64,
                "part {}: the size must be the file's own",
                p.index
            );
        }
        // The provenance is still recorded — it is auditable, just never reused.
        assert!(p.provenance.source_part_id > 0);
        assert!(!p.provenance.source_part_type.is_empty());
    }

    // The source's ClickHouse merge levels span 0..1232. Every generated part sits at
    // one fixed non-L0 level instead, so an idle benchmark is stable — and so that no
    // one reads a compaction conclusion out of a ladder that was never climbed.
    assert!(
        g.manifest
            .parts
            .iter()
            .any(|p| p.provenance.source_level > 1),
        "the profile must still contain levels above 1 for this to be testing anything"
    );
    assert_eq!(generate::GENERATED_LEVEL, 1);
}

// ---------------------------------------------------------------------------
// Offline verification, and the failures it must catch.
// ---------------------------------------------------------------------------

#[test]
fn verify_accepts_a_freshly_generated_fixture() {
    let tmp = tempfile::tempdir().unwrap();
    let out = tmp.path().join("fx");
    let g = tiny(&out, 0);

    let v = verify::verify(&out.join("manifest.json")).expect("a fresh fixture verifies");
    assert_eq!(v.parts, g.manifest.parts.len());
    assert_eq!(v.rows, g.manifest.generated.rows);
    assert_eq!(v.bytes, g.manifest.generated.bytes);
}

#[test]
fn verify_catches_a_corrupted_parquet_file() {
    let tmp = tempfile::tempdir().unwrap();
    let out = tmp.path().join("fx");
    let g = tiny(&out, 0);

    // Truncate one part. This is the transport failure a benchmark host must never
    // silently absorb.
    let victim = out.join(&g.manifest.parts[0].path);
    let mut bytes = std::fs::read(&victim).unwrap();
    bytes.truncate(bytes.len() - 64);
    std::fs::write(&victim, bytes).unwrap();

    let e = verify::verify(&out.join("manifest.json")).unwrap_err();
    assert!(
        matches!(
            e,
            verify::VerifyError::Contract(prod_synth_contract::ContractError::Digest { .. })
        ),
        "{e}"
    );
}

#[test]
fn verify_catches_a_topology_from_another_seed() {
    let tmp = tempfile::tempdir().unwrap();
    let a = tmp.path().join("a");
    let b = tmp.path().join("b");
    tiny(&a, 0);
    tiny(&b, 1);

    // Swap in the other seed's topology. Without the digest this would be a subtly
    // wrong benchmark three hours later instead of an error now.
    std::fs::copy(b.join("topology.json"), a.join("topology.json")).unwrap();
    let e = verify::verify(&a.join("manifest.json")).unwrap_err();
    assert!(
        matches!(
            e,
            verify::VerifyError::Contract(prod_synth_contract::ContractError::Digest { .. })
        ),
        "{e}"
    );
}

/// The check that catches a *generator* bug rather than a transport bug: if the
/// writer's idea of a part's key range ever drifted from the graph's, the catalog
/// would prune against a range the file does not have, and rows would vanish from
/// query results with nothing in any log.
#[test]
fn verify_catches_a_manifest_range_that_the_file_does_not_have() {
    let tmp = tempfile::tempdir().unwrap();
    let out = tmp.path().join("fx");
    tiny(&out, 0);

    let path = out.join("manifest.json");
    let mut m: serde_json::Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    let old = m["parts"][0]["key_max"].as_i64().unwrap();
    m["parts"][0]["key_max"] = serde_json::json!(old + 1_000_000);
    std::fs::write(&path, serde_json::to_vec_pretty(&m).unwrap()).unwrap();

    let e = verify::verify(&path).unwrap_err();
    let msg = e.to_string();
    assert!(
        msg.contains("members' range") || msg.contains("team_id bounds"),
        "the declared range must be checked against the graph AND the footer: {msg}"
    );
}

// ---------------------------------------------------------------------------
// Atomic publication.
// ---------------------------------------------------------------------------

#[test]
fn generation_refuses_to_overwrite_without_replace() {
    let tmp = tempfile::tempdir().unwrap();
    let out = tmp.path().join("fx");
    tiny(&out, 0);

    let profile = ProductionProfile::load(&profile_dir()).unwrap();
    let config = TopologyConfig {
        tier: Tier::Smoke,
        tenants: 200,
        rows_per_shard: 20_000,
        shards: 1,
        seed: 0,
        overrides: vec![],
    };
    let topo = topology::compile(&profile, &config).unwrap();

    let e = generate::generate(&profile, &topo, &out, false, false).unwrap_err();
    assert!(matches!(e, generate::GenerateError::Exists { .. }), "{e}");
    // A report may already cite this fixture; replacing it must be deliberate.
    assert!(e.to_string().contains("--replace"));

    generate::generate(&profile, &topo, &out, true, false).expect("--replace is the escape hatch");
}

/// A failed fixture is never published. A half-written one that looks complete is
/// worse than none: it will be loaded, benchmarked, and cited.
#[test]
fn a_fixture_that_fails_its_gates_is_not_published() {
    let tmp = tempfile::tempdir().unwrap();
    let out = tmp.path().join("fx");

    let profile = ProductionProfile::load(&profile_dir()).unwrap();
    // Baseline is the tier the distribution gates bind on — but with a row budget far
    // too small to carry the activity skew, so they will fail.
    let config = TopologyConfig {
        tier: Tier::Baseline,
        tenants: 10_000,
        rows_per_shard: 200_000,
        shards: 1,
        seed: 0,
        overrides: vec!["rows_per_shard".into()],
    };
    let topo = topology::compile(&profile, &config).unwrap();

    let e = generate::generate(&profile, &topo, &out, false, true).unwrap_err();
    assert!(matches!(e, generate::GenerateError::Fidelity(_)), "{e}");
    assert!(
        !out.exists(),
        "nothing may be left behind at the published path"
    );
    assert!(
        !tmp.path().join(".fx.staging").exists(),
        "and no staging directory either"
    );
}
