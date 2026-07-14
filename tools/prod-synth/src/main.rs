//! The `prod-synth` CLI: `profile`, `generate`, `verify`.
//!
//! Three properties this file is responsible for, all of them load-bearing:
//!
//! * **No implicit paths.** Every input and output is named on the command line.
//!   The tool has no idea what a repository is, no default `bench/` directory, and
//!   no opinion about the working directory. It is meant to be installed with
//!   `cargo install` and run from anywhere against a copied profile.
//! * **No service.** `--help`, `profile`, `generate` and `verify` all work with
//!   nothing running.
//! * **Explicit output.** `generate` requires `--output`, refuses to overwrite
//!   without `--replace`, and publishes atomically.

use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use prod_synth::profile::ProductionProfile;
use prod_synth::topology::TopologyConfig;
use prod_synth::{fidelity, generate, topology, verify};
use prod_synth_contract::Tier;

#[derive(Parser)]
#[command(
    name = "prod-synth",
    version,
    about = "Deterministically compile an anonymized workload profile into a portable, synthetic, production-shaped Parquet fixture.",
    long_about = "prod-synth turns the anonymized aggregates in a profile directory into a \
deterministic synthetic event fixture.\n\n\
The output is SYNTHETIC and PROFILE-DERIVED. It reproduces the geometry of an observed \
workload — tenant activity skew, part-to-tenant membership, key sparsity — and nothing else. \
It carries no production rows, no real tenant identifiers, and no real column values. It is \
not a production copy, and no report drawn from it may present it as one.\n\n\
The tool is offline: it never contacts a network, a database, or an object store."
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Parse and validate a profile, and write its summary.
    Profile {
        /// The profile directory (table.json, show-create.sql, part-geometry.jsonl,
        /// tenant-fanout.jsonl).
        #[arg(long)]
        profile: PathBuf,
        /// Where to write the JSON summary. Required: a report with nowhere to go
        /// is a report nobody reads.
        #[arg(long)]
        report: PathBuf,
    },
    /// Compile a profile into a fixture: manifest.json, topology.json, parquet/.
    Generate {
        #[arg(long)]
        profile: PathBuf,
        /// Output directory. No default — the tool has no idea where your
        /// repository is, and should not.
        #[arg(long)]
        output: PathBuf,
        #[arg(long, default_value = "baseline")]
        tier: Tier,
        #[arg(long)]
        tenants: Option<u64>,
        #[arg(long)]
        rows_per_shard: Option<u64>,
        #[arg(long)]
        shards: Option<u32>,
        #[arg(long)]
        seed: Option<u64>,
        /// Replace an existing fixture at exactly this path.
        #[arg(long)]
        replace: bool,
        /// Report the fidelity gates but do not fail on them. For deliberately
        /// off-distribution runs (a tiny smoke fixture cannot reproduce a
        /// distribution, and pretending otherwise would be theatre).
        #[arg(long)]
        no_gate: bool,
    },
    /// Offline structural, digest, footer, and census verification of a fixture.
    Verify {
        /// Path to manifest.json.
        manifest: PathBuf,
    },
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("prod-synth: {e:#}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Profile { profile, report } => cmd_profile(&profile, &report),
        Command::Generate {
            profile,
            output,
            tier,
            tenants,
            rows_per_shard,
            shards,
            seed,
            replace,
            no_gate,
        } => cmd_generate(
            &profile,
            &output,
            tier,
            tenants,
            rows_per_shard,
            shards,
            seed,
            replace,
            !no_gate,
        ),
        Command::Verify { manifest } => cmd_verify(&manifest),
    }
}

fn load(dir: &std::path::Path) -> Result<ProductionProfile> {
    ProductionProfile::load(dir)
        .with_context(|| format!("reading the profile in {}", dir.display()))
}

fn cmd_profile(dir: &std::path::Path, report: &std::path::Path) -> Result<()> {
    let profile = load(dir)?;
    let summary = profile.summary();

    println!("profile: {}", dir.display());
    println!(
        "  {} parts ({} multi-key), {} tenant samples, {} memberships",
        summary.parts, summary.multi_key_parts, summary.tenant_samples, summary.memberships
    );
    println!(
        "  exact_parts p50/p90 {:.0}/{:.0}   range_parts p50/p90 {:.0}/{:.0}   overfetch p50/p90 {:.3}/{:.0}",
        summary.exact_parts.p50,
        summary.exact_parts.p90,
        summary.range_parts.p50,
        summary.range_parts.p90,
        summary.range_overfetch.p50,
        summary.range_overfetch.p90,
    );
    println!(
        "  tenant_rows p50/p90/p99 {:.0}/{:.0}/{:.0}   key_density p10/p50/p90 {:.8}/{:.8}/{:.8}",
        summary.tenant_rows.p50,
        summary.tenant_rows.p90,
        summary.tenant_rows.p99,
        summary.key_density.p10,
        summary.key_density.p50,
        summary.key_density.p90,
    );
    println!(
        "  estimated active tenants: {} = {} / {:.6}",
        summary.estimated_active_tenants, summary.memberships, summary.mean_exact_parts
    );
    println!("\ncaveats:");
    for a in profile.assumptions() {
        println!("  - {a}");
    }

    let json = serde_json::json!({
        "profile_dir": dir.display().to_string(),
        "files": profile.files,
        "summary": summary,
        "assumptions": profile.assumptions(),
    });
    std::fs::write(report, serde_json::to_vec_pretty(&json)?)
        .with_context(|| format!("writing {}", report.display()))?;
    println!("\nwrote {}", report.display());
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn cmd_generate(
    dir: &std::path::Path,
    output: &std::path::Path,
    tier: Tier,
    tenants: Option<u64>,
    rows_per_shard: Option<u64>,
    shards: Option<u32>,
    seed: Option<u64>,
    replace: bool,
    gate: bool,
) -> Result<()> {
    let profile = load(dir)?;

    // Every override is recorded, so a manifest can never claim to be a stock tier
    // when it is not.
    let mut overrides = Vec::new();
    if tenants.is_some() {
        overrides.push("tenants".into());
    }
    if rows_per_shard.is_some() {
        overrides.push("rows_per_shard".into());
    }
    if shards.is_some() {
        overrides.push("shards".into());
    }
    if seed.is_some() {
        overrides.push("seed".into());
    }
    if !gate {
        overrides.push("no_gate".into());
    }

    // `shape`'s tenant count is the profile's own estimate, resolved here against
    // the profile in hand rather than baked into the binary as a stale constant.
    let tenants = tenants
        .or_else(|| tier.default_tenants())
        .unwrap_or_else(|| profile.estimated_active_tenants());

    let config = TopologyConfig {
        tier,
        tenants,
        rows_per_shard: rows_per_shard.unwrap_or_else(|| tier.default_rows_per_shard()),
        shards: shards.unwrap_or(1),
        seed: seed.unwrap_or(0),
        overrides,
    };

    println!(
        "compiling tier {} — {} tenants, {} rows/shard, {} shard(s), seed {}",
        config.tier.as_str(),
        config.tenants,
        config.rows_per_shard,
        config.shards,
        config.seed
    );

    let topo = topology::compile(&profile, &config).context("compiling the topology")?;
    let s = &topo.summary;
    println!(
        "  {} parts, {} memberships, key universe {}, {} degree repairs",
        topo.parts.len(),
        s.memberships,
        topo.key_universe,
        s.degree_repairs,
    );

    // Row density against the source's, and the floor's share of the population.
    // Not gates — see `fidelity::check` — but the two numbers that explain the
    // activity gates, so they are printed next to them rather than buried in JSON.
    let src = profile.summary();
    println!(
        "  {:.0} rows per membership (the source carries {:.0}); {} of {} tenants ({:.1}%) take \
         their row count from the one-row-per-membership floor rather than their activity weight",
        s.rows as f64 / s.memberships.max(1) as f64,
        src.observed_rows as f64 / src.memberships.max(1) as f64,
        s.floor_pinned_tenants,
        s.tenants,
        s.floor_pinned_tenants as f64 / s.tenants.max(1) as f64 * 100.0,
    );

    // Print the gates before deciding on them: evidence first, verdict second.
    let checks = fidelity::check(&profile.summary(), &topo.summary);
    println!("\n{}", fidelity::render(&checks));

    let out = generate::generate(&profile, &topo, output, replace, gate)
        .context("writing the fixture")?;

    println!(
        "wrote {} — {} parts, {} rows, {} bytes",
        out.dir.display(),
        out.manifest.parts.len(),
        out.manifest.generated.rows,
        out.manifest.generated.bytes,
    );
    println!("{}", out.manifest.disclaimer);
    Ok(())
}

fn cmd_verify(manifest: &std::path::Path) -> Result<()> {
    let v =
        verify::verify(manifest).with_context(|| format!("verifying {}", manifest.display()))?;
    println!(
        "verified {} — {} parts, {} rows, {} bytes",
        manifest.display(),
        v.parts,
        v.rows,
        v.bytes
    );
    println!(
        "  generator {} {} ({}), seed {}, tier {}",
        v.manifest.generator.name,
        v.manifest.generator.version,
        v.manifest.generator.rng,
        v.manifest.config.seed,
        v.manifest.config.tier.as_str(),
    );
    println!("  topology digest {}", v.manifest.topology.digest);
    for f in &v.manifest.source.files {
        println!("  profile {} {}", f.path, f.digest);
    }

    let failed: Vec<_> = v.manifest.fidelity.iter().filter(|c| !c.pass).collect();
    if !failed.is_empty() {
        // Not an error: a fixture may legitimately have been generated with
        // --no-gate. But it must never pass silently.
        println!(
            "\nWARNING: this fixture was published with {} failing fidelity gate(s):",
            failed.len()
        );
        for c in failed {
            println!(
                "  {} — source {} generated {}",
                c.name, c.source, c.generated
            );
        }
    }
    println!("\n{}", v.manifest.disclaimer);
    Ok(())
}
