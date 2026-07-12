//! ClickHouse reference for the official suites (plan 34, ClickHouse half).
//!
//! Plan 32 measured ukiel at ~2.1x bare DataFusion, and that number was
//! *unanchored*: 2.1x of what, in absolute terms? Is this machine fast? This
//! module answers it by running ClickBench's 43 queries on ClickHouse **on this
//! machine**, in three modes, each earning its keep:
//!
//! * **`official`** — upstream's **verbatim** 105-column MergeTree and its
//!   verbatim `queries.sql`. Zero adaptations; that is the point. Its total is
//!   held against ClickHouse's *published* ClickBench figure — the **machine
//!   calibration** that makes every other number in `bench/README.md`
//!   interpretable. If we are wildly off the published number, something is
//!   misconfigured and every other measurement is suspect.
//! * **`cb`** — the same 25 columns as ukiel's `hits_cb` fixture, with ukiel's
//!   sort key and day partitioning mirrored, on MergeTree. The apples-to-apples
//!   ceiling: what a purpose-built OLAP engine does with the identical logical
//!   table.
//! * **`parquet`** — ClickHouse reading **the same parquet files bare DataFusion
//!   reads**, via `file()`. No MergeTree, no native format. This is the most
//!   informative mode in the file: the only measurement where ClickHouse and
//!   DataFusion differ in *nothing but the engine* — same bytes, same substrate,
//!   same 25 columns, same queries — so it prices engine quality with no format
//!   advantage to either side. `cb` minus `parquet` is then what MergeTree's
//!   native format and primary index are worth.
//!
//! ClickHouse is driven over its **HTTP interface** (8123) with `reqwest`, which
//! is already a dependency; the native protocol's 9000 is deliberately not
//! exposed, since it collides with MinIO's default.

use std::path::PathBuf;
use std::time::Instant;

use anyhow::{Context, bail};

use crate::hits::{Cast, HITS_CB};
use crate::suite::{self, Session};

/// ClickBench's official row count for the full 100-file dataset. A load that
/// produces anything else is a broken fixture, and benchmarking it would be
/// worse than not benchmarking at all.
const CLICKBENCH_ROWS: i64 = 99_997_497;
const CLICKBENCH_QUERIES: usize = 43;

fn endpoint() -> String {
    std::env::var("UKIEL_BENCH_CLICKHOUSE").unwrap_or_else(|_| "http://127.0.0.1:8123".to_string())
}

/// ClickHouse over its HTTP interface, scoped to one database.
///
/// The database is not decoration: the three fixtures (`official`, `cb`,
/// `parquet`) would otherwise collide — `cb` and `parquet` both present a table
/// called `hits_cb`, and loading one would silently replace the other, leaving
/// the next run benchmarking a fixture nobody asked for. One database per
/// fixture makes that impossible instead of merely unlikely.
pub struct ClickHouseEngine {
    client: reqwest::Client,
    url: String,
    database: String,
}

impl ClickHouseEngine {
    pub fn new(url: &str, database: &str) -> Self {
        Self {
            client: reqwest::Client::builder()
                // The 105-column load is a single statement that runs for
                // minutes; the default timeout would kill it mid-insert.
                .timeout(std::time::Duration::from_secs(7200))
                .build()
                .expect("build http client"),
            url: url.to_string(),
            database: database.to_string(),
        }
    }

    /// POSTs `sql` and returns the raw response body, erroring on a non-2xx with
    /// ClickHouse's own message (which is specific and worth surfacing verbatim).
    async fn post(&self, sql: &str) -> anyhow::Result<String> {
        let resp = self
            .client
            .post(&self.url)
            .query(&[("database", self.database.as_str())])
            .body(sql.to_string())
            .send()
            .await
            .with_context(|| {
                format!(
                    "POST to ClickHouse at {} — is it up? `docker compose --profile bench up -d clickhouse`",
                    self.url
                )
            })?;
        let status = resp.status();
        let body = resp.text().await?;
        if !status.is_success() {
            bail!("clickhouse {status}: {}", body.trim());
        }
        Ok(body)
    }

    /// Runs a statement and returns its row count. SELECTs are wrapped in
    /// `FORMAT JSONCompact` so ClickHouse itself reports `rows`; DDL and INSERT
    /// return 0.
    pub async fn query(&self, sql: &str) -> anyhow::Result<usize> {
        let trimmed = sql.trim().trim_end_matches(';');
        let upper = trimmed.to_ascii_uppercase();
        if !(upper.starts_with("SELECT") || upper.starts_with("WITH")) {
            self.post(trimmed).await?;
            return Ok(0);
        }
        let body = self
            .post(&format!("{trimmed} FORMAT JSONCompact"))
            .await
            .with_context(|| {
                let head: String = trimmed.chars().take(120).collect();
                format!("query failed: {head}…")
            })?;
        let v: serde_json::Value = serde_json::from_str(&body)
            .with_context(|| format!("parse JSONCompact response: {}", body.trim()))?;
        Ok(v.get("rows").and_then(|r| r.as_u64()).unwrap_or(0) as usize)
    }

    /// The coldest ClickHouse can be made without sudo: drop its mark and
    /// uncompressed caches.
    ///
    /// This is deliberately **not** ClickBench's cold (upstream restarts the
    /// daemon and drops the OS page cache with sudo — we can do neither from an
    /// unprivileged container, and we do neither for ukiel either). It is cold
    /// in exactly the sense ukiel's "fresh session, empty metadata cache" is
    /// cold, which is what the comparison needs: rungs timed by different rules
    /// do not compare. The hot minimum is the headline for precisely this reason.
    pub async fn drop_caches(&self) -> anyhow::Result<()> {
        for stmt in ["SYSTEM DROP MARK CACHE", "SYSTEM DROP UNCOMPRESSED CACHE"] {
            self.post(stmt).await?;
        }
        Ok(())
    }
}

/// Which ClickHouse fixture a command targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Table {
    /// Upstream's verbatim 105-column MergeTree — the machine calibration.
    Official,
    /// ukiel's 25 columns, ukiel's sort key and day partitioning, on MergeTree.
    Cb,
    /// The raw parquet through `file()` — no MergeTree, no native format.
    Parquet,
}

impl Table {
    pub fn parse(s: &str) -> anyhow::Result<Self> {
        match s {
            "official" => Ok(Table::Official),
            "cb" => Ok(Table::Cb),
            "parquet" => Ok(Table::Parquet),
            other => bail!("unknown --table '{other}' (official | cb | parquet)"),
        }
    }

    /// The SQL table/view name the suite's queries target.
    pub fn name(self) -> &'static str {
        match self {
            Table::Official => "hits",
            Table::Cb | Table::Parquet => "hits_cb",
        }
    }

    /// One database per fixture — `cb` and `parquet` both call their table
    /// `hits_cb`, so sharing a database would have them overwrite each other.
    pub fn database(self) -> &'static str {
        match self {
            Table::Official => "bench_official",
            Table::Cb => "bench_cb",
            Table::Parquet => "bench_parquet",
        }
    }

    /// The vendored query file this table is run with.
    pub fn queries_path(self) -> PathBuf {
        match self {
            // Verbatim upstream — the whole point of the calibration mode.
            Table::Official => {
                PathBuf::from("bench/queries/clickbench/queries-clickhouse-official.sql")
            }
            // Our 25-column suite, translated to ClickHouse's dialect.
            Table::Cb | Table::Parquet => {
                PathBuf::from("bench/queries/clickbench/queries-clickhouse.sql")
            }
        }
    }

    pub fn engine_label(self) -> &'static str {
        match self {
            Table::Official => "clickhouse-official",
            Table::Cb => "clickhouse-mergetree",
            Table::Parquet => "clickhouse-parquet",
        }
    }
}

/// ClickHouse's column type for a `hits_cb` column.
///
/// ukiel has no integer narrower than `int64`, and the whole value of the `cb`
/// table is that it is the *same logical table* — same columns, same widths,
/// same sort key — so ClickHouse is not handed a narrower-and-therefore-cheaper
/// version of the data. `Int64`/`String` mirror ukiel exactly. (The `official`
/// table keeps upstream's narrow types, which is why it is the calibration
/// fixture and not the comparison one.)
fn ch_type(cast: Cast) -> &'static str {
    match cast {
        Cast::Utf8 => "String",
        _ => "Int64",
    }
}

/// The 25-column MergeTree, mirroring ukiel's physical organization: same sort
/// key `(CounterID, EventTime)`, same day partitioning. Rung F only means
/// anything if the *layout* matches and only the engine and format differ.
fn cb_ddl() -> String {
    let cols: Vec<String> = HITS_CB
        .cols
        .iter()
        .map(|c| format!("    \"{}\" {}", c.ukiel, ch_type(c.cast)))
        .collect();
    format!(
        "CREATE OR REPLACE TABLE hits_cb\n(\n{}\n)\nENGINE = MergeTree\n\
         PARTITION BY \"EventDate\"\nORDER BY (\"CounterID\", \"EventTime\")\n\
         SETTINGS index_granularity = 8192",
        cols.join(",\n")
    )
}

/// A view over the raw parquet via `file()` — the same bytes bare DataFusion
/// reads, projected to the same 25 columns so `SELECT *` (q24) means the same
/// thing on every rung.
fn parquet_view_ddl() -> String {
    let cols: Vec<String> = HITS_CB
        .cols
        .iter()
        .map(|c| {
            // The source's narrow ints and Binary strings widen to ukiel's
            // types, exactly as ukiel's loader does — so the engines are handed
            // the same values, not merely the same file.
            format!(
                "    CAST(\"{}\" AS {}) AS \"{}\"",
                c.src,
                ch_type(c.cast),
                c.ukiel
            )
        })
        .collect();
    format!(
        "CREATE OR REPLACE VIEW hits_cb AS\nSELECT\n{}\nFROM file('hits_*.parquet', Parquet)",
        cols.join(",\n")
    )
}

/// Upstream's verbatim DDL, with `--` comments stripped and the trailing `;`
/// removed — the HTTP interface takes one statement, and upstream's file carries
/// explanatory comments *after* its terminator.
fn read_official_ddl() -> anyhow::Result<String> {
    let p = PathBuf::from("bench/queries/clickbench/create-clickhouse-official.sql");
    let raw = std::fs::read_to_string(&p)
        .with_context(|| format!("read {} (vendored upstream DDL)", p.display()))?;
    let stripped: String = raw
        .lines()
        .map(|l| match l.find("--") {
            Some(i) => &l[..i],
            None => l,
        })
        .collect::<Vec<_>>()
        .join("\n");
    Ok(stripped.trim().trim_end_matches(';').to_string())
}

/// `bench clickhouse load --table official|cb [--files N]`.
///
/// Loads from `bench/datasets/hits/`, which is mounted read-only into the
/// container at ClickHouse's `user_files` — the files are read **in place**,
/// never copied and never re-fetched.
pub async fn load(table: Table, files: Option<usize>) -> anyhow::Result<()> {
    // The database must exist before the engine can be scoped to it.
    ClickHouseEngine::new(&endpoint(), "default")
        .query(&format!(
            "CREATE DATABASE IF NOT EXISTS {}",
            table.database()
        ))
        .await
        .context("ClickHouse not reachable — `docker compose --profile bench up -d clickhouse`")?;
    let ch = ClickHouseEngine::new(&endpoint(), table.database());

    // `file()` reads the mount directly, so `parquet` needs only its view.
    let glob = match files {
        // A capped smoke load: name the files explicitly rather than globbing.
        Some(n) => format!("hits_{{0..{}}}.parquet", n.saturating_sub(1)),
        None => "hits_*.parquet".to_string(),
    };

    match table {
        Table::Parquet => {
            ch.query(&parquet_view_ddl()).await?;
            println!(
                "clickhouse: hits_cb view over file('{glob}') — no MergeTree, no native format"
            );
        }
        Table::Official => {
            ch.query(&read_official_ddl()?).await?;
            println!("clickhouse: loading the verbatim 105-column MergeTree from {glob}");
            let start = Instant::now();
            // Upstream's own load statement (ClickBench `clickhouse/load`).
            ch.query(&format!(
                "INSERT INTO hits SELECT * FROM file('{glob}') SETTINGS max_insert_threads = {}",
                (num_threads() / 4).max(1)
            ))
            .await?;
            report_load("official", start, &ch, "hits", files).await?;
        }
        Table::Cb => {
            ch.query(&cb_ddl()).await?;
            println!(
                "clickhouse: loading the 25-column MergeTree (ukiel's sort key + day partitioning) from {glob}"
            );
            let start = Instant::now();
            let cols: Vec<String> = HITS_CB
                .cols
                .iter()
                .map(|c| {
                    format!(
                        "CAST(\"{}\" AS {}) AS \"{}\"",
                        c.src,
                        ch_type(c.cast),
                        c.ukiel
                    )
                })
                .collect();
            ch.query(&format!(
                "INSERT INTO hits_cb SELECT {} FROM file('{glob}') SETTINGS max_insert_threads = {}",
                cols.join(", "),
                (num_threads() / 4).max(1)
            ))
            .await?;
            report_load("cb", start, &ch, "hits_cb", files).await?;
        }
    }
    Ok(())
}

fn num_threads() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8)
}

/// Prints the load time and **asserts the row count** — a fixture that does not
/// hold exactly the rows ClickBench says it holds is not a baseline, it is a
/// bug wearing a baseline's clothes.
async fn report_load(
    what: &str,
    start: Instant,
    ch: &ClickHouseEngine,
    table: &str,
    files: Option<usize>,
) -> anyhow::Result<()> {
    let secs = start.elapsed().as_secs_f64();
    println!("PERF clickhouse/{what}/load_secs={secs:.1}");

    let body = ch
        .post(&format!("SELECT count() FROM {table} FORMAT TabSeparated"))
        .await?;
    let rows: i64 = body.trim().parse().context("parse count()")?;
    let expected = match files {
        None => CLICKBENCH_ROWS,
        Some(n) => 1_000_000 * n as i64, // each ClickBench file is exactly 1M rows
    };
    if rows != expected {
        bail!("clickhouse {table} holds {rows} rows, expected {expected} — the fixture is wrong");
    }
    println!("clickhouse {table}: {rows} rows ✓");
    Ok(())
}

/// `bench clickhouse run --table official|cb|parquet [--iters N] [--label L]`.
pub async fn run(table: Table, iters: usize, label: &str) -> anyhow::Result<()> {
    let ch = ClickHouseEngine::new(&endpoint(), table.database());
    let name = table.name();
    let body = ch
        .post(&format!("SELECT count() FROM {name} FORMAT TabSeparated"))
        .await
        .with_context(|| {
            format!("{name} not loaded — run `bench clickhouse load --table …` first")
        })?;
    let table_rows: i64 = body.trim().parse().context("parse count()")?;
    println!(
        "clickhouse {name}: {table_rows} rows ({})",
        match table {
            Table::Official => "verbatim 105-column MergeTree — the machine calibration",
            Table::Cb => "25-column MergeTree, ukiel's sort key and day partitioning",
            Table::Parquet =>
                "the same parquet bare DataFusion reads — engine quality, identical bytes",
        }
    );

    let queries = suite::load_queries(&table.queries_path(), CLICKBENCH_QUERIES)?;
    let url = endpoint();
    let db = table.database();
    let report = suite::run_suite(
        table.engine_label(),
        label,
        iters,
        table_rows,
        &queries,
        || async {
            // A "cold" session for ClickHouse is a fresh connection with the mark
            // and uncompressed caches dropped — see `drop_caches`.
            let engine = ClickHouseEngine::new(&url, db);
            engine.drop_caches().await?;
            Ok(Session::Http(engine))
        },
    )
    .await?;
    suite::finish(
        report,
        &format!("clickbench-{}-{label}.json", table.engine_label()),
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Rung F is only apples-to-apples if the physical organization matches
    /// ukiel's: same 25 columns, same widths, same sort key, same day partition.
    /// Hand ClickHouse a narrower table and the comparison silently becomes
    /// "ukiel's types vs ClickHouse's", which is not the question.
    #[test]
    fn cb_table_mirrors_ukiels_layout_and_columns() {
        let ddl = cb_ddl();
        assert!(
            ddl.contains(r#"ORDER BY ("CounterID", "EventTime")"#),
            "{ddl}"
        );
        assert!(ddl.contains(r#"PARTITION BY "EventDate""#), "{ddl}");
        for c in HITS_CB.cols {
            assert!(
                ddl.contains(&format!("\"{}\"", c.ukiel)),
                "{} missing",
                c.ukiel
            );
        }
        // ukiel has no integer narrower than int64; ClickHouse must not get one.
        assert!(!ddl.contains("Int16") && !ddl.contains("UInt16") && !ddl.contains("Int32"));
    }

    /// The parquet view must project exactly the columns every other rung
    /// projects, or q24's `SELECT *` compares a 25-column read to a 105-column one.
    #[test]
    fn parquet_view_projects_the_same_25_columns() {
        let ddl = parquet_view_ddl();
        for c in HITS_CB.cols {
            assert!(
                ddl.contains(&format!("AS \"{}\"", c.ukiel)),
                "{} missing",
                c.ukiel
            );
        }
        assert_eq!(ddl.matches(" AS \"").count(), 25);
        assert!(ddl.contains("file('hits_*.parquet', Parquet)"));
    }

    #[test]
    fn table_names_and_engine_labels() {
        assert_eq!(Table::parse("official").unwrap().name(), "hits");
        assert_eq!(Table::parse("cb").unwrap().name(), "hits_cb");
        assert_eq!(Table::parse("parquet").unwrap().name(), "hits_cb");
        assert!(Table::parse("nope").is_err());
    }
}
