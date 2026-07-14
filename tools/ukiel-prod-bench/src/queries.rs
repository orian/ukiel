//! The query suite, and the equivalence check that has to pass before any of it is
//! timed.

use anyhow::{Context, Result, bail};
use arrow::array::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use datafusion::prelude::SessionContext;

/// The suite, embedded. A benchmark that reads its queries from a path relative to the
/// working directory is a benchmark that measures something different depending on where
/// you ran it from.
const SUITE: &str = include_str!("../../../bench/queries/prod-synth/queries.sql");

#[derive(Debug, Clone)]
pub struct Query {
    pub id: String,
    pub sql: String,
}

/// Split the suite into its statements, keeping the `qN` labels from the comments.
pub fn suite() -> Vec<Query> {
    let mut out = Vec::new();
    let mut current: Option<String> = None;
    let mut sql = String::new();

    for line in SUITE.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("-- q") {
            // `-- q3: top events.` -> id "q3"
            if let Some(id) = rest.split(':').next() {
                current = Some(format!("q{}", id.trim()));
            }
            continue;
        }
        if trimmed.starts_with("--") || trimmed.is_empty() {
            continue;
        }
        sql.push_str(line);
        sql.push('\n');
        if trimmed.ends_with(';') {
            if let Some(id) = current.take() {
                out.push(Query {
                    id,
                    sql: sql.trim().trim_end_matches(';').trim().to_string(),
                });
            }
            sql.clear();
        }
    }
    out
}

/// The raw-DataFusion equivalent of a scoped query: the same SQL, plus the `team_id`
/// predicate the scoped session applies for you.
///
/// The scoped arm never sees `team_id` — it cannot, the slice is a property of the
/// session. The raw arm has no session to carry a slice, so it has to say so out loud.
/// Getting this wrong in either direction makes the comparison meaningless: without the
/// predicate the raw arm answers a question about every tenant, and the two arms would
/// "disagree" for a reason that has nothing to do with Ukiel.
pub fn scoped_to_raw(sql: &str, tenant: i64) -> String {
    let lower = sql.to_lowercase();

    // Insert into the existing WHERE, or add one — before GROUP BY / ORDER BY / LIMIT,
    // which is the only place a predicate can legally go.
    if let Some(pos) = keyword(&lower, "where") {
        let after = pos + "where".len();
        let (head, tail) = sql.split_at(after);
        format!("{head} team_id = {tenant} AND{tail}")
    } else {
        let cut = ["group by", "order by", "limit"]
            .iter()
            .filter_map(|kw| keyword(&lower, kw))
            .min()
            .unwrap_or(sql.len());
        let (head, tail) = sql.split_at(cut);
        format!(
            "{} WHERE team_id = {tenant} {}",
            head.trim_end(),
            tail.trim_start()
        )
    }
}

/// Find `kw` as a whole word, not as a substring.
///
/// The obvious `sql.find(" where ")` is wrong twice over: the suite's SQL is
/// multi-line, so `WHERE` is preceded by a newline rather than a space and the search
/// misses it — which silently appends a *second* `WHERE` and produces SQL that does not
/// parse. And a naked `find("where")` would match inside an identifier. Both bugs are
/// invisible until a query fails to plan, which is exactly what happened.
fn keyword(haystack: &str, kw: &str) -> Option<usize> {
    let bytes = haystack.as_bytes();
    let mut from = 0;
    while let Some(rel) = haystack[from..].find(kw) {
        let at = from + rel;
        let before_ok = at == 0 || !is_word(bytes[at - 1]);
        let end = at + kw.len();
        let after_ok = end == bytes.len() || !is_word(bytes[end]);
        if before_ok && after_ok {
            return Some(at);
        }
        from = at + 1;
    }
    None
}

fn is_word(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b'$'
}

/// Run a query and collect its batches.
pub async fn run(ctx: &SessionContext, sql: &str) -> Result<Vec<RecordBatch>> {
    ctx.sql(sql)
        .await
        .with_context(|| format!("planning: {sql}"))?
        .collect()
        .await
        .with_context(|| format!("executing: {sql}"))
}

/// Two result sets are the same result set.
///
/// Compared as *rendered rows*, not as Arrow batches: the two arms legitimately produce
/// different batch boundaries and different physical string representations (`Utf8` vs
/// `Utf8View`, plan 39), and neither difference is an answer. What must match is the
/// rows, in order — and every query in the suite is deterministically ordered so that
/// "in order" means something.
pub fn same(a: &[RecordBatch], b: &[RecordBatch]) -> Result<()> {
    let rows = |batches: &[RecordBatch]| -> Result<String> {
        Ok(pretty_format_batches(batches)
            .context("rendering results")?
            .to_string())
    };
    let (ra, rb) = (rows(a)?, rows(b)?);
    if ra != rb {
        bail!(
            "scoped Ukiel and raw DataFusion disagree.\n\n--- scoped Ukiel ---\n{ra}\n\
             --- raw DataFusion ---\n{rb}\n\n\
             They read the same files and must answer the same question. A difference here is \
             either a pruning bug (a part that holds rows was skipped) or a scoping bug (rows \
             from another tenant leaked in). Neither is a performance result."
        );
    }
    Ok(())
}

/// Total rows across the batches — the number a report quotes.
pub fn row_count(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_suite_parses_into_six_labelled_queries() {
        let qs = suite();
        let ids: Vec<&str> = qs.iter().map(|q| q.id.as_str()).collect();
        assert_eq!(ids, vec!["q1", "q2", "q3", "q4", "q5", "q6"]);
        assert!(qs.iter().all(|q| !q.sql.is_empty()));
    }

    /// The `$` identifiers must survive intact. Unquoted, `mat_$lib` parses as `mat_`
    /// followed by a parameter placeholder — silently, and the query then measures
    /// something else entirely.
    #[test]
    fn the_dollar_identifiers_stay_quoted() {
        let qs = suite();
        let q5 = qs.iter().find(|q| q.id == "q5").expect("q5");
        assert!(
            q5.sql.contains("\"mat_$current_url\""),
            "the promoted-URL column must stay double-quoted: {}",
            q5.sql
        );
        let q6 = qs.iter().find(|q| q.id == "q6").expect("q6");
        assert!(q6.sql.contains("\"mat_$lib\""), "{}", q6.sql);
    }

    /// The scoped suite must never name the packing key. A query that did would be a
    /// query a real client could not write, and the isolation it measures would be its
    /// own rather than Ukiel's.
    #[test]
    fn no_query_in_the_scoped_suite_mentions_the_packing_key() {
        for q in suite() {
            assert!(
                !q.sql.to_lowercase().contains("team_id"),
                "{} names team_id; the scoped path supplies the slice, the SQL must not: {}",
                q.id,
                q.sql
            );
        }
    }

    /// And the raw reference adds exactly that predicate back — in a legal position.
    #[test]
    fn the_raw_reference_adds_the_tenant_predicate_where_it_belongs() {
        // No WHERE: the predicate is appended before GROUP BY.
        let raw = scoped_to_raw(
            "SELECT event, count(*) AS n FROM events GROUP BY event ORDER BY n DESC LIMIT 10",
            42,
        );
        assert!(raw.contains("WHERE team_id = 42"), "{raw}");
        let lower = raw.to_lowercase();
        assert!(
            lower.find("where").unwrap() < lower.find("group by").unwrap(),
            "the predicate must precede GROUP BY: {raw}"
        );

        // An existing WHERE: the predicate joins it rather than replacing it.
        let raw = scoped_to_raw(
            "SELECT count(*) FROM events WHERE timestamp >= 1 AND timestamp < 2",
            7,
        );
        assert!(raw.contains("team_id = 7 AND timestamp >= 1"), "{raw}");
        assert_eq!(raw.matches("WHERE").count(), 1, "one WHERE, not two: {raw}");
    }

    /// Every query in the **real, multi-line suite** rewrites into valid SQL with exactly
    /// one WHERE.
    ///
    /// The first version of this test used a hand-written single-line query, and passed
    /// while `scoped_to_raw` was searching for `" where "` with a leading space. The suite's
    /// SQL puts `WHERE` at the start of a line, so the search missed it and a *second*
    /// `WHERE` was appended — SQL that does not parse, discovered only when q2 failed to
    /// plan against a live DataFusion. A test that does not use the real inputs is a test
    /// that agrees with you.
    #[test]
    fn every_query_in_the_real_suite_rewrites_into_valid_sql() {
        for q in suite() {
            let raw = scoped_to_raw(&q.sql, 99);
            let lower = raw.to_lowercase();

            assert!(
                lower.contains("team_id = 99"),
                "{} lost its predicate: {raw}",
                q.id
            );
            assert_eq!(
                lower.matches("where").count(),
                1,
                "{}: exactly one WHERE, or the SQL does not parse:\n{raw}",
                q.id
            );
            for kw in ["group by", "order by", "limit"] {
                if let Some(at) = lower.find(kw) {
                    assert!(
                        lower.find("where").unwrap() < at,
                        "{}: WHERE must precede {kw}:\n{raw}",
                        q.id
                    );
                }
            }
        }
    }

    /// The multi-line shape that broke it, pinned directly.
    #[test]
    fn a_where_at_the_start_of_a_line_is_found_not_duplicated() {
        let sql = "SELECT count(*) AS events\nFROM events\nWHERE timestamp >= 1 AND timestamp < 2";
        let raw = scoped_to_raw(sql, 760);
        assert_eq!(
            raw.to_lowercase().matches("where").count(),
            1,
            "a newline before WHERE must not produce a second one: {raw}"
        );
        assert!(raw.contains("team_id = 760 AND timestamp >= 1"), "{raw}");
    }

    /// And a `where` inside an identifier is not a keyword.
    #[test]
    fn an_identifier_containing_where_is_not_mistaken_for_the_keyword() {
        let raw = scoped_to_raw("SELECT nowhere FROM events", 1);
        assert!(
            raw.contains("WHERE team_id = 1"),
            "`nowhere` is not a WHERE clause: {raw}"
        );
        assert_eq!(
            raw.to_lowercase().matches("where").count(),
            2,
            "one in the identifier, one real"
        );
    }
}
