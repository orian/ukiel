//! Physical-plan instruments for the stack-residual investigation (plan 37).
//!
//! Two things the ordinary tools could not tell us, and the reason this module
//! exists at all:
//!
//! * **`EXPLAIN` truncates.** Its file-group list prints five groups and then
//!   an ellipsis, and it never prints equivalence properties at all. The
//!   gap-note investigation concluded "textually identical plans" from that
//!   output — which was never evidence, because the part that could differ is
//!   the part that does not print. [`plan_report`] dumps the *complete*
//!   composition: every group, every file, every byte range, plus partitioning
//!   and orderings.
//! * **`EXPLAIN ANALYZE` aggregates.** It sums metrics across partitions, so a
//!   plan whose work is skewed onto four of twenty-four partitions looks
//!   identical to one that is perfectly balanced. [`analyze_report`] reads
//!   `Metric.partition()` and reports per-partition compute, skew, and achieved
//!   parallelism.
//!
//! Both run against either session — the ukiel operator session or the raw
//! DataFusion reference — because the whole method is differential: same bytes,
//! same binary, two stacks.

use std::sync::Arc;
use std::time::Instant;

use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;

/// One plan node, flattened for printing.
struct Node {
    depth: usize,
    name: String,
    partitions: usize,
    /// Equivalence-class orderings — what `EXPLAIN` never shows, and the whole
    /// reason H1 was unfalsifiable before this instrument existed.
    orderings: Vec<String>,
    /// `Some` when the node is a `DataSourceExec` over a `FileScanConfig`.
    scan: Option<ScanInfo>,
}

/// One file group of a scan: its files (path + byte range) and their total size.
struct Group {
    bytes: u64,
    files: Vec<(String, String)>,
}

struct ScanInfo {
    declared_ordering: Vec<String>,
    groups: Vec<Group>,
}

fn scan_info(exec: &DataSourceExec) -> Option<ScanInfo> {
    let cfg = exec.data_source().downcast_ref::<FileScanConfig>()?;
    let groups = cfg
        .file_groups
        .iter()
        .map(|g| {
            let files: Vec<(String, String)> = g
                .files()
                .iter()
                .map(|f| {
                    let range = f
                        .range
                        .as_ref()
                        .map(|r| format!("{}..{}", r.start, r.end))
                        .unwrap_or_else(|| "whole".to_string());
                    (f.object_meta.location.to_string(), range)
                })
                .collect();
            let bytes: u64 = g.files().iter().map(|f| f.object_meta.size).sum();
            Group { bytes, files }
        })
        .collect();
    Some(ScanInfo {
        declared_ordering: cfg.output_ordering.iter().map(|o| format!("{o}")).collect(),
        groups,
    })
}

fn walk(plan: &Arc<dyn ExecutionPlan>, depth: usize, out: &mut Vec<Node>) {
    let props = plan.properties();
    out.push(Node {
        depth,
        name: plan.name().to_string(),
        partitions: props.output_partitioning().partition_count(),
        orderings: props
            .equivalence_properties()
            .oeq_class()
            .iter()
            .map(|o| format!("{o}"))
            .collect(),
        // DF 54's `ExecutionPlan: Any`, with no `as_any()` — upcast to downcast.
        scan: (plan.as_ref() as &dyn std::any::Any)
            .downcast_ref::<DataSourceExec>()
            .and_then(scan_info),
    });
    for child in plan.children() {
        walk(child, depth + 1, out);
    }
}

/// Formats the complete plan: nodes, partition counts, equivalence orderings,
/// and — untruncated — every file group of every scan.
///
/// `max_files_per_group` bounds only the *per-file* listing (a 1,102-file group
/// would drown the group table); group counts and byte totals are never
/// truncated, and the cap is reported when it bites, so nothing silently
/// disappears the way `EXPLAIN`'s five-group cutoff did.
pub fn plan_report(plan: &Arc<dyn ExecutionPlan>, max_files_per_group: usize) -> String {
    let mut nodes = Vec::new();
    walk(plan, 0, &mut nodes);

    let mut s = String::new();
    for n in &nodes {
        let indent = "  ".repeat(n.depth);
        s.push_str(&format!(
            "{indent}{} [partitions={}]\n",
            n.name, n.partitions
        ));
        for o in &n.orderings {
            s.push_str(&format!("{indent}  eq-ordering: {o}\n"));
        }
        let Some(scan) = &n.scan else { continue };
        if scan.declared_ordering.is_empty() {
            s.push_str(&format!("{indent}  declared-ordering: <none>\n"));
        }
        for o in &scan.declared_ordering {
            s.push_str(&format!("{indent}  declared-ordering: {o}\n"));
        }
        let total_files: usize = scan.groups.iter().map(|g| g.files.len()).sum();
        let total_bytes: u64 = scan.groups.iter().map(|g| g.bytes).sum();
        s.push_str(&format!(
            "{indent}  file_groups: {} groups, {total_files} files, {:.1} MiB\n",
            scan.groups.len(),
            total_bytes as f64 / (1024.0 * 1024.0)
        ));
        // Size skew across groups is the H1 signal: 24 balanced groups execute
        // very differently from 24 groups where one holds most of the bytes.
        let sizes: Vec<u64> = scan.groups.iter().map(|g| g.bytes).collect();
        if let (Some(&mx), Some(&mn)) = (sizes.iter().max(), sizes.iter().min()) {
            let mean = total_bytes as f64 / sizes.len().max(1) as f64;
            s.push_str(&format!(
                "{indent}  group bytes: min={:.1} MiB max={:.1} MiB mean={:.1} MiB (max/mean={:.2})\n",
                mn as f64 / 1048576.0,
                mx as f64 / 1048576.0,
                mean / 1048576.0,
                if mean > 0.0 { mx as f64 / mean } else { 0.0 }
            ));
        }
        for (i, g) in scan.groups.iter().enumerate() {
            s.push_str(&format!(
                "{indent}    group[{i:>4}] {:>4} files {:>8.2} MiB\n",
                g.files.len(),
                g.bytes as f64 / 1048576.0
            ));
            for (path, range) in g.files.iter().take(max_files_per_group) {
                s.push_str(&format!("{indent}      {path} [{range}]\n"));
            }
            if g.files.len() > max_files_per_group {
                s.push_str(&format!(
                    "{indent}      … {} more files in this group (raise --max-files to see them)\n",
                    g.files.len() - max_files_per_group
                ));
            }
        }
    }
    s
}

/// Per-partition execution profile: what `EXPLAIN ANALYZE` averages away.
///
/// Executes the plan, then reads each node's `MetricsSet`, whose values carry a
/// `partition` label. Reports per-partition `output_rows` and `elapsed_compute`,
/// the **skew** (max/mean partition compute — a plan can be perfectly balanced
/// on paper and wildly skewed in practice), and **achieved parallelism**
/// (sum-of-compute / wall), which is the number the gap note could only estimate
/// from `/usr/bin/time`'s CPU%.
pub async fn analyze_report(
    ctx: &SessionContext,
    plan: Arc<dyn ExecutionPlan>,
) -> anyhow::Result<String> {
    use datafusion::physical_plan::metrics::MetricValue;

    let start = Instant::now();
    let batches = datafusion::physical_plan::collect(plan.clone(), ctx.task_ctx()).await?;
    let wall_ms = start.elapsed().as_secs_f64() * 1000.0;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    let mut nodes = Vec::new();
    walk(&plan, 0, &mut nodes);

    let mut s = String::new();
    let mut total_compute_ns: u128 = 0;

    fn collect_nodes(
        plan: &Arc<dyn ExecutionPlan>,
        depth: usize,
        out: &mut Vec<(usize, Arc<dyn ExecutionPlan>)>,
    ) {
        out.push((depth, plan.clone()));
        for c in plan.children() {
            collect_nodes(c, depth + 1, out);
        }
    }
    let mut flat = Vec::new();
    collect_nodes(&plan, 0, &mut flat);

    for (depth, node) in &flat {
        let Some(metrics) = node.metrics() else {
            continue;
        };
        let indent = "  ".repeat(*depth);
        // (partition, compute_ns, rows)
        let mut per_partition: std::collections::BTreeMap<usize, (u128, usize)> =
            std::collections::BTreeMap::new();
        for m in metrics.iter() {
            let Some(p) = m.partition() else { continue };
            let e = per_partition.entry(p).or_default();
            match m.value() {
                MetricValue::ElapsedCompute(t) => e.0 += t.value() as u128,
                MetricValue::OutputRows(c) => e.1 += c.value(),
                _ => {}
            }
        }
        if per_partition.is_empty() {
            continue;
        }
        let computes: Vec<u128> = per_partition.values().map(|(c, _)| *c).collect();
        let sum: u128 = computes.iter().sum();
        total_compute_ns += sum;
        let mean = sum as f64 / computes.len() as f64;
        let max = *computes.iter().max().unwrap_or(&0) as f64;
        s.push_str(&format!(
            "{indent}{} — {} partitions, compute {:.1} ms, skew max/mean {:.2}\n",
            node.name(),
            per_partition.len(),
            sum as f64 / 1e6,
            if mean > 0.0 { max / mean } else { 0.0 }
        ));
        // Busiest partitions first: skew is the story, not the roster.
        let mut rows_by: Vec<(usize, u128, usize)> = per_partition
            .iter()
            .map(|(p, (c, r))| (*p, *c, *r))
            .collect();
        rows_by.sort_by_key(|(_, c, _)| std::cmp::Reverse(*c));
        for (p, c, r) in rows_by.iter().take(8) {
            s.push_str(&format!(
                "{indent}    part[{p:>3}] compute {:>8.1} ms  rows {r:>10}\n",
                *c as f64 / 1e6
            ));
        }
        if rows_by.len() > 8 {
            s.push_str(&format!(
                "{indent}    … {} more partitions\n",
                rows_by.len() - 8
            ));
        }
    }

    let parallelism = (total_compute_ns as f64 / 1e6) / wall_ms.max(f64::MIN_POSITIVE);
    s.push_str(&format!(
        "\nwall {wall_ms:.1} ms | sum(elapsed_compute) {:.1} ms | achieved parallelism {parallelism:.2}x | {rows} rows\n",
        total_compute_ns as f64 / 1e6
    ));
    Ok(s)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::Schema;
    use datafusion::datasource::listing::PartitionedFile;
    use datafusion::datasource::object_store::ObjectStoreUrl;
    use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource};

    /// The instrument's whole reason to exist: it must print **every** group.
    /// `EXPLAIN` stops at five, which is how the earlier investigation concluded
    /// "identical plans" from output that could not have shown a difference.
    #[test]
    fn plan_report_prints_every_group_untruncated() {
        let schema = Arc::new(Schema::empty());
        let source = Arc::new(ParquetSource::new(schema.clone()));
        // Seven groups — more than EXPLAIN's cutoff — with deliberate size skew.
        let groups: Vec<FileGroup> = (0..7)
            .map(|i| {
                FileGroup::new(vec![PartitionedFile::new(
                    format!("f{i}.parquet"),
                    if i == 0 { 1000 } else { 100 },
                )])
            })
            .collect();
        let config = FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), source)
            .with_file_groups(groups)
            .build();
        let exec: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(config);

        let report = plan_report(&exec, 3);
        for i in 0..7 {
            assert!(
                report.contains(&format!("f{i}.parquet")),
                "group {i} missing — the report truncated, which is the bug it exists to prevent:\n{report}"
            );
        }
        assert!(report.contains("7 groups, 7 files"));
        // Skew must be surfaced, not averaged away: one group is 10x the others.
        assert!(report.contains("max/mean"), "{report}");
        assert!(report.contains("declared-ordering: <none>"), "{report}");
    }
}
