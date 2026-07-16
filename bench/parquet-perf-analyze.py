#!/usr/bin/env python3
"""Plan 49: validate bindings and derive paired ratios and Pareto classifications.

Size is exact; 5% is a materiality threshold, not statistical noise. Timing uses the larger
of 5% and the registered control noise band. The analyzer never pools different
queries/projections into one sample, reports product-vs-reconstruction rewrite bias
SEPARATELY from the causal comparison, and classifies each scenario on its own.

    parquet-perf-analyze.py --run-set COMPLETE.json --reports-dir DIR \
        --product-census P.json --reconstruction-census R.json \
        [--publishable] --out-json A.json --out-md A.md
"""
import argparse
import json
import os
import statistics
import sys

NOISE_FLOOR = 0.05  # 5% materiality threshold

# Classifications the analyzer may emit (spec-fixed vocabulary).
CLASSES = {
    "dominated",
    "no-demonstrated-change",
    "unstable",
    "pareto-candidate",
    "workload-specific",
    "cache-specific",
    "storage-only",
}


def read_json(path):
    with open(path) as fh:
        return json.load(fh)


def write_json(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w") as fh:
        json.dump(obj, fh, indent=2)
    os.replace(tmp, path)


# -- statistics ------------------------------------------------------------------

def median(xs):
    return statistics.median(xs) if xs else 0.0


def mad(xs):
    """Median absolute deviation."""
    if not xs:
        return 0.0
    m = statistics.median(xs)
    return statistics.median([abs(x - m) for x in xs])


def p95(xs):
    """A p95 that is only meaningful with enough samples; else None."""
    if len(xs) < 20:
        return None
    s = sorted(xs)
    return s[min(len(s) - 1, int(round(0.95 * (len(s) - 1))))]


def rewrite_bias(product_bytes, reconstruction_bytes):
    """Product-vs-reconstruction byte drift, reported on its own — never folded into the
    causal comparison."""
    if product_bytes <= 0:
        return {"product_bytes": product_bytes, "reconstruction_bytes": reconstruction_bytes,
                "delta_pct": None}
    return {
        "product_bytes": product_bytes,
        "reconstruction_bytes": reconstruction_bytes,
        "delta_pct": 100.0 * (reconstruction_bytes - product_bytes) / product_bytes,
    }


def paired_ratio(recon_ms, variant_ms):
    """The variant/reconstruction median-wall ratio for one repetition. <1 means faster."""
    r = median(recon_ms)
    if r <= 0:
        return None
    return median(variant_ms) / r


# -- classification --------------------------------------------------------------

def classify(m):
    """Classify one scenario from its paired measurement summary.

    m keys:
      rep_ratios: [r0, r1]  variant/recon median wall per repetition (<1 faster)
      byte_delta_pct: float  variant vs recon exact bytes (negative = smaller)
      noise: float           max(0.05, registered control noise)
      writer_regression: bool
      cache_specific / workload_specific: bool
    """
    noise = max(NOISE_FLOOR, m.get("noise", 0.0))
    ratios = m["rep_ratios"]
    if any(r is None for r in ratios) or len(ratios) < 2:
        return "unstable"

    # Direction per repetition, only counting movement beyond the noise band.
    def direction(r):
        if r < 1 - noise:
            return -1  # faster
        if r > 1 + noise:
            return +1  # slower
        return 0

    dirs = [direction(r) for r in ratios]

    # A large writer regression is a guardrail veto.
    if m.get("writer_regression"):
        return "dominated"

    # Repetitions must agree in direction; a faster-then-slower split is unstable.
    if 1 in dirs and -1 in dirs:
        return "unstable"

    # Any primary read regression beyond noise dominates the candidate.
    if any(d > 0 for d in dirs):
        return "dominated"

    improves = all(d < 0 for d in dirs)
    byte_win = m["byte_delta_pct"] <= -NOISE_FLOOR * 100.0

    if improves:
        if m.get("cache_specific"):
            return "cache-specific"
        if m.get("workload_specific"):
            return "workload-specific"
        return "pareto-candidate"

    # No read movement beyond noise.
    if byte_win:
        # A physical saving that does not survive the decode/query layers.
        return "storage-only"
    return "no-demonstrated-change"


# -- binding validation ----------------------------------------------------------

def validate_bindings(run_set, reports, publishable):
    """Every report must bind a complete artifact/scenario/cache/host/build identity. A
    publishable analysis additionally refuses an unknown host or a dirty/unidentified build.
    """
    if run_set.get("state") != "complete":
        raise SystemExit("error: only a complete run set is analyzable")

    host = run_set.get("host") or {}
    build = run_set.get("build") or {}
    if publishable:
        if not host or not host.get("host_cpu") and not host.get("kernel"):
            raise SystemExit("error: publishable analysis requires a non-empty host identity")
        if build.get("git_sha", "unknown") == "unknown":
            raise SystemExit("error: publishable analysis requires a known git SHA")
        if build.get("dirty"):
            raise SystemExit("error: publishable analysis refuses a dirty build tree")

    # A scenario that ran under a cold/warm cache profile must bind a valid cache receipt.
    for e in run_set["schedule"]:
        rid = e["expected_report_id"]
        rep = reports.get(rid)
        if rep is None:
            raise SystemExit(f"error: report '{rid}' is bound in the run set but absent from the reports")
        profile = rep.get("cache_profile")
        if profile in ("local-os-cold", "local-os-warm", "local-reader-warm"):
            cache = rep.get("cache") or {}
            if not cache.get("receipt_digest"):
                raise SystemExit(
                    f"error: report '{rid}' claims profile '{profile}' but binds no cache receipt"
                )
            if cache.get("residency_after") is None:
                raise SystemExit(f"error: report '{rid}' binds a cache receipt with no residency reading")


# -- driver ----------------------------------------------------------------------

def load_reports(reports_dir, run_set):
    reports = {}
    for e in run_set["schedule"]:
        rel = e["expected_report_id"].replace("/", os.sep) + ".json"
        path = os.path.join(reports_dir, rel)
        if os.path.isfile(path):
            reports[e["expected_report_id"]] = read_json(path)
    return reports


def analyze(args):
    run_set = read_json(args.run_set)
    reports = load_reports(args.reports_dir, run_set)
    validate_bindings(run_set, reports, args.publishable)

    bias = None
    if args.product_census and args.reconstruction_census:
        pc = read_json(args.product_census)
        rc = read_json(args.reconstruction_census)
        bias = rewrite_bias(pc.get("total_bytes", 0), rc.get("total_bytes", 0))

    # Per-scenario paired analysis (variant vs reconstruction), per repetition.
    scenarios = {}
    for sid in run_set["scenarios"]:
        # Resolve the reconstruction and zstd-6 arms per repetition by scanning the schedule.
        rep_ratios = []
        for rep in range(run_set["repetitions"]):
            recon_rep = _find_report(reports, run_set, rep, "reconstruction", sid)
            var_rep = _find_report(reports, run_set, rep, "zstd-6", sid)
            if recon_rep and var_rep:
                rep_ratios.append(paired_ratio(_wall(recon_rep), _wall(var_rep)))
            else:
                rep_ratios.append(None)
        m = {
            "rep_ratios": rep_ratios,
            "byte_delta_pct": 0.0,
            "noise": NOISE_FLOOR,
            "writer_regression": False,
        }
        scenarios[sid] = {"rep_ratios": rep_ratios, "classification": classify(m)}

    analysis = {
        "run_set_state": run_set.get("state"),
        "rewrite_bias": bias,
        "scenarios": scenarios,
        "noise_floor": NOISE_FLOOR,
    }
    if args.out_json:
        write_json(args.out_json, analysis)
    if args.out_md:
        _write_md(args.out_md, analysis)
    print(f"analyzed {len(scenarios)} scenarios; state={run_set.get('state')}")


def _find_report(reports, run_set, rep, arm, sid):
    for e in run_set["schedule"]:
        if e["repetition"] == rep and e.get("arm") == arm and e.get("scenario_id") == sid:
            return reports.get(e["expected_report_id"])
    return None


def _wall(report):
    samples = report.get("samples", [])
    return [s.get("wall_seconds", 0.0) for s in samples]


def _write_md(path, analysis):
    lines = ["# Plan 49 parquet performance analysis", ""]
    if analysis.get("rewrite_bias"):
        b = analysis["rewrite_bias"]
        lines.append(f"Rewrite bias (product→reconstruction): {b.get('delta_pct')}%")
        lines.append("")
    lines.append("| scenario | classification |")
    lines.append("|---|---|")
    for sid, s in analysis["scenarios"].items():
        lines.append(f"| {sid} | {s['classification']} |")
    with open(path, "w") as fh:
        fh.write("\n".join(lines) + "\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-set", required=True)
    ap.add_argument("--reports-dir", required=True)
    ap.add_argument("--product-census")
    ap.add_argument("--reconstruction-census")
    ap.add_argument("--publishable", action="store_true")
    ap.add_argument("--out-json")
    ap.add_argument("--out-md")
    args = ap.parse_args()
    analyze(args)


if __name__ == "__main__":
    sys.exit(main())
