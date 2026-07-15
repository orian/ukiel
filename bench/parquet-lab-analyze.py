#!/usr/bin/env python3
"""Plan 47 (Task 47F): classify a COMPLETE run set's registered repetitions.

This is analysis over immutable, *registered* reports — not a glob. It accepts one complete
`ukiel-parquet-run-set/v1`, loads only the reports it binds (verifying each report digest),
and derives the noise band from **like-for-like** suite totals: for warm iteration i it sums
that iteration's per-query latency into one suite total T_i, and takes median/MAD over the
T_i samples. It NEVER pools raw samples from different queries (a fast count and a slow scan
are not comparable). The 5% size floor is a decision threshold, not measured size noise.

    parquet-lab-analyze.py --run-set COMPLETE.json --reports rep0 rep1 [--json-out OUT.json]

Noise band (per the plan), applied to the warm suite total:

    time_band = 3 * MAD(control T_i pool) / median(control T_i pool)
    size_band = 0.05   # fixed decision threshold
"""
import argparse
import hashlib
import json
import os
import statistics
import sys

CONTROL = "product-control"


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def read_json(path):
    with open(path) as fh:
        return json.load(fh)


def median(xs):
    return statistics.median(xs) if xs else 0.0


def mad(xs):
    if not xs:
        return 0.0
    m = median(xs)
    return median([abs(x - m) for x in xs])


def suite_totals(report):
    """Like-for-like suite totals: T_i = sum over queries of warm_ms[i]. Returns [] with a
    reason if the warm-iteration counts are not equal across queries (incomparable)."""
    qs = report["body"]["queries"]
    if not qs:
        return [], "no_queries"
    lengths = {len(q.get("warm_ms", [])) for q in qs}
    if len(lengths) != 1 or 0 in lengths:
        return [], "unequal_warm_iterations"
    n = lengths.pop()
    return [sum(q["warm_ms"][i] for q in qs) for i in range(n)], None


def classify(size_delta, time_delta, size_band, time_band):
    size_better, size_worse = size_delta < -size_band, size_delta > size_band
    time_better, time_worse = time_delta < -time_band, time_delta > time_band
    if not (size_better or time_better) and (size_worse or time_worse):
        return "dominated"
    if (size_better or time_better) and not (size_worse or time_worse):
        return "pareto-candidate"
    if (size_better or time_better) and (size_worse or time_worse):
        return "workload-specific"
    return "no-demonstrated-change"


def report_path(rep_dir, entry):
    # Prefer the scheduled report_id (Plan 48); fall back to the label for older run sets.
    name = entry.get("report_id") or entry["label"]
    return os.path.join(rep_dir, "bench", f"{name}.json")


def load_reports(run_set, rep_dirs):
    """Load exactly the bound reports, verifying each digest. Returns {label: [(entry, report)]}.
    Each entry is a distinct scheduled measurement — the two control brackets of a repetition
    are two separate reports, never the same file loaded twice."""
    if run_set.get("state") != "complete":
        raise SystemExit("error: only a COMPLETE run set is analyzable")
    if len(rep_dirs) != run_set["repetitions"]:
        raise SystemExit(f"error: expected {run_set['repetitions']} report dirs, got {len(rep_dirs)}")
    by_label = {}
    exclusions = []
    for entry in run_set["schedule"]:
        path = report_path(rep_dirs[entry["repetition"]], entry)
        if not os.path.isfile(path):
            raise SystemExit(f"error: bound report missing: {entry['expected_report_id']}")
        if entry.get("report_digest") and sha256_file(path) != entry["report_digest"]:
            raise SystemExit(f"error: report {entry['expected_report_id']} does not match its bound digest")
        by_label.setdefault(entry["label"], []).append((entry, read_json(path)))
    return by_label, exclusions


def census_bytes(rep_dir, label):
    path = os.path.join(rep_dir, "census", f"{label}.json")
    if os.path.isfile(path):
        return read_json(path).get("total_compressed_bytes")
    return None


def analyze(run_set, rep_dirs):
    by_label, exclusions = load_reports(run_set, rep_dirs)
    if CONTROL not in by_label:
        raise SystemExit("error: the run set has no product control")

    # The control T_i pool across all its repetitions -> the noise band. Each control report
    # is a distinct measurement (the start and end brackets are separate files).
    control_pool = []
    per_control = {}  # (repetition, order_index) -> suite median
    for entry, rep in by_label[CONTROL]:
        ts, reason = suite_totals(rep)
        if reason:
            exclusions.append({"label": CONTROL, "reason": reason})
        control_pool.extend(ts)
        per_control[(entry["repetition"], entry["order_index"])] = median(ts)
    control_median = median(control_pool)
    time_band = (3.0 * mad(control_pool) / control_median) if control_median else 0.0
    size_band = 0.05

    # Start-to-end control drift per repetition: the first and last scheduled control.
    control_drift = []
    reps_seen = sorted({k[0] for k in per_control})
    for r in reps_seen:
        orders = sorted(o for (rr, o) in per_control if rr == r)
        if len(orders) >= 2:
            start = per_control[(r, orders[0])]
            end = per_control[(r, orders[-1])]
            drift = abs(end - start) / control_median if control_median else 0.0
            # A control that moves more than 5% start-to-end signals machine drift within the
            # repetition. (Comparing to the pooled `time_band` cannot work: two equal control
            # clusters make the pool's MAD exactly inflate the band past the drift, so the
            # drift can never "exceed" it — a fixed threshold is the honest flag here.)
            control_drift.append({
                "repetition": r,
                "start_ms": round(start, 3),
                "end_ms": round(end, 3),
                "drift_pct": round(drift * 100, 2),
                "exceeds_noise": drift > 0.05,
            })

    control_bytes = census_bytes(rep_dirs[0], CONTROL)

    variants = []
    for label, reps in sorted(by_label.items()):
        if label == CONTROL:
            continue
        pool = []
        matched = True
        for entry, rep in reps:
            ts, reason = suite_totals(rep)
            if reason:
                exclusions.append({"label": label, "reason": reason})
            pool.extend(ts)
            # Correctness gate: every query answer matched the control.
            if not all(q.get("expected_match", False) for q in rep["body"]["queries"]):
                matched = False
        v_median = median(pool)
        time_delta = (v_median - control_median) / control_median if control_median else 0.0
        v_bytes = census_bytes(rep_dirs[0], label)
        size_delta = ((v_bytes - control_bytes) / control_bytes) if (v_bytes and control_bytes) else 0.0
        cls = "REJECTED-answer-changed" if not matched else classify(size_delta, time_delta, size_band, time_band)
        variants.append({
            "variant": label,
            "answers_match": matched,
            "reps": len(reps),
            "time_median_ms": round(v_median, 3),
            "time_delta_pct": round(time_delta * 100, 2),
            "size_delta_pct": round(size_delta * 100, 2),
            "classification": cls,
        })

    return {
        "formulas": {
            "suite_total": "T_i = sum_q warm_ms[q][i]",
            "time_band": "3 * MAD(control T_i pool) / median(control T_i pool)",
            "size_band": "0.05 fixed decision threshold",
        },
        "seed": run_set["seed"],
        "repetitions": run_set["repetitions"],
        "order": [e["expected_report_id"] for e in run_set["schedule"]],
        "suite_digest": run_set.get("suite_digest", ""),
        "control_digest": run_set.get("control_digest", ""),
        "control_median_ms": round(control_median, 3),
        "control_bytes": control_bytes,
        "control_drift": control_drift,
        "time_band": time_band,
        "size_band": size_band,
        "exclusions": exclusions,
        "variants": variants,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-set", required=True)
    ap.add_argument("--reports", nargs="+", required=True)
    ap.add_argument("--json-out")
    args = ap.parse_args()
    run_set = read_json(args.run_set)
    result = analyze(run_set, args.reports)

    print(f"control: {result['control_bytes']} compressed bytes, {result['control_median_ms']} ms warm median")
    print(f"noise bands: size {result['size_band']*100:.1f}% (threshold), time {result['time_band']*100:.1f}%")
    for d in result["control_drift"]:
        flag = " EXCEEDS NOISE" if d["exceeds_noise"] else ""
        print(f"control drift rep {d['repetition']}: {d['start_ms']} -> {d['end_ms']} ms ({d['drift_pct']}%){flag}")
    if result["exclusions"]:
        print(f"exclusions: {result['exclusions']}")
    print(f"\n{'variant':32} {'size Δ%':>8} {'time Δ%':>8}  classification")
    for v in result["variants"]:
        print(f"{v['variant']:32} {v['size_delta_pct']:>8} {v['time_delta_pct']:>8}  {v['classification']}")

    if args.json_out:
        json.dump(result, open(args.json_out, "w"), indent=2)
    return 0


if __name__ == "__main__":
    sys.exit(main())
