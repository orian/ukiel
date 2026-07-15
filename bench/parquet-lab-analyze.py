#!/usr/bin/env python3
"""Plan 47: derive the dominated / Pareto / workload-specific classification from ONE
block's raw reports.

This is analysis, not measurement — it reads the immutable JSON `bench/parquet-lab.sh`
wrote and computes the registered noise band and the decision-rule labels. It never mutates
a spec, re-runs a benchmark, or picks bytes; the chosen candidate stays an explicit digest a
human carries forward.

    bench/parquet-lab-analyze.py --run RUN_DIR [--noise-floor 0.05]

Noise band (per dimension) from the plan:

    noise = max(5%, 3 * MAD(control reps) / median(control reps))

A block run has one product-control result; its per-query warm iterations are the control
reps used for the time band. Size has no within-run reps, so the size band is the floor.
Publishable results interleave two runs (Task 8) — point --run at a merged directory, or run
this per block and compare, to get a real multi-rep band.
"""
import argparse
import glob
import json
import os
import statistics
import sys


def median(xs):
    return statistics.median(xs) if xs else 0.0


def mad(xs):
    if not xs:
        return 0.0
    m = median(xs)
    return median([abs(x - m) for x in xs])


def noise_band(reps, floor):
    m = median(reps)
    if m == 0:
        return floor
    return max(floor, 3.0 * mad(reps) / m)


def warm_all(qs):
    """All warm samples across every query, as one control-rep pool for the time band."""
    out = []
    for q in qs:
        out.extend(q.get("warm_ms", []))
    return out


def query_time(qs):
    """A single representative time for a report: sum of per-query warm medians."""
    return sum(q.get("warm_median_ms", 0.0) for q in qs)


def load(run):
    bench = {}
    for p in glob.glob(os.path.join(run, "bench", "*.json")):
        label = os.path.splitext(os.path.basename(p))[0]
        bench[label] = json.load(open(p))
    census = {}
    for p in glob.glob(os.path.join(run, "census", "*.json")):
        label = os.path.splitext(os.path.basename(p))[0]
        census[label] = json.load(open(p))
    return bench, census


def classify(size_delta, time_delta, size_band, time_band):
    size_better = size_delta < -size_band
    size_worse = size_delta > size_band
    time_better = time_delta < -time_band
    time_worse = time_delta > time_band
    if not (size_better or time_better) and (size_worse or time_worse):
        return "dominated"
    if (size_better or time_better) and not (size_worse or time_worse):
        return "pareto-candidate"
    if (size_better or time_better) and (size_worse or time_worse):
        return "workload-specific"
    return "no-demonstrated-change"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", required=True)
    ap.add_argument("--noise-floor", type=float, default=0.05)
    ap.add_argument("--json-out")
    args = ap.parse_args()

    bench, census = load(args.run)
    if "product-control" not in bench:
        print("error: no product-control bench report in the run", file=sys.stderr)
        return 2

    control_qs = bench["product-control"]["body"]["queries"]
    control_time = query_time(control_qs)
    control_bytes = census.get("product-control", {}).get("total_compressed_bytes", 0)

    time_band = noise_band(warm_all(control_qs), args.noise_floor)
    size_band = args.noise_floor  # no within-run size reps; the floor is the honest band

    rows = []
    for label, rep in sorted(bench.items()):
        if label == "product-control":
            continue
        # Every query must have matched the control before this variant is even eligible.
        matched = all(q.get("expected_match", False) for q in rep["body"]["queries"])
        vtime = query_time(rep["body"]["queries"])
        vbytes = census.get(label, {}).get("total_compressed_bytes", control_bytes)
        size_delta = (vbytes - control_bytes) / control_bytes if control_bytes else 0.0
        time_delta = (vtime - control_time) / control_time if control_time else 0.0
        label_class = "REJECTED-answer-changed" if not matched else classify(
            size_delta, time_delta, size_band, time_band
        )
        rows.append({
            "variant": label,
            "answers_match": matched,
            "size_delta_pct": round(size_delta * 100, 2),
            "time_delta_pct": round(time_delta * 100, 2),
            "classification": label_class,
        })

    print(f"control: {control_bytes} compressed bytes, {control_time:.2f} ms total warm")
    print(f"noise bands: size {size_band*100:.1f}%, time {time_band*100:.1f}%\n")
    print(f"{'variant':32} {'size Δ%':>8} {'time Δ%':>8}  classification")
    for r in rows:
        print(f"{r['variant']:32} {r['size_delta_pct']:>8} {r['time_delta_pct']:>8}  {r['classification']}")

    if args.json_out:
        json.dump(
            {
                "control_bytes": control_bytes,
                "control_time_ms": control_time,
                "size_band": size_band,
                "time_band": time_band,
                "variants": rows,
            },
            open(args.json_out, "w"),
            indent=2,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
