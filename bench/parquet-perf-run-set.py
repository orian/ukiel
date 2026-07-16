#!/usr/bin/env python3
"""Plan 49: plan and close the paired parquet-performance schedule.

The framework compares exactly three artifact roles per dataset — a frozen product control,
a reconstruction control, and a one-variable ZSTD-6 child — under a small scenario pack.
This planner owns the *schedule*; `parquet-perf.sh` executes one entry, and
`parquet-perf-analyze.py` interprets a *complete* run set.

Discipline this planner enforces, before any variant report is read:

* two seeded, independently shuffled repetitions;
* each repetition BRACKETED by both the product and the reconstruction control at start and
  end (so control drift is two independent measurements, not one);
* every ZSTD-6 scenario PAIRED with the reconstruction scenario of the *same* id; and
* sample counts frozen from a CONTROL pilot — never derived from a variant result.

    parquet-perf-run-set.py plan  --experiment E.json --scenarios S.json --pilot P.json \
        --host H.json --build B.json --seed N [--repetitions 2] --out RUNSET.json
    parquet-perf-run-set.py close --run-set RUNSET.json --reports-dir DIR --out COMPLETE.json
"""
import argparse
import hashlib
import json
import os
import random
import sys

VERSION = "ukiel-parquet-perf-run-set/v1"
WARM_MIN = 7
WARM_TARGET_SECONDS = 3.0
WARM_CAP = 64
COLD_MIN = 3


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def read_json(path):
    with open(path) as fh:
        return json.load(fh)


def write_json(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w") as fh:
        json.dump(obj, fh, indent=2)
    os.replace(tmp, path)


def warm_count(control_median_seconds):
    """The warm sample count implied by a CONTROL median. Frozen before variants run."""
    if control_median_seconds <= 0:
        return min(WARM_MIN, WARM_CAP)
    import math
    needed = math.ceil(WARM_TARGET_SECONDS / control_median_seconds)
    return min(max(WARM_MIN, needed), WARM_CAP)


def freeze_sample_counts(scenarios, pilot):
    """Freeze the sample count of every scenario from the control pilot. A scenario with no
    control-pilot median cannot be sized — refusing here is what stops variant-first sizing."""
    counts = {}
    for s in scenarios:
        sid = s["id"]
        if sid not in pilot:
            raise SystemExit(
                f"error: scenario '{sid}' has no control-pilot median; sample counts must be "
                f"frozen from the control, never a variant"
            )
        median = pilot[sid].get("control_median_seconds")
        if median is None:
            raise SystemExit(f"error: pilot entry for '{sid}' has no control_median_seconds")
        counts[sid] = {
            "warm": warm_count(median),
            "cold": COLD_MIN,
            "sized_from": "control-pilot",
        }
    return counts


def build_plan(experiment, scenarios, pilot, host, build, seed, repetitions):
    variant_digests = experiment.get("variant_delta_digests") or []
    if len(variant_digests) != 1:
        raise SystemExit("error: Plan 49 binds exactly one variant delta (compression-zstd-6)")
    artifacts = {
        "product": experiment["product_digest"],
        "reconstruction": experiment["reconstruction_digest"],
        "zstd-6": variant_digests[0],
    }
    sample_counts = freeze_sample_counts(scenarios, pilot)
    scenario_ids = [s["id"] for s in scenarios]

    schedule = []
    for rep in range(repetitions):
        rng = random.Random(seed + rep)
        order = 0
        shuffled = scenario_ids[:]
        rng.shuffle(shuffled)

        def control(arm):
            nonlocal order
            e = {
                "repetition": rep, "order_index": order, "kind": "control",
                "arm": arm, "scenario_id": None,
                "artifact_digest": artifacts[arm],
                "expected_report_id": f"rep{rep}/order{order:03d}/control-{arm}",
                "report_digest": None,
            }
            order += 1
            schedule.append(e)

        # Opening bracket: both controls.
        control("product")
        control("reconstruction")
        # Paired scenario arms.
        for sid in shuffled:
            for arm in ("reconstruction", "zstd-6"):
                schedule.append({
                    "repetition": rep, "order_index": order, "kind": "paired",
                    "arm": arm, "scenario_id": sid,
                    "artifact_digest": artifacts[arm],
                    "expected_report_id": f"rep{rep}/order{order:03d}/{arm}-{sid}",
                    "report_digest": None,
                })
                order += 1
        # Closing bracket: both controls again.
        control("product")
        control("reconstruction")

    run_set = {
        "run_set_version": VERSION,
        "state": "planned",
        "experiment_digest": experiment.get("_digest", ""),
        "seed": seed,
        "repetitions": repetitions,
        "artifacts": artifacts,
        "scenarios": scenario_ids,
        "sample_counts": sample_counts,
        "host": host,
        "build": build,
        "schedule": schedule,
    }
    validate_plan(run_set)
    return run_set


def validate_plan(run_set):
    """Structural invariants a planned run set must satisfy."""
    sched = run_set["schedule"]
    if not sched:
        raise SystemExit("error: empty schedule")
    slots = set()
    ids = set()
    for e in sched:
        slot = (e["repetition"], e["order_index"])
        if slot in slots:
            raise SystemExit(f"error: duplicate schedule slot {slot}")
        slots.add(slot)
        if e["expected_report_id"] in ids:
            raise SystemExit(f"error: duplicate report id {e['expected_report_id']}")
        ids.add(e["expected_report_id"])

    for rep in range(run_set["repetitions"]):
        rep_entries = [e for e in sched if e["repetition"] == rep]
        controls = [e for e in rep_entries if e["kind"] == "control"]
        for arm in ("product", "reconstruction"):
            arm_ctrls = [e for e in controls if e["arm"] == arm]
            if len(arm_ctrls) < 2:
                raise SystemExit(
                    f"error: repetition {rep} is not bracketed by the {arm} control at both "
                    f"start and end (found {len(arm_ctrls)})"
                )
            # A bracket means one before every paired entry and one after.
            paired_orders = [e["order_index"] for e in rep_entries if e["kind"] == "paired"]
            if paired_orders:
                lo, hi = min(paired_orders), max(paired_orders)
                if not any(e["order_index"] < lo for e in arm_ctrls):
                    raise SystemExit(f"error: repetition {rep} {arm} control does not open the bracket")
                if not any(e["order_index"] > hi for e in arm_ctrls):
                    raise SystemExit(f"error: repetition {rep} {arm} control does not close the bracket")

        # Paired arms: every scenario must appear once as reconstruction and once as zstd-6,
        # and the two arms of a pair must name the SAME scenario id.
        for sid in run_set["scenarios"]:
            arms = sorted(e["arm"] for e in rep_entries if e["kind"] == "paired" and e["scenario_id"] == sid)
            if arms != ["reconstruction", "zstd-6"]:
                raise SystemExit(
                    f"error: repetition {rep} scenario '{sid}' is not paired reconstruction↔zstd-6 "
                    f"(arms {arms})"
                )

    # Sample counts must be frozen from the control pilot for every scenario.
    for sid in run_set["scenarios"]:
        sc = run_set["sample_counts"].get(sid)
        if not sc:
            raise SystemExit(f"error: scenario '{sid}' has no frozen sample count")
        if sc.get("sized_from") != "control-pilot":
            raise SystemExit(
                f"error: scenario '{sid}' sample count was not frozen from the control pilot "
                f"(sized_from={sc.get('sized_from')}) — variant-first sizing is refused"
            )


def plan(args):
    experiment = read_json(args.experiment)
    experiment["_digest"] = sha256_file(args.experiment)
    scenarios = read_json(args.scenarios)
    pilot = read_json(args.pilot)
    host = read_json(args.host)
    build = read_json(args.build)
    run_set = build_plan(experiment, scenarios, pilot, host, build, args.seed, args.repetitions)
    write_json(args.out, run_set)
    print(f"planned {len(run_set['schedule'])} entries "
          f"({args.repetitions} reps, {len(run_set['scenarios'])} scenarios paired, "
          f"controls bracketed) -> {args.out}")


def close(args):
    run_set = read_json(args.run_set)
    if run_set.get("run_set_version") != VERSION:
        raise SystemExit("error: not a ukiel-parquet-perf-run-set/v1")
    missing = []
    for e in run_set["schedule"]:
        rel = e["expected_report_id"].replace("/", os.sep) + ".json"
        path = os.path.join(args.reports_dir, rel)
        if not os.path.isfile(path):
            missing.append(e["expected_report_id"])
            continue
        e["report_digest"] = sha256_file(path)
    if missing:
        run_set["state"] = "failed"
        write_json(args.out, run_set)
        raise SystemExit(f"error: run set INCOMPLETE ({len(missing)} missing): {missing[:5]}")
    run_set["state"] = "complete"
    validate_plan(run_set)
    write_json(args.out, run_set)
    print(f"closed: {len(run_set['schedule'])} reports bound; state=complete -> {args.out}")


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("plan")
    for a in ("experiment", "scenarios", "pilot", "host", "build", "out"):
        p.add_argument(f"--{a}", required=True)
    p.add_argument("--seed", type=int, required=True)
    p.add_argument("--repetitions", type=int, default=2)
    p.set_defaults(func=plan)
    c = sub.add_parser("close")
    c.add_argument("--run-set", required=True)
    c.add_argument("--reports-dir", required=True)
    c.add_argument("--out", required=True)
    c.set_defaults(func=close)
    args = ap.parse_args()
    args.func(args)


if __name__ == "__main__":
    sys.exit(main())
