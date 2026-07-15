#!/usr/bin/env python3
"""Plan 47 (Task 47E): plan and close a reproducible interleaved matrix run set.

`parquet-lab.sh` executes ONE declared block/repetition. This planner owns the *schedule*:
it emits a `ukiel-parquet-run-set/v1` with a seeded interleaved order — the product control
at the beginning and end of each repetition — recorded before execution, and later binds
every produced report so only a *complete* run set is analyzable. It implements no rewrite,
census, query, upload, or analysis logic.

    parquet-lab-run-set.py plan  --block DIR --backend NAME --repetitions 2 --seed N --out RUNSET.json
    parquet-lab-run-set.py close --run-set RUNSET.json --reports REP0_DIR REP1_DIR ... --out COMPLETE.json

Digests inside the run set are computed with SHA-256 consistently by plan/close and the
analyzer, so binding is internally consistent without a blake3 dependency; the suite/control
identity is cross-checked from the reports' own recorded digests at close.
"""
import argparse
import glob
import hashlib
import json
import os
import random
import sys

VERSION = "ukiel-parquet-run-set/v1"


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def spec_labels(block_dir):
    labels = [os.path.splitext(os.path.basename(p))[0] for p in sorted(glob.glob(os.path.join(block_dir, "*.toml")))]
    if not labels:
        raise SystemExit(f"error: block '{block_dir}' holds no *.toml specs")
    return labels


def plan(args):
    labels = spec_labels(args.block)
    schedule = []
    order = 0
    for rep in range(args.repetitions):
        # A seeded per-repetition order of the variants, framed by the control at both ends
        # so machine drift within a repetition is visible.
        rng = random.Random(args.seed + rep)
        variants = labels[:]
        rng.shuffle(variants)
        sequence = ["product-control", *variants, "product-control"]
        for label in sequence:
            kind = "control" if label == "product-control" else "variant"
            schedule.append({
                "repetition": rep,
                "order_index": order,
                "artifact_kind": kind,
                "label": label,
                "artifact_digest": "",  # filled at close from the report identity
                "expected_report_id": f"rep{rep}/order{order}/{label}",
                "report_digest": None,
            })
            order += 1

    run_set = {
        "run_set_version": VERSION,
        "state": "planned",
        "suite_digest": "",
        "control_digest": "",
        "block": args.block,
        "backend": args.backend,
        "reader_config": {},
        "host": {},
        "repetitions": args.repetitions,
        "seed": args.seed,
        "schedule": schedule,
    }
    write_json(args.out, run_set)
    print(f"planned {len(schedule)} scheduled measurements over {args.repetitions} repetition(s) -> {args.out}")


def close(args):
    run_set = json.load(open(args.run_set))
    if run_set.get("run_set_version") != VERSION:
        raise SystemExit("error: not a ukiel-parquet-run-set/v1")
    # Reports live under per-repetition run directories: reports[rep]/bench/<label>.json.
    reps = args.reports
    if len(reps) != run_set["repetitions"]:
        raise SystemExit(f"error: expected {run_set['repetitions']} report dirs, got {len(reps)}")

    suite_digests, control_digests = set(), set()
    missing = []
    for entry in run_set["schedule"]:
        rep_dir = reps[entry["repetition"]]
        report = os.path.join(rep_dir, "bench", f"{entry['label']}.json")
        if not os.path.isfile(report):
            missing.append(entry["expected_report_id"])
            continue
        entry["report_digest"] = sha256_file(report)
        ident = json.load(open(report)).get("identity", {})
        if ident.get("suite_digest"):
            suite_digests.add(ident["suite_digest"])
        if entry["artifact_kind"] == "control" and ident.get("snapshot_digest"):
            control_digests.add(ident["snapshot_digest"])
            entry["artifact_digest"] = ident["snapshot_digest"]
        elif ident.get("variant_digest"):
            entry["artifact_digest"] = ident["variant_digest"]

    if missing:
        run_set["state"] = "failed"
        write_json(args.out, run_set)
        raise SystemExit(f"error: run set is INCOMPLETE ({len(missing)} missing): {missing[:5]}...")
    if len(suite_digests) > 1:
        raise SystemExit(f"error: reports disagree on the suite digest: {suite_digests}")
    if len(control_digests) != 1:
        raise SystemExit(f"error: reports disagree on the control (snapshot) digest: {control_digests}")

    run_set["suite_digest"] = next(iter(suite_digests), "")
    run_set["control_digest"] = next(iter(control_digests))
    run_set["state"] = "complete"
    write_json(args.out, run_set)
    print(f"closed: {len(run_set['schedule'])} reports bound; state=complete -> {args.out}")


def write_json(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w") as fh:
        json.dump(obj, fh, indent=2)
    os.replace(tmp, path)


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("plan")
    p.add_argument("--block", required=True)
    p.add_argument("--backend", required=True)
    p.add_argument("--repetitions", type=int, default=2)
    p.add_argument("--seed", type=int, required=True)
    p.add_argument("--out", required=True)
    p.set_defaults(func=plan)
    c = sub.add_parser("close")
    c.add_argument("--run-set", required=True)
    c.add_argument("--reports", nargs="+", required=True)
    c.add_argument("--out", required=True)
    c.set_defaults(func=close)
    args = ap.parse_args()
    args.func(args)


if __name__ == "__main__":
    sys.exit(main())
