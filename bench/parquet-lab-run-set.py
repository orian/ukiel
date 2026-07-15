#!/usr/bin/env python3
"""Plan 47/48: plan and close a reproducible interleaved matrix run set.

`parquet-lab.sh` executes ONE declared repetition of a run set; this planner owns the
*schedule*. `plan` emits a `ukiel-parquet-run-set/v1` with a seeded interleaved order — the
product control measured *independently* at the start AND end of each repetition — where
every scheduled entry has a unique `report_id` (its report filename). `close` resolves the
exact recorded path for each entry, verifies its digest, and binds it, so only a *complete*
run set is analyzable and the two control brackets are two distinct measurements.

    parquet-lab-run-set.py plan  --block DIR | --specs-from LIST \
        --backend NAME --repetitions 2 --seed N --out RUNSET.json
    parquet-lab-run-set.py close --run-set RUNSET.json --reports REP0_DIR REP1_DIR --out COMPLETE.json

Digests inside the run set are SHA-256 (computed consistently by plan/close and the
analyzer); the suite/control/variant identity is cross-checked from the reports at close.
"""
import argparse
import glob
import hashlib
import json
import os
import random
import sys

VERSION = "ukiel-parquet-run-set/v1"
CONTROL = "product-control"
DEFAULT_READER = {
    "enable_page_index": True,
    "pruning": True,
    "pushdown_filters": True,
    "reorder_filters": True,
    "bloom_filter_on_read": True,
}


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def sha256_bytes(b):
    return hashlib.sha256(b).hexdigest()


def read_json(path):
    with open(path) as fh:
        return json.load(fh)


# -- resolving the variant set ---------------------------------------------------

def labels_from_block(block_dir):
    specs = sorted(glob.glob(os.path.join(block_dir, "*.toml")))
    if not specs:
        raise SystemExit(f"error: block '{block_dir}' holds no *.toml specs")
    return [(os.path.splitext(os.path.basename(p))[0], p) for p in specs], None


def labels_from_specs(list_path):
    """Read an ordered, comment-capable list of TOML spec paths under
    bench/config/parquet-lab. Refuse missing/duplicate/absolute/traversing/non-TOML/outside
    paths and duplicate embedded labels."""
    base = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "config", "parquet-lab"))
    text = open(list_path).read()
    entries = []
    seen_paths = set()
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if os.path.isabs(line):
            raise SystemExit(f"error: absolute spec path '{line}' in {list_path}")
        if ".." in line.split("/"):
            raise SystemExit(f"error: traversing spec path '{line}' in {list_path}")
        if not line.endswith(".toml"):
            raise SystemExit(f"error: non-TOML spec path '{line}' in {list_path}")
        full = os.path.normpath(os.path.join(os.getcwd(), line))
        if not full.startswith(base + os.sep):
            raise SystemExit(f"error: spec '{line}' is outside bench/config/parquet-lab")
        if not os.path.isfile(full):
            raise SystemExit(f"error: spec '{line}' does not exist")
        if line in seen_paths:
            raise SystemExit(f"error: duplicate spec path '{line}' in {list_path}")
        seen_paths.add(line)
        label = spec_label(full, line)
        entries.append((label, line))
    if not entries:
        raise SystemExit(f"error: spec list '{list_path}' is empty")
    # A duplicate embedded label would collide two variants under one report_id.
    labels = [e[0] for e in entries]
    dupes = {l for l in labels if labels.count(l) > 1}
    if dupes:
        raise SystemExit(f"error: duplicate spec labels {sorted(dupes)} in {list_path}")
    list_digest = sha256_bytes(text.encode())
    return entries, list_digest


def spec_label(full_path, rel):
    """The label a spec declares, so the report_id matches what parquet-rewrite emits."""
    for line in open(full_path):
        s = line.strip()
        if s.startswith("label"):
            # label = "foo"
            val = s.split("=", 1)[1].strip().strip('"').strip("'")
            if not val:
                raise SystemExit(f"error: spec '{rel}' has an empty label")
            return val
    raise SystemExit(f"error: spec '{rel}' has no label")


# -- plan ------------------------------------------------------------------------

def report_id(order, label):
    return f"order-{order:03d}-{label}"


def plan(args):
    if bool(args.block) == bool(args.specs_from):
        raise SystemExit("error: pass exactly one of --block or --specs-from")
    if args.block:
        entries, list_digest = labels_from_block(args.block)
        source = {"kind": "block", "path": args.block}
    else:
        entries, list_digest = labels_from_specs(args.specs_from)
        source = {"kind": "specs_from", "path": args.specs_from, "list_sha256": list_digest}

    labels = [e[0] for e in entries]
    # label -> (spec path as given, absolute path). --specs-from paths are cwd-relative.
    def resolve(path):
        return os.path.normpath(os.path.join(os.getcwd(), path)) if args.specs_from else path
    spec_path = {label: path for label, path in entries}
    spec_digests = {label: sha256_file(resolve(path)) for label, path in entries}

    schedule = []
    for rep in range(args.repetitions):
        rng = random.Random(args.seed + rep)
        variants = labels[:]
        rng.shuffle(variants)
        sequence = [CONTROL, *variants, CONTROL]
        for order, label in enumerate(sequence):
            schedule.append({
                "repetition": rep,
                "order_index": order,
                "artifact_kind": "control" if label == CONTROL else "variant",
                "label": label,
                "report_id": report_id(order, label),
                "spec_path": spec_path.get(label, ""),
                "artifact_digest": "",
                "spec_digest": spec_digests.get(label, ""),
                "expected_report_id": f"rep{rep}/{report_id(order, label)}",
                "report_digest": None,
            })

    run_set = {
        "run_set_version": VERSION,
        "state": "planned",
        "suite_digest": "",
        "control_digest": "",
        "source": source,
        "backend": args.backend,
        "reader_config": DEFAULT_READER,
        "host": {},
        "repetitions": args.repetitions,
        "seed": args.seed,
        "schedule": schedule,
    }
    write_json(args.out, run_set)
    n_variants = len(labels)
    print(f"planned {len(schedule)} measurements ({n_variants} variants x {args.repetitions} reps, "
          f"control bracketed) -> {args.out}")


# -- close -----------------------------------------------------------------------

def close(args):
    run_set = read_json(args.run_set)
    if run_set.get("run_set_version") != VERSION:
        raise SystemExit("error: not a ukiel-parquet-run-set/v1")
    reps = args.reports
    if len(reps) != run_set["repetitions"]:
        raise SystemExit(f"error: expected {run_set['repetitions']} report dirs, got {len(reps)}")

    suite_digests = set()
    control_snapshot = set()
    variant_digests = {}  # label -> digest, must be stable across reps
    seen_control_reports = {}  # rep -> set of control report paths (must be distinct)
    missing = []

    for entry in run_set["schedule"]:
        rep_dir = reps[entry["repetition"]]
        path = os.path.join(rep_dir, "bench", f"{entry['report_id']}.json")
        if not os.path.isfile(path):
            missing.append(entry["expected_report_id"])
            continue
        entry["report_digest"] = sha256_file(path)
        report = read_json(path)
        ident = report.get("identity", {})

        # Backend and reader configuration must match the plan.
        if ident.get("backend") not in (None, run_set["backend"]):
            raise SystemExit(f"error: {entry['report_id']} backend {ident.get('backend')} != {run_set['backend']}")
        rflags = report.get("body", {}).get("reader_flags")
        if rflags is not None and rflags != run_set["reader_config"]:
            raise SystemExit(f"error: {entry['report_id']} reader flags differ from the planned reader config")
        # The report must carry the scheduled run order.
        if ident.get("run_order") not in (None, entry["order_index"]):
            raise SystemExit(f"error: {entry['report_id']} run_order {ident.get('run_order')} != {entry['order_index']}")

        if ident.get("suite_digest"):
            suite_digests.add(ident["suite_digest"])
        if entry["artifact_kind"] == "control":
            control_snapshot.add(ident.get("snapshot_digest"))
            entry["artifact_digest"] = ident.get("snapshot_digest", "")
            seen_control_reports.setdefault(entry["repetition"], set()).add(path)
        else:
            d = ident.get("variant_digest", "")
            entry["artifact_digest"] = d
            if entry["label"] in variant_digests and variant_digests[entry["label"]] != d:
                raise SystemExit(f"error: variant '{entry['label']}' resolved to two different digests across reps")
            variant_digests[entry["label"]] = d

    if missing:
        run_set["state"] = "failed"
        write_json(args.out, run_set)
        raise SystemExit(f"error: run set INCOMPLETE ({len(missing)} missing): {missing[:5]}...")

    # The start and end control of each repetition must be two distinct reports.
    for rep, paths in seen_control_reports.items():
        n_control = sum(1 for e in run_set["schedule"] if e["repetition"] == rep and e["artifact_kind"] == "control")
        if len(paths) != n_control:
            raise SystemExit(f"error: repetition {rep} has {n_control} scheduled controls but only "
                             f"{len(paths)} distinct control reports — the brackets collapsed to one file")
    if len(control_snapshot) != 1:
        raise SystemExit(f"error: control reports disagree on the snapshot digest: {control_snapshot}")
    if len(suite_digests) > 1:
        raise SystemExit(f"error: reports disagree on the suite digest: {suite_digests}")

    run_set["suite_digest"] = next(iter(suite_digests), "")
    run_set["control_digest"] = next(iter(control_snapshot))
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
    p.add_argument("--block")
    p.add_argument("--specs-from")
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
