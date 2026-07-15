#!/usr/bin/env python3
"""Synthetic-fixture tests for bench/parquet-lab-analyze.py (Task 47F).

Run: python3 -m unittest bench/tests/test_parquet_lab_analyze.py
"""
import hashlib
import importlib.util
import json
import os
import tempfile
import unittest

_HERE = os.path.dirname(os.path.abspath(__file__))
_SPEC = importlib.util.spec_from_file_location(
    "plab_analyze", os.path.join(_HERE, "..", "parquet-lab-analyze.py")
)
A = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(A)


def report(queries, snapshot="snap", variant=None, suite="su"):
    ident = {"suite_digest": suite, "snapshot_digest": snapshot}
    if variant:
        ident["variant_digest"] = variant
    return {"identity": ident, "body": {"queries": queries}}


def q(name, warm, match=True):
    return {"name": name, "warm_ms": warm, "expected_match": match}


def write_run(dirpath, reps, seed=7, control_end=None):
    """Build a Plan-48 run set: each repetition is [control, *variants, control] with the two
    control brackets written as DISTINCT reports (per-entry report_id). `reps` is a list over
    repetitions of {label: (report_dict, compressed_bytes)}; the `product-control` entry gives
    the *start* control. `control_end[r]` (optional) overrides that repetition's *end* control
    report so a start-to-end drift can be simulated. Returns (run_set, rep_dirs)."""
    rep_dirs = []
    schedule = []
    bytes_by_label = {}

    def emit(rd, r, order, label, kind, rep_report):
        rid = f"order-{order:03d}-{label}"
        bp = os.path.join(rd, "bench", f"{rid}.json")
        with open(bp, "w") as fh:
            json.dump(rep_report, fh)
        schedule.append({
            "repetition": r, "order_index": order, "artifact_kind": kind, "label": label,
            "report_id": rid, "spec_path": "", "artifact_digest": "", "spec_digest": "",
            "expected_report_id": f"rep{r}/{rid}",
            "report_digest": hashlib.sha256(open(bp, "rb").read()).hexdigest(),
        })

    for r, labels in enumerate(reps):
        rd = os.path.join(dirpath, f"rep{r}")
        os.makedirs(os.path.join(rd, "bench"))
        os.makedirs(os.path.join(rd, "census"))
        rep_dirs.append(rd)
        control_rep, control_bytes = labels["product-control"]
        variants = [(l, v) for l, v in labels.items() if l != "product-control"]
        # census (one file per label; the analyzer reads rep0's census).
        for label, (_rep, cb) in labels.items():
            with open(os.path.join(rd, "census", f"{label}.json"), "w") as fh:
                json.dump({"total_compressed_bytes": cb}, fh)
            bytes_by_label[label] = cb
        order = 0
        emit(rd, r, order, "product-control", "control", control_rep)  # start bracket
        order += 1
        for label, (rep_report, _cb) in variants:
            emit(rd, r, order, label, "variant", rep_report)
            order += 1
        end_rep = (control_end or {}).get(r, control_rep)
        emit(rd, r, order, "product-control", "control", end_rep)  # end bracket

    run_set = {
        "run_set_version": "ukiel-parquet-run-set/v1", "state": "complete",
        "suite_digest": "su", "control_digest": "snap", "source": {"kind": "test"},
        "backend": "local", "reader_config": {},
        "host": {}, "repetitions": len(reps), "seed": seed, "schedule": schedule,
    }
    return run_set, rep_dirs


class AnalyzeTests(unittest.TestCase):
    def test_suite_totals_are_like_for_like(self):
        rep = report([q("fast", [1, 2]), q("slow", [10, 20])])
        ts, reason = A.suite_totals(rep)
        self.assertIsNone(reason)
        self.assertEqual(ts, [11, 22])  # per-iteration sums, never pooling fast+slow

    def test_unequal_warm_counts_excluded(self):
        rep = report([q("a", [1, 2]), q("b", [3])])
        ts, reason = A.suite_totals(rep)
        self.assertEqual(ts, [])
        self.assertEqual(reason, "unequal_warm_iterations")

    def test_classification_thresholds(self):
        self.assertEqual(A.classify(-0.4, 0.0, 0.05, 0.1), "pareto-candidate")
        self.assertEqual(A.classify(0.4, 0.4, 0.05, 0.1), "dominated")
        self.assertEqual(A.classify(-0.4, 0.4, 0.05, 0.1), "workload-specific")
        self.assertEqual(A.classify(0.0, 0.0, 0.05, 0.1), "no-demonstrated-change")

    def test_end_to_end_pareto_and_noise_band(self):
        with tempfile.TemporaryDirectory() as d:
            control = (report([q("a", [10, 10]), q("b", [10, 10])], variant=None), 1000)
            # A variant: same time, much smaller -> pareto candidate.
            variant = (report([q("a", [10, 10]), q("b", [10, 10])], variant="v1"), 600)
            reps = [
                {"product-control": control, "smaller": variant},
                {"product-control": control, "smaller": variant},
            ]
            run_set, rep_dirs = write_run(d, reps)
            res = A.analyze(run_set, rep_dirs)
            self.assertEqual(res["control_median_ms"], 20.0)  # T_i = 10+10
            self.assertAlmostEqual(res["time_band"], 0.0)     # no variance in control
            v = res["variants"][0]
            self.assertEqual(v["variant"], "smaller")
            self.assertEqual(v["classification"], "pareto-candidate")
            self.assertEqual(v["size_delta_pct"], -40.0)

    def test_answer_mismatch_is_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            control = (report([q("a", [10, 10])]), 1000)
            bad = (report([q("a", [10, 10], match=False)], variant="v"), 600)
            run_set, rep_dirs = write_run(d, [{"product-control": control, "bad": bad}])
            res = A.analyze(run_set, rep_dirs)
            self.assertEqual(res["variants"][0]["classification"], "REJECTED-answer-changed")

    def test_a_tampered_report_is_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            control = (report([q("a", [10, 10])]), 1000)
            run_set, rep_dirs = write_run(d, [{"product-control": control}])
            # Tamper with a bound report after the run-set recorded its digest.
            with open(os.path.join(rep_dirs[0], "bench", "order-000-product-control.json"), "w") as fh:
                json.dump(report([q("a", [999])]), fh)
            with self.assertRaises(SystemExit):
                A.analyze(run_set, rep_dirs)

    def test_control_drift_uses_two_distinct_brackets(self):
        with tempfile.TemporaryDirectory() as d:
            start = (report([q("a", [10, 10])]), 1000)
            end = report([q("a", [12, 12])])  # end control slower than start -> drift
            run_set, rep_dirs = write_run(d, [{"product-control": start}], control_end={0: end})
            res = A.analyze(run_set, rep_dirs)
            self.assertEqual(len(res["control_drift"]), 1)
            drift = res["control_drift"][0]
            self.assertEqual(drift["start_ms"], 10.0)
            self.assertEqual(drift["end_ms"], 12.0)
            self.assertTrue(drift["exceeds_noise"])

    def test_opposing_repetitions_are_unstable(self):
        with tempfile.TemporaryDirectory() as d:
            ctrl = (report([q("a", [10, 10])]), 1000)
            faster = (report([q("a", [5, 5])], variant="v"), 1000)   # rep0: faster
            slower = (report([q("a", [15, 15])], variant="v"), 1000)  # rep1: slower
            reps = [
                {"product-control": ctrl, "v": faster},
                {"product-control": ctrl, "v": slower},
            ]
            run_set, rep_dirs = write_run(d, reps)
            res = A.analyze(run_set, rep_dirs)
            v = res["variants"][0]
            self.assertEqual(v["classification"], "unstable")
            self.assertIn(0, v["per_rep_time_delta_pct"])
            self.assertLess(v["per_rep_time_delta_pct"][0], 0)   # rep0 faster
            self.assertGreater(v["per_rep_time_delta_pct"][1], 0)  # rep1 slower

    def test_missing_census_yields_zero_size_delta_not_a_crash(self):
        with tempfile.TemporaryDirectory() as d:
            ctrl = (report([q("a", [10, 10])]), 1000)
            v = (report([q("a", [10, 10])], variant="v"), 500)
            run_set, rep_dirs = write_run(d, [{"product-control": ctrl, "v": v}])
            os.remove(os.path.join(rep_dirs[0], "census", "v.json"))
            res = A.analyze(run_set, rep_dirs)
            self.assertEqual(res["variants"][0]["size_delta_pct"], 0.0)

    def test_per_query_evidence_is_reported(self):
        with tempfile.TemporaryDirectory() as d:
            ctrl = (report([q("fast", [1, 1]), q("slow", [100, 100])]), 1000)
            v = (report([q("fast", [1, 1]), q("slow", [100, 100])], variant="v"), 1000)
            run_set, rep_dirs = write_run(d, [{"product-control": ctrl, "v": v}])
            res = A.analyze(run_set, rep_dirs)
            pq = {p["query"]: p for p in res["variants"][0]["per_query"]}
            self.assertEqual(set(pq), {"fast", "slow"})
            self.assertEqual(pq["slow"]["warm_median_ms"], 100.0)

    def test_a_non_complete_run_set_is_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            control = (report([q("a", [10, 10])]), 1000)
            run_set, rep_dirs = write_run(d, [{"product-control": control}])
            run_set["state"] = "planned"
            with self.assertRaises(SystemExit):
                A.analyze(run_set, rep_dirs)


if __name__ == "__main__":
    unittest.main()
