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


def write_run(dirpath, reps, seed=7):
    """reps: list over repetitions of {label: (report_dict, compressed_bytes)}. Builds rep
    dirs, a complete run-set with correct sha256 digests, and returns (run_set, rep_dirs)."""
    rep_dirs = []
    schedule = []
    order = 0
    for r, labels in enumerate(reps):
        rd = os.path.join(dirpath, f"rep{r}")
        os.makedirs(os.path.join(rd, "bench"))
        os.makedirs(os.path.join(rd, "census"))
        rep_dirs.append(rd)
        for label, (rep, cbytes) in labels.items():
            bp = os.path.join(rd, "bench", f"{label}.json")
            json.dump(rep, open(bp, "w"))
            json.dump({"total_compressed_bytes": cbytes}, open(os.path.join(rd, "census", f"{label}.json"), "w"))
            digest = hashlib.sha256(open(bp, "rb").read()).hexdigest()
            schedule.append({
                "repetition": r, "order_index": order,
                "artifact_kind": "control" if label == "product-control" else "variant",
                "label": label, "artifact_digest": "",
                "expected_report_id": f"rep{r}/order{order}/{label}",
                "report_digest": digest,
            })
            order += 1
    run_set = {
        "run_set_version": "ukiel-parquet-run-set/v1", "state": "complete",
        "suite_digest": "su", "control_digest": "snap", "block": "b", "backend": "local",
        "reader_config": {}, "host": {}, "repetitions": len(reps), "seed": seed,
        "schedule": schedule,
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
            # Tamper with the report after the run-set bound its digest.
            with open(os.path.join(rep_dirs[0], "bench", "product-control.json"), "w") as fh:
                json.dump(report([q("a", [999])]), fh)
            with self.assertRaises(SystemExit):
                A.analyze(run_set, rep_dirs)

    def test_a_non_complete_run_set_is_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            control = (report([q("a", [10, 10])]), 1000)
            run_set, rep_dirs = write_run(d, [{"product-control": control}])
            run_set["state"] = "planned"
            with self.assertRaises(SystemExit):
                A.analyze(run_set, rep_dirs)


if __name__ == "__main__":
    unittest.main()
