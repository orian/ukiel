#!/usr/bin/env python3
"""Plan 49 analyzer: rewrite bias is reported on its own, classification follows the
spec-fixed vocabulary, and binding validation refuses an incomplete cache receipt, an unknown
host, or a dirty build in publishable mode."""
import importlib.util
import os
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SPEC = importlib.util.spec_from_file_location(
    "perf_analyze", os.path.join(HERE, "..", "parquet-perf-analyze.py")
)
an = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(an)


class StatsTest(unittest.TestCase):
    def test_rewrite_bias_is_separate_and_signed(self):
        b = an.rewrite_bias(1000, 880)
        self.assertAlmostEqual(b["delta_pct"], -12.0)

    def test_paired_ratio(self):
        self.assertAlmostEqual(an.paired_ratio([10, 10], [8, 8]), 0.8)

    def test_p95_needs_enough_samples(self):
        self.assertIsNone(an.p95([1, 2, 3]))
        self.assertIsNotNone(an.p95(list(range(30))))


class ClassifyTest(unittest.TestCase):
    def base(self, **kw):
        m = {"rep_ratios": [1.0, 1.0], "byte_delta_pct": 0.0, "noise": 0.05,
             "writer_regression": False}
        m.update(kw)
        return m

    def test_opposite_repetition_directions_are_unstable(self):
        # One rep faster, the other slower, both beyond noise.
        self.assertEqual(an.classify(self.base(rep_ratios=[0.8, 1.2])), "unstable")

    def test_small_exact_column_win_is_storage_only(self):
        # Bytes shrink >5% but read time is flat within noise.
        self.assertEqual(
            an.classify(self.base(rep_ratios=[1.0, 1.01], byte_delta_pct=-12.0)),
            "storage-only",
        )

    def test_writer_regression_dominates(self):
        self.assertEqual(
            an.classify(self.base(rep_ratios=[0.9, 0.9], byte_delta_pct=-10.0,
                                  writer_regression=True)),
            "dominated",
        )

    def test_read_regression_dominates(self):
        self.assertEqual(an.classify(self.base(rep_ratios=[1.2, 1.2])), "dominated")

    def test_cache_only_win_is_cache_specific(self):
        self.assertEqual(
            an.classify(self.base(rep_ratios=[0.85, 0.85], byte_delta_pct=-12.0,
                                  cache_specific=True)),
            "cache-specific",
        )

    def test_true_pareto_result(self):
        self.assertEqual(
            an.classify(self.base(rep_ratios=[0.85, 0.88], byte_delta_pct=-12.0)),
            "pareto-candidate",
        )

    def test_no_change(self):
        self.assertEqual(an.classify(self.base(rep_ratios=[1.0, 1.01])),
                         "no-demonstrated-change")

    def test_every_class_is_in_the_fixed_vocabulary(self):
        for ratios in ([0.8, 1.2], [1.0, 1.0], [0.8, 0.8], [1.2, 1.2]):
            self.assertIn(an.classify(self.base(rep_ratios=ratios)), an.CLASSES)


class BindingTest(unittest.TestCase):
    def run_set(self, **over):
        rset = {
            "state": "complete",
            "repetitions": 1,
            "scenarios": ["l2-read"],
            "host": {"kernel": "6.17", "host_cpu": "x86-64"},
            "build": {"git_sha": "abc123", "dirty": False},
            "schedule": [
                {"repetition": 0, "order_index": 0, "kind": "paired", "arm": "reconstruction",
                 "scenario_id": "l2-read", "expected_report_id": "rep0/order000/reconstruction-l2-read"},
            ],
        }
        rset.update(over)
        return rset

    def reports(self, profile="local-os-warm", with_receipt=True):
        cache = {"receipt_digest": "d" * 64, "residency_after": 0.98} if with_receipt else {}
        return {
            "rep0/order000/reconstruction-l2-read": {
                "cache_profile": profile,
                "cache": cache,
                "samples": [{"wall_seconds": 0.1}],
            }
        }

    def test_incomplete_cache_receipt_is_refused(self):
        with self.assertRaises(SystemExit):
            an.validate_bindings(self.run_set(), self.reports(with_receipt=False), publishable=False)

    def test_unknown_host_refused_when_publishable(self):
        with self.assertRaises(SystemExit):
            an.validate_bindings(self.run_set(host={}), self.reports(), publishable=True)

    def test_dirty_build_refused_when_publishable(self):
        rset = self.run_set(build={"git_sha": "abc", "dirty": True})
        with self.assertRaises(SystemExit):
            an.validate_bindings(rset, self.reports(), publishable=True)

    def test_unknown_git_sha_refused_when_publishable(self):
        rset = self.run_set(build={"git_sha": "unknown", "dirty": False})
        with self.assertRaises(SystemExit):
            an.validate_bindings(rset, self.reports(), publishable=True)

    def test_a_complete_bound_run_set_validates(self):
        an.validate_bindings(self.run_set(), self.reports(), publishable=True)

    def test_only_complete_is_analyzable(self):
        with self.assertRaises(SystemExit):
            an.validate_bindings(self.run_set(state="planned"), self.reports(), publishable=False)


if __name__ == "__main__":
    unittest.main()
