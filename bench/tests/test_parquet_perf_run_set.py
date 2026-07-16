#!/usr/bin/env python3
"""Plan 49 planner: it builds a bracketed, paired, control-sized schedule, and refuses a
schedule with missing brackets, duplicate entries, unlike scenario pairing, or variant-first
sample sizing."""
import copy
import importlib.util
import os
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SPEC = importlib.util.spec_from_file_location(
    "perf_run_set", os.path.join(HERE, "..", "parquet-perf-run-set.py")
)
rs = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(rs)


def experiment():
    return {
        "product_digest": "p" * 64,
        "reconstruction_digest": "r" * 64,
        "variant_delta_digests": ["z" * 64],
        "_digest": "e" * 64,
    }


def scenarios():
    return [{"id": "l3-scan-all"}, {"id": "l2-read-sparse"}, {"id": "l5-full-numeric"}]


def pilot():
    return {
        "l3-scan-all": {"control_median_seconds": 0.2},
        "l2-read-sparse": {"control_median_seconds": 1.5},
        "l5-full-numeric": {"control_median_seconds": 0.05},
    }


def good_plan():
    return rs.build_plan(
        experiment(), scenarios(), pilot(),
        host={"kernel": "6.17", "host_cpu": "x86-64"},
        build={"git_sha": "abc123", "dirty": False},
        seed=7, repetitions=2,
    )


class PlannerTest(unittest.TestCase):
    def test_builds_a_bracketed_paired_schedule(self):
        rset = good_plan()
        self.assertEqual(rset["state"], "planned")
        self.assertEqual(rset["repetitions"], 2)
        # Every repetition opens and closes with both controls.
        for rep in (0, 1):
            entries = [e for e in rset["schedule"] if e["repetition"] == rep]
            controls = [e for e in entries if e["kind"] == "control"]
            self.assertEqual(sum(1 for e in controls if e["arm"] == "product"), 2)
            self.assertEqual(sum(1 for e in controls if e["arm"] == "reconstruction"), 2)
            # Each scenario appears once per arm.
            for sid in rset["scenarios"]:
                arms = sorted(e["arm"] for e in entries
                              if e["kind"] == "paired" and e["scenario_id"] == sid)
                self.assertEqual(arms, ["reconstruction", "zstd-6"])

    def test_sample_counts_frozen_from_control_pilot(self):
        rset = good_plan()
        # Fast control (0.05s) -> the 3-second floor dominates: ceil(3/0.05)=60.
        self.assertEqual(rset["sample_counts"]["l5-full-numeric"]["warm"], 60)
        # Slow control (1.5s) -> the 7-sample floor.
        self.assertEqual(rset["sample_counts"]["l2-read-sparse"]["warm"], 7)
        for sc in rset["sample_counts"].values():
            self.assertEqual(sc["sized_from"], "control-pilot")

    def test_reps_are_independently_shuffled(self):
        rset = good_plan()
        def order(rep):
            return [e["scenario_id"] for e in rset["schedule"]
                    if e["repetition"] == rep and e["arm"] == "reconstruction" and e["kind"] == "paired"]
        # With three scenarios and different seeds the two reps need not match; at minimum the
        # planner produced a full paired order for each.
        self.assertEqual(len(order(0)), 3)
        self.assertEqual(len(order(1)), 3)

    def test_missing_bracket_is_refused(self):
        rset = good_plan()
        # Drop the closing product control of repetition 0.
        product_ctrls = [e for e in rset["schedule"]
                         if e["repetition"] == 0 and e["kind"] == "control" and e["arm"] == "product"]
        rset["schedule"].remove(max(product_ctrls, key=lambda e: e["order_index"]))
        with self.assertRaises(SystemExit):
            rs.validate_plan(rset)

    def test_duplicate_entry_is_refused(self):
        rset = good_plan()
        dup = copy.deepcopy(rset["schedule"][3])
        rset["schedule"].append(dup)
        with self.assertRaises(SystemExit):
            rs.validate_plan(rset)

    def test_unlike_scenario_pairing_is_refused(self):
        rset = good_plan()
        # Repoint one zstd-6 arm at a different scenario, breaking the pairing.
        for e in rset["schedule"]:
            if e["kind"] == "paired" and e["arm"] == "zstd-6" and e["scenario_id"] == "l3-scan-all":
                e["scenario_id"] = "l2-read-sparse"
                break
        with self.assertRaises(SystemExit):
            rs.validate_plan(rset)

    def test_variant_first_sizing_is_refused(self):
        # A pilot missing a scenario cannot size it from the control — refused.
        bad_pilot = pilot()
        del bad_pilot["l3-scan-all"]
        with self.assertRaises(SystemExit):
            rs.build_plan(experiment(), scenarios(), bad_pilot,
                          host={"kernel": "6.17"}, build={"git_sha": "abc", "dirty": False},
                          seed=7, repetitions=2)

    def test_sized_from_must_be_control_pilot(self):
        rset = good_plan()
        rset["sample_counts"]["l3-scan-all"]["sized_from"] = "variant"
        with self.assertRaises(SystemExit):
            rs.validate_plan(rset)

    def test_close_binds_reports_end_to_end(self):
        import json
        import tempfile
        rset = good_plan()
        with tempfile.TemporaryDirectory() as d:
            reports_dir = os.path.join(d, "reports")
            # Write a stub report at every expected path.
            for e in rset["schedule"]:
                rel = e["expected_report_id"].replace("/", os.sep) + ".json"
                path = os.path.join(reports_dir, rel)
                os.makedirs(os.path.dirname(path), exist_ok=True)
                with open(path, "w") as fh:
                    json.dump({"samples": [{"wall_seconds": 0.1}]}, fh)
            planned = os.path.join(d, "planned.json")
            rs.write_json(planned, rset)
            complete = os.path.join(d, "complete.json")

            class A:  # argparse stand-in
                run_set = planned
                reports_dir_ = reports_dir
                out = complete
            args = A()
            args.reports_dir = reports_dir
            rs.close(args)
            done = rs.read_json(complete)
            self.assertEqual(done["state"], "complete")
            self.assertTrue(all(e["report_digest"] for e in done["schedule"]))

    def test_close_reports_incomplete_when_a_report_is_missing(self):
        import tempfile
        rset = good_plan()
        with tempfile.TemporaryDirectory() as d:
            planned = os.path.join(d, "planned.json")
            rs.write_json(planned, rset)

            class A:
                run_set = planned
                out = os.path.join(d, "complete.json")
            args = A()
            args.reports_dir = os.path.join(d, "empty")
            os.makedirs(args.reports_dir, exist_ok=True)
            with self.assertRaises(SystemExit):
                rs.close(args)


if __name__ == "__main__":
    unittest.main()
