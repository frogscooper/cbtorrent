import copy
from pathlib import Path
import runpy
import unittest


evaluate = runpy.run_path(str(Path(__file__).resolve().parents[1] /
                             "benchmarks" / "evaluate_time_budget.py"))["evaluate"]


class AcceptanceTests(unittest.TestCase):
    def fixture(self):
        scenarios = dict.fromkeys(("budget-static", "late-change", "sustained-delay",
                                   "three-slots", "serial-slow-peer", "before-midpoint"))
        policies = ["heuristic", "recovery", "timed", "ewma-probe"]
        rows = []
        for scenario in scenarios:
            for policy in policies:
                duration = 1.0 if policy == "heuristic" else 0.8
                if policy == "timed":
                    duration = 0.812 if scenario == "before-midpoint" else 0.78
                rows.append(dict(scenario=scenario, trial=0, policy=policy, metrics=dict(
                    complete=True, completion_seconds=duration, wire_sent_bytes=10,
                    wire_received_bytes=100, wasted_payload_bytes=0, connections=2,
                    policy_seconds=0.001, policy_update_seconds=0.001)))
        return dict(scenarios=scenarios, policies=policies, trials=1, runs=rows)

    def test_accepts_all_gates_and_calculates_retained_gain(self):
        result = evaluate(self.fixture())
        self.assertTrue(result["accepted"])
        self.assertAlmostEqual(result["retained_early_gain"], 0.94)
        self.assertAlmostEqual(result["best_stationary_gain"], 0.025)

    def test_failure_extra_bytes_slowdown_lost_recovery_and_cost_each_reject(self):
        for failure in ("incomplete", "traffic", "slowdown", "recovery", "cost"):
            report = self.fixture()
            row = next(r for r in report["runs"] if r["policy"] == "timed" and
                       r["scenario"] == ("before-midpoint" if failure == "recovery" else "budget-static"))
            if failure == "incomplete":
                row["metrics"]["complete"] = False
                row["metrics"]["completion_seconds"] = None
            elif failure == "traffic":
                row["metrics"]["wire_received_bytes"] += 1
            elif failure in ("slowdown", "recovery"):
                row["metrics"]["completion_seconds"] = 0.85
            else:
                for row in report["runs"]:
                    if row["policy"] == "timed":
                        row["metrics"]["policy_seconds"] = 0.01
            with self.subTest(failure=failure):
                self.assertFalse(evaluate(report)["accepted"])

    def test_missing_and_duplicate_pairs_are_rejected(self):
        for missing in (True, False):
            report = self.fixture()
            if missing:
                report["runs"].pop()
            else:
                report["runs"].append(copy.deepcopy(report["runs"][0]))
            with self.assertRaises(ValueError):
                evaluate(report)
