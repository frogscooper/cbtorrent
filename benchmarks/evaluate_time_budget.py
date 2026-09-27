"""Apply the predeclared TIME_BUDGET_EXPERIMENT.md gates to a completed report.

Run: python benchmarks/evaluate_time_budget.py benchmarks/timed-validation-01.json
This reads results only; no output is fed to peer learning or benchmark fixtures.
"""

import json
import statistics
import sys


def evaluate(report):
    rows = report["runs"]
    lookup = {(r["scenario"], r["trial"], r["policy"]): r["metrics"] for r in rows}
    expected = {(s, t, p) for s in report["scenarios"] for t in range(report["trials"])
                for p in report["policies"]}
    if len(lookup) != len(rows) or lookup.keys() != expected:
        raise ValueError("report has duplicate or missing trial/policy rows")
    required = {"heuristic", "recovery", "timed", "ewma-probe"}
    if not required <= set(report["policies"]):
        raise ValueError("required baseline/candidate policies are absent")
    complete = all(r["metrics"]["complete"] for r in rows)
    if not complete:
        return {"accepted": False, "complete": False,
                "note": "Incomplete trials fail the first gate; do not drop them for timing analysis."}

    gains = {}
    same_traffic = True
    keys = ("wire_sent_bytes", "wire_received_bytes", "wasted_payload_bytes", "connections")
    for scenario in report["scenarios"]:
        for baseline in ("heuristic", "recovery"):
            gains[scenario, baseline] = statistics.mean(
                1 - lookup[scenario, t, "timed"]["completion_seconds"] /
                lookup[scenario, t, baseline]["completion_seconds"]
                for t in range(report["trials"]))
        for t in range(report["trials"]):
            same_traffic &= all(lookup[scenario, t, "timed"][k] == lookup[scenario, t, "recovery"][k]
                                for k in keys)
    stationary = ("budget-static", "late-change", "sustained-delay", "three-slots", "serial-slow-peer")
    worst_gain = min(gains[s, "recovery"] for s in report["scenarios"])
    best_stationary_gain = max(gains[s, "recovery"] for s in stationary)
    early = "before-midpoint"
    old_early = statistics.mean(1 - lookup[early, t, "recovery"]["completion_seconds"] /
                               lookup[early, t, "heuristic"]["completion_seconds"]
                               for t in range(report["trials"]))
    retained = gains[early, "heuristic"] / old_early if old_early > 0 else None
    costs = {p: statistics.median(r["metrics"]["policy_seconds"] + r["metrics"]["policy_update_seconds"]
                                 for r in rows if r["policy"] == p) for p in ("timed", "recovery")}
    cost_ratio = costs["timed"] / costs["recovery"]
    gates = dict(complete_and_equal_traffic=complete and same_traffic,
                 completion_tradeoff=worst_gain >= -0.02 and best_stationary_gain >= 0.01,
                 recovery_retention=retained is None or retained >= 0.8,
                 policy_cost=cost_ratio <= 2)
    return dict(accepted=all(gates.values()), gates=gates, worst_mean_gain_vs_recovery=worst_gain,
                best_stationary_gain=best_stationary_gain, retained_early_gain=retained,
                median_policy_cost_ratio=cost_ratio, downloads=len(rows))


if __name__ == "__main__":
    with open(sys.argv[1], encoding="utf-8") as stream:
        print(json.dumps(evaluate(json.load(stream)), indent=2))

