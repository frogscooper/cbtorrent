"""Paired engine ablation using the independent lifecycle test peers.

Run from the checkout: python benchmarks/run_resilience.py --trials 3 --report NEW.json
Delays are accelerated fixtures, not defaults or a public-swarm performance claim.
"""
import argparse
import asyncio
import hashlib
import json
import platform
import random
import statistics
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))
from test_lifecycle import LifecycleTests
from cbtorrent.client import DownloadError


async def run(trials):
    rng = random.Random(20260930)
    rows = []
    modes = {"without-recovery-endgame": dict(peer_retries=0, endgame=False),
             "recovery-endgame": dict(peer_retries=2, endgame=True)}
    scenarios = ("stable", "disconnect-once", "stalled-last-block", "always-disconnected")
    for scenario in scenarios:
        for trial in range(trials):
            order = list(modes)
            rng.shuffle(order)
            for mode in order:
                fixture = LifecycleTests()
                await fixture.asyncSetUp()
                try:
                    if scenario == "stalled-last-block":
                        peers = [await fixture.seed(hold_last=True), await fixture.seed()]
                    elif scenario == "disconnect-once":
                        peers = [await fixture.seed(fail_first="disconnect")]
                    elif scenario == "always-disconnected":
                        peers = [await fixture.seed(always_fail="disconnect")]
                    else:
                        peers = [await fixture.seed()]
                    try:
                        report = await fixture.get(peers, **modes[mode])
                        if (fixture.root / "out").read_bytes() != fixture.data:
                            raise AssertionError("payload differs from fixture")
                    except DownloadError as error:
                        report = error.report
                    rows.append(dict(scenario=scenario, trial=trial, mode=mode, metrics=report))
                    print(scenario, trial + 1, mode, report["complete"], flush=True)
                finally:
                    await fixture.asyncTearDown()
    summary = []
    for scenario in scenarios:
        for mode in modes:
            values = [r["metrics"] for r in rows if r["scenario"] == scenario and r["mode"] == mode]
            successes = [r for r in values if r["complete"]]
            item = dict(scenario=scenario, mode=mode, trials=trials, successes=len(successes),
                        failures=trials - len(successes))
            for key in ("elapsed_seconds", "payload_received_bytes", "wasted_payload_bytes",
                        "protocol_overhead_bytes", "cpu_seconds", "connections", "endgame_requested_bytes"):
                item["median_" + key] = statistics.median(r[key] for r in values)
            item["median_completion_seconds"] = statistics.median(r["completion_seconds"] for r in successes) if successes else None
            summary.append(item)
    digest = hashlib.sha256()
    for path in (ROOT / "cbtorrent/client.py", ROOT / "cbtorrent/wire.py",
                 ROOT / "tests/test_lifecycle.py", Path(__file__)):
        digest.update(path.read_bytes())
    return dict(schema_version=1, code_sha256=digest.hexdigest(), python=platform.python_version(),
                platform=platform.platform(), trials=trials, seed=20260930, modes=modes,
                fixture_options=dict(timeout=0.4, piece_timeout=2, retry_delay=0.01,
                                     endgame_delay=0.03, concurrency=1, max_connections=2),
                note="Paired loopback ablation in randomized mode order; accelerated timers. All attempts, including failures, retained. No public-swarm or mature-client speed claim.",
                summary=summary, runs=rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    if not 1 <= args.trials <= 20:
        parser.error("trials must be 1..20")
    if args.report.exists():
        parser.error("report already exists; choose a new path")
    report = asyncio.run(run(args.trials))
    args.report.parent.mkdir(parents=True, exist_ok=True)
    with args.report.open("x", encoding="utf-8") as stream:
        json.dump(report, stream, indent=2)
        stream.write("\n")
    print(json.dumps(report["summary"], indent=2))


if __name__ == "__main__":
    main()
