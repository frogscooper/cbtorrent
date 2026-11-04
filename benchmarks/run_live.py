"""Paired live-swarm comparison of download policies.

Run from the checkout:
    python benchmarks/run_live.py --torrent A.torrent --torrent B.torrent \
        --policies heuristic,adaptive --rounds 3 --report benchmarks/live-NEW.json

Each trial is a separate `python -m cbtorrent download` process with a hard
wall-clock deadline, writing into a fresh temporary directory that is deleted
afterwards. Within each round the policies run back to back in a seeded random
order, so slow drift in the swarm affects every policy alike. Every attempt is
kept, including timeouts and crashes.

This uses the public Internet: trackers, DHT, and real peers. Only use torrents
you are allowed to download and share. It is not a CI test, and results describe
these swarms at this time from this network, not public swarms in general.
"""
import argparse
import json
import platform
import random
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
POLICIES = ("heuristic", "bandit", "adaptive", "optimistic", "recovery", "timed")
KEPT_METRICS = ("complete", "completion_seconds", "elapsed_seconds", "cpu_seconds",
                "verified_bytes", "payload_received_bytes", "wasted_payload_bytes",
                "wire_sent_bytes", "wire_received_bytes", "protocol_overhead_bytes",
                "uploaded_bytes", "connections", "peer_failures", "peer_retries",
                "hash_failures", "peers_banned", "endgame_duplicate_bytes",
                "tracker_requests", "tracker_failures", "dht_requests", "dht_failures",
                "dht_peers", "pex_peers", "policy_seconds", "peer_failure_phases")


def plan(torrents, policies, rounds, seed):
    """Return trials in run order: per round, per torrent, policies shuffled."""
    rng = random.Random(seed)
    trials = []
    for round_index in range(rounds):
        for torrent in torrents:
            order = list(policies)
            rng.shuffle(order)
            for position, policy in enumerate(order):
                trials.append(dict(round=round_index, torrent=torrent, policy=policy,
                                   position=position))
    return trials


def download_command(torrent, policy, output, report, extra_args):
    return [sys.executable, "-m", "cbtorrent", "download", str(torrent),
            "--output", str(output), "--policy", policy, "--report", str(report),
            "--no-metadata-cache", *extra_args]


def run_trial(trial, deadline, extra_args, command=download_command):
    """Run one download in a fresh directory; never raises for download failure."""
    work = Path(tempfile.mkdtemp(prefix="cbtorrent-live-"))
    report_path = work / "report.json"
    argv = command(trial["torrent"], trial["policy"], work / "data", report_path, extra_args)
    started = time.monotonic()
    outcome, stderr_tail, exit_code = "ok", "", None
    try:
        process = subprocess.run(argv, cwd=ROOT, capture_output=True, text=True,
                                 errors="replace", timeout=deadline)
        exit_code = process.returncode
        stderr_tail = process.stderr[-2000:]
        if exit_code != 0:
            outcome = "failed"
    except subprocess.TimeoutExpired as error:
        outcome = "deadline"
        stderr = error.stderr or b""
        stderr_tail = (stderr.decode("utf-8", "replace") if isinstance(stderr, bytes) else stderr)[-2000:]
    wall = time.monotonic() - started
    metrics = None
    try:
        report = json.loads(report_path.read_text(encoding="utf-8"))
        metrics = {key: report.get(key) for key in KEPT_METRICS}
        metrics["dht_errors"] = (report.get("dht_errors") or [])[:10]
    except (OSError, ValueError):
        if outcome == "ok":
            outcome = "no-report"
    finally:
        shutil.rmtree(work, ignore_errors=True)
    complete = bool(metrics and metrics.get("complete")) and outcome == "ok"
    return dict(trial, torrent=str(trial["torrent"]), outcome=outcome, complete=complete,
                exit_code=exit_code, wall_seconds=round(wall, 3), metrics=metrics,
                stderr_tail=stderr_tail if not complete else "")


def median(values):
    values = [v for v in values if v is not None]
    return statistics.median(values) if values else None


def summarize(rows, policies):
    """Per torrent and policy: counts, medians over completed runs, failures."""
    summary = []
    for torrent in dict.fromkeys(r["torrent"] for r in rows):
        for policy in policies:
            runs = [r for r in rows if r["torrent"] == torrent and r["policy"] == policy]
            done = [r["metrics"] for r in runs if r["complete"]]
            phases = {}
            for r in runs:
                for phase, count in ((r["metrics"] or {}).get("peer_failure_phases") or {}).items():
                    phases[phase] = phases.get(phase, 0) + count
            summary.append(dict(
                torrent=torrent, policy=policy, attempts=len(runs), completed=len(done),
                outcomes={o: sum(r["outcome"] == o for r in runs)
                          for o in dict.fromkeys(r["outcome"] for r in runs)},
                median_completion_seconds=median(m["completion_seconds"] for m in done),
                completion_seconds=[round(m["completion_seconds"], 3) for m in done],
                median_wire_received_bytes=median(m["wire_received_bytes"] for m in done),
                median_wasted_payload_bytes=median(m["wasted_payload_bytes"] for m in done),
                median_connections=median(m["connections"] for m in done),
                peer_failure_phases_total=phases))
    return summary


def paired_ratios(rows, baseline, policies):
    """Per round and torrent, completion time of each policy divided by baseline."""
    ratios = {}
    for policy in policies:
        if policy == baseline:
            continue
        values = []
        for r in rows:
            if r["policy"] != policy or not r["complete"]:
                continue
            base = [b for b in rows if b["policy"] == baseline and b["complete"]
                    and b["round"] == r["round"] and b["torrent"] == r["torrent"]]
            if base:
                values.append(r["metrics"]["completion_seconds"] / base[0]["metrics"]["completion_seconds"])
        ratios[policy] = dict(pairs=len(values), median_ratio=median(values),
                              ratios=[round(v, 3) for v in values])
    return ratios


def git_commit():
    try:
        out = subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True,
                             text=True, timeout=10)
        dirty = subprocess.run(["git", "status", "--porcelain", "--", "cbtorrent"], cwd=ROOT,
                               capture_output=True, text=True, timeout=10).stdout.strip()
        return out.stdout.strip() + ("-dirty" if dirty else "")
    except (OSError, subprocess.SubprocessError):
        return None


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--torrent", action="append", required=True,
                        help=".torrent path or quoted magnet (repeatable)")
    parser.add_argument("--policies", default="heuristic,adaptive")
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--deadline", type=float, default=1800.0,
                        help="wall-clock seconds before a trial is killed")
    parser.add_argument("--seed", type=int, default=20261103)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--note", default="", help="free text: network, time of day, etc.")
    parser.add_argument("extra", nargs=argparse.REMAINDER,
                        help="after --, extra arguments for cbtorrent download")
    args = parser.parse_args(argv)
    policies = tuple(p.strip() for p in args.policies.split(",") if p.strip())
    unknown = set(policies) - set(POLICIES)
    if unknown or not policies:
        parser.error(f"unknown policies: {sorted(unknown)}")
    if args.rounds < 1 or args.deadline <= 0:
        parser.error("--rounds and --deadline must be positive")
    if args.report.exists():
        parser.error(f"report already exists: {args.report}")
    extra = args.extra[1:] if args.extra[:1] == ["--"] else args.extra

    started = datetime.now(timezone.utc).isoformat(timespec="seconds")
    rows = []
    trials = plan(args.torrent, policies, args.rounds, args.seed)
    for index, trial in enumerate(trials, 1):
        row = run_trial(trial, args.deadline, extra)
        rows.append(row)
        seconds = (row["metrics"] or {}).get("completion_seconds")
        print(f"[{index}/{len(trials)}] round {trial['round'] + 1} {Path(str(trial['torrent'])).name} "
              f"{trial['policy']}: {row['outcome']}"
              + (f" {seconds:.1f}s" if seconds else f" ({row['wall_seconds']:.0f}s)"), flush=True)

    report = dict(
        schema_version=1, suite="live-swarm", started_utc=started,
        finished_utc=datetime.now(timezone.utc).isoformat(timespec="seconds"),
        code_commit=git_commit(), python=platform.python_version(),
        platform=platform.platform(), policies=list(policies), rounds=args.rounds,
        deadline_seconds=args.deadline, seed=args.seed, extra_args=extra, note=args.note,
        caveat=("Public swarms at one time from one network. Swarm state changes between "
                "trials; interleaving reduces but does not remove that drift. Small samples."),
        summary=summarize(rows, policies),
        paired_vs_baseline=paired_ratios(rows, policies[0], policies),
        trials=rows)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    with args.report.open("x", encoding="utf-8") as stream:
        json.dump(report, stream, indent=2)
    print(json.dumps(dict(summary=report["summary"], paired_vs_baseline=report["paired_vs_baseline"]),
                     indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
