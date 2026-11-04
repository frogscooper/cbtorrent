"""Paired loopback swarm with pipelined round-trip delay and heterogeneous peers.

Run from the checkout:
    python benchmarks/run_swarm_sim.py --trials 5 --report benchmarks/swarm-sim-NEW.json

Each trial draws one swarm from a seeded distribution (per-peer upload rate,
round-trip delay, and some dead addresses) and runs every configuration against
that same swarm in random order. The distribution is fixed in this file before
any result is seen; the learned policy never reads it. Loopback has no real
congestion, loss, or NAT, so this checks mechanics, not public-swarm speed.
"""
import argparse
import asyncio
import json
import math
import os
import platform
import random
import socket
import statistics
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from cbtorrent.client import DownloadError, download  # noqa: E402
from cbtorrent.metainfo import create  # noqa: E402
from cbtorrent.policy import LearnedPolicy, ThroughputPolicy  # noqa: E402
from cbtorrent.seeder import FileSource, SeedServer  # noqa: E402

KIB = 1024
CONFIGS = {
    # The current default engine settings.
    "heuristic": dict(policy=ThroughputPolicy, concurrency=4, pipeline=8),
    # Static control: a wide fixed setting with no learning.
    "wide-static": dict(policy=ThroughputPolicy, concurrency=12, pipeline=64),
    # Learned slots, windows, and peer ranking, from the default starting point.
    "learned": dict(policy=LearnedPolicy, concurrency=4, pipeline=8),
}


def draw_swarm(rng, peers, dead):
    """Log-uniform rates 20 KiB/s .. 2 MiB/s, round trips 30 .. 300 ms."""
    swarm = [dict(rate=math.exp(rng.uniform(math.log(20 * KIB), math.log(2048 * KIB))),
                  delay=rng.uniform(0.03, 0.30)) for _ in range(peers)]
    return dict(peers=swarm, dead=dead, order_seed=rng.randrange(2 ** 32))


def dead_addresses(count):
    """Ports that refuse connections: bound briefly, then closed."""
    addresses = []
    for _ in range(count):
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            addresses.append(("127.0.0.1", probe.getsockname()[1]))
    return addresses


async def run_trial(torrent, source_path, swarm, name, output):
    servers, sources = [], []
    try:
        addresses = []
        for spec in swarm["peers"]:
            source = FileSource(torrent, source_path)
            sources.append(source)
            server = SeedServer(torrent, source, rate=spec["rate"], delay=spec["delay"], max_clients=4)
            servers.append(server)
            addresses.append(("127.0.0.1", await server.start("127.0.0.1", 0)))
        addresses += dead_addresses(swarm["dead"])
        random.Random(swarm["order_seed"]).shuffle(addresses)  # same order for every config
        config = CONFIGS[name]
        try:
            report = await asyncio.wait_for(download(
                torrent, addresses, output, use_trackers=False, policy=config["policy"](),
                concurrency=config["concurrency"], pipeline=config["pipeline"],
                max_connections=16, timeout=5.0, piece_timeout=60.0), 600)
        except DownloadError as error:
            report = error.report
        except TimeoutError:
            report = dict(complete=False, completion_seconds=None, note="harness deadline")
        keep = ("complete", "completion_seconds", "verified_bytes", "wire_received_bytes",
                "wasted_payload_bytes", "connections", "peer_failures", "cpu_seconds",
                "endgame_duplicate_bytes", "policy_diagnostics")
        return {k: report.get(k) for k in keep}
    finally:
        for server in servers:
            await server.close()
        for source in sources:
            source.close()


async def run(trials, size_mib, peers, dead, seed, names):
    rng = random.Random(seed)
    rows = []
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        source_path = root / "payload.bin"
        source_path.write_bytes(os.urandom(int(size_mib * 1024 * 1024)))
        torrent = create(source_path, root / "payload.torrent", piece_length=256 * KIB)
        for trial in range(trials):
            swarm = draw_swarm(rng, peers, dead)
            order = list(names)
            rng.shuffle(order)
            for name in order:
                output = root / f"out-{trial}-{name}.bin"
                metrics = await run_trial(torrent, source_path, swarm, name, output)
                if metrics["complete"] and output.read_bytes() != source_path.read_bytes():
                    metrics["complete"], metrics["note"] = False, "payload mismatch"
                output.unlink(missing_ok=True)
                rows.append(dict(trial=trial, config=name, metrics=metrics))
                print(f"trial {trial + 1} {name}: "
                      + (f"{metrics['completion_seconds']:.2f}s" if metrics["complete"] else "FAILED"),
                      flush=True)
            # Available upload capacity, as context for the times above.
            rows[-1]["swarm_total_rate"] = round(sum(p["rate"] for p in swarm["peers"]))
    return rows


def summarize(rows, names, baseline="heuristic"):
    summary = {}
    for name in names:
        done = [r["metrics"]["completion_seconds"] for r in rows
                if r["config"] == name and r["metrics"]["complete"]]
        ratios = []
        for r in rows:
            if r["config"] != name or not r["metrics"]["complete"]:
                continue
            base = [b for b in rows if b["config"] == baseline and b["trial"] == r["trial"]
                    and b["metrics"]["complete"]]
            if base:
                ratios.append(r["metrics"]["completion_seconds"] / base[0]["metrics"]["completion_seconds"])
        summary[name] = dict(
            attempts=sum(r["config"] == name for r in rows), completed=len(done),
            median_seconds=statistics.median(done) if done else None,
            seconds=[round(x, 2) for x in done],
            median_ratio_vs_baseline=statistics.median(ratios) if ratios else None,
            faster_than_baseline=sum(x < 1 for x in ratios), pairs=len(ratios))
    return summary


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--trials", type=int, default=5)
    parser.add_argument("--size-mib", type=float, default=48)
    parser.add_argument("--peers", type=int, default=30)
    parser.add_argument("--dead", type=int, default=8)
    parser.add_argument("--seed", type=int, default=20261104)
    parser.add_argument("--configs", default=",".join(CONFIGS))
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args(argv)
    names = tuple(n for n in args.configs.split(",") if n)
    if set(names) - set(CONFIGS) or "heuristic" not in names:
        parser.error(f"configs must include heuristic and come from {sorted(CONFIGS)}")
    if args.report.exists():
        parser.error(f"report already exists: {args.report}")
    rows = asyncio.run(run(args.trials, args.size_mib, args.peers, args.dead, args.seed, names))
    report = dict(schema_version=1, suite="swarm-sim", python=platform.python_version(),
                  platform=platform.platform(), trials=args.trials, size_mib=args.size_mib,
                  peers=args.peers, dead=args.dead, seed=args.seed, piece_kib=256,
                  configs={n: dict(CONFIGS[n], policy=CONFIGS[n]["policy"].__name__) for n in names},
                  note=__doc__.split("\n\n")[1].replace("\n", " "),
                  summary=summarize(rows, names), rows=rows)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    with args.report.open("x", encoding="utf-8") as stream:
        json.dump(report, stream, indent=2)
    print(json.dumps(report["summary"], indent=2))


if __name__ == "__main__":
    main()
