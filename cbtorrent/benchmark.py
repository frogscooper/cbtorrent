"""Paired real-TCP trials, development/held-out suites, and bootstrap comparisons."""
import asyncio
import hashlib
import platform
import random
import statistics
import tempfile
from pathlib import Path

from .client import DownloadError, download
from .metainfo import create
from .policy import AdaptivePolicy, BanditPolicy, PlannedHeuristic, ThroughputPolicy
from .seeder import FileSource, SeedServer

POLICIES = {
    "heuristic": ThroughputPolicy,
    "bandit": BanditPolicy,
    "adaptive": AdaptivePolicy,
    "adaptive-no-defer": lambda: AdaptivePolicy(defer=False),
    "planned-heuristic": PlannedHeuristic,
}


class UnreliableSource:
    def __init__(self, source):
        self.source = source
        self.verified = source.verified

    def read(self, index, offset, length):
        data = self.source.read(index, offset, length)
        return bytes([data[0] ^ 1]) + data[1:]


def scenarios_for(suite, size):
    kib = 1024
    def case(rates, delay=0.002, changes=(), corrupt=None, piece_length=32768):
        return dict(peers=[dict(rate=r * kib, latency=delay, corrupt=i == corrupt)
                           for i, r in enumerate(rates)],
                    changes=list(changes), size=size, piece_length=piece_length)
    def change(after, peer, rate):
        return dict(after=after, peer=peer, rate=rate * kib)
    if suite == "baseline":
        return {
            "mixed": case([256, 2048, 768]),
            "uniform": case([1024, 1024, 1024]),
            "corrupt-peer": case([2048, 512, 1024], corrupt=0),
        }
    if suite == "development":
        return {
            "mixed": case([192, 2048, 768]),
            "near-uniform": case([900, 1024, 1100]),
            "rate-swap": case([2048, 384, 768], changes=[
                change(0.3, 0, 192), change(0.3, 1, 2048)]),
            "slowdown": case([2048, 768, 512], changes=[change(0.25, 0, 128)]),
            "corrupt-peer": case([2048, 512, 1024], corrupt=0),
        }
    if suite == "validation":
        # Fixed before examining validation measurements. No policy parameters
        # are fitted to these scenarios or injected into the client's model.
        return {
            "wide-mix": case([160, 640, 1536, 3072], piece_length=65536),
            "narrow-mix": case([800, 960, 1152, 1280], delay=0.004),
            "equal-peers": case([896, 896, 896, 896], delay=0.001, piece_length=16384),
            "delayed-swap": case([2560, 320, 1024, 640], changes=[
                change(0.55, 0, 160), change(0.55, 1, 3072)], piece_length=65536),
            "recovering-peer": case([768, 192, 512], changes=[change(0.35, 1, 2560)]),
            "double-slowdown": case([2048, 1536, 640, 384], changes=[
                change(0.3, 0, 128), change(0.6, 1, 256)]),
            "corrupt-fast-peer": case([3072, 1792, 640, 320], corrupt=0, piece_length=65536),
            "high-delay": case([256, 1536, 2560], delay=0.012),
        }
    raise ValueError("unknown benchmark suite")


def paired_comparisons(rows, names, *, bootstrap_seed=90210):
    """Pair by scenario/trial, retaining failures instead of silently dropping them."""
    comparisons = []
    rng = random.Random(bootstrap_seed)
    scenarios = list(dict.fromkeys(row["scenario"] for row in rows))
    for scenario in scenarios:
        lookup = {(row["trial"], row["policy"]): row["metrics"]
                  for row in rows if row["scenario"] == scenario}
        trials = sorted({trial for trial, _ in lookup})
        for name in names:
            if name == "heuristic":
                continue
            ratios, overhead, waste, cpu = [], [], [], []
            failures = 0
            for trial in trials:
                base, candidate = lookup.get((trial, "heuristic")), lookup.get((trial, name))
                if base is None or candidate is None:
                    continue
                if not base["complete"] or not candidate["complete"]:
                    failures += 1
                    continue
                ratios.append(1 - candidate["completion_seconds"] / base["completion_seconds"])
                overhead.append(candidate["protocol_overhead_bytes"] - base["protocol_overhead_bytes"])
                waste.append(candidate["wasted_payload_bytes"] - base["wasted_payload_bytes"])
                cpu.append(candidate["cpu_seconds"] - base["cpu_seconds"])
            interval = None
            if len(ratios) >= 2:
                means = sorted(statistics.mean(rng.choices(ratios, k=len(ratios))) for _ in range(2000))
                interval = [means[49], means[1949]]
            comparisons.append(dict(
                scenario=scenario, policy=name, baseline="heuristic",
                paired_successes=len(ratios), pairs_with_failure=failures,
                mean_completion_speedup=statistics.mean(ratios) if ratios else None,
                bootstrap_95_percent_interval=interval,
                median_overhead_delta_bytes=statistics.median(overhead) if overhead else None,
                median_waste_delta_bytes=statistics.median(waste) if waste else None,
                median_cpu_delta_seconds=statistics.median(cpu) if cpu else None,
            ))
    return comparisons


async def run_benchmark(*, trials=3, size=1024 * 1024, seed=2026, progress=None,
                        suite="baseline", policies=("heuristic", "bandit", "adaptive")):
    if not 1 <= trials <= 100 or not 16384 <= size <= 64 * 1024 * 1024:
        raise ValueError("benchmark requires 1..100 trials and 16 KiB..64 MiB")
    if not policies or len(set(policies)) != len(policies) or any(p not in POLICIES for p in policies):
        raise ValueError("unknown or duplicate benchmark policy")
    rng = random.Random(seed)
    scenarios = scenarios_for(suite, size)
    rows = []
    code_hash = hashlib.sha256()
    for name in ("policy.py", "client.py", "wire.py", "benchmark.py", "seeder.py"):
        code_hash.update(Path(__file__).with_name(name).read_bytes())
    with tempfile.TemporaryDirectory(prefix="cbtorrent-benchmark-") as directory:
        root = Path(directory)
        for scenario, config in scenarios.items():
            source_path = root / f"{scenario}.bin"
            source_path.write_bytes(rng.randbytes(config["size"]))
            torrent = create(source_path, root / f"{scenario}.torrent", piece_length=config["piece_length"])
            source = FileSource(torrent, source_path)
            try:
                for trial in range(trials):
                    order = list(range(len(config["peers"])))
                    rng.shuffle(order)
                    policy_order = list(policies)
                    rng.shuffle(policy_order)
                    for name in policy_order:
                        servers, peers = [], []
                        change_task = None
                        try:
                            for index in order:
                                spec = config["peers"][index]
                                server = SeedServer(torrent, UnreliableSource(source) if spec["corrupt"] else source,
                                                    rate=spec["rate"], latency=spec["latency"])
                                port = await server.start()
                                servers.append(server)
                                peers.append(("127.0.0.1", port))
                            async def change_rates():
                                started = asyncio.get_running_loop().time()
                                for event in sorted(config["changes"], key=lambda x: x["after"]):
                                    await asyncio.sleep(max(0, started + event["after"] - asyncio.get_running_loop().time()))
                                    servers[order.index(event["peer"])].rate = event["rate"]
                            change_task = asyncio.create_task(change_rates())
                            output = root / f"{scenario}-{trial}-{name}.bin"
                            try:
                                report = await download(torrent, peers, output, concurrency=2,
                                                        policy=POLICIES[name](), use_trackers=False,
                                                        timeout=3, piece_timeout=10)
                            except DownloadError as error:
                                report = error.report
                            rows.append(dict(scenario=scenario, trial=trial, policy=name,
                                             peer_order=order.copy(), metrics=report))
                            if progress:
                                progress(scenario, trial + 1, name, report)
                        finally:
                            if change_task is not None:
                                change_task.cancel()
                                await asyncio.gather(change_task, return_exceptions=True)
                            await asyncio.gather(*(server.close() for server in servers))
            finally:
                source.close()
    summary = []
    for scenario in scenarios:
        for policy in policies:
            subset = [r["metrics"] for r in rows if r["scenario"] == scenario and r["policy"] == policy]
            successful = [r for r in subset if r["complete"]]
            item = dict(scenario=scenario, policy=policy, successes=len(successful), trials=trials)
            for key in ("completion_seconds", "cpu_seconds", "protocol_overhead_bytes",
                        "wasted_payload_bytes", "connections", "policy_seconds", "policy_update_seconds",
                        "policy_deferrals"):
                values = [r[key] for r in successful]
                item[f"median_{key}"] = statistics.median(values) if values else None
                item[f"stdev_{key}"] = statistics.stdev(values) if len(values) > 1 else None
            summary.append(item)
    return dict(
        schema_version=2, suite=suite, seed=seed, trials=trials, size_bytes=size,
        code_sha256=code_hash.hexdigest(), policies=list(policies),
        python=platform.python_version(), platform=platform.platform(),
        concurrency=2, pipeline=8, scenarios=scenarios,
        note="Local loopback with application-level rate/delay injection. Not public-swarm evidence. Bootstrap intervals are descriptive, per-scenario, not multiple-comparison corrected.",
        summary=summary, comparisons=paired_comparisons(rows, policies), runs=rows)
