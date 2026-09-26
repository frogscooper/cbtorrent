"""Paired, real-TCP local trials; no synthetic performance claims."""
import asyncio
import platform
import random
import statistics
import tempfile
from pathlib import Path

from .client import DownloadError, download
from .metainfo import create
from .policy import BanditPolicy, ThroughputPolicy
from .seeder import FileSource, SeedServer


class UnreliableSource:
    def __init__(self, source):
        self.source = source
        self.verified = source.verified

    def read(self, index, offset, length):
        data = self.source.read(index, offset, length)
        return bytes([data[0] ^ 1]) + data[1:]


async def run_benchmark(*, trials=3, size=1024 * 1024, seed=2026, progress=None):
    if not 1 <= trials <= 100 or not 16384 <= size <= 64 * 1024 * 1024:
        raise ValueError("benchmark requires 1..100 trials and 16 KiB..64 MiB")
    rng = random.Random(seed)
    scenarios = {
        "mixed": [(256 * 1024, 0.005, False), (2 * 1024 * 1024, 0.001, False),
                  (768 * 1024, 0.002, False)],
        "uniform": [(1024 * 1024, 0.002, False)] * 3,
        "corrupt-peer": [(2 * 1024 * 1024, 0, True), (512 * 1024, 0.002, False),
                         (1024 * 1024, 0.002, False)],
    }
    rows = []
    with tempfile.TemporaryDirectory(prefix="cbtorrent-benchmark-") as directory:
        root = Path(directory)
        source_path = root / "fixture.bin"
        source_path.write_bytes(rng.randbytes(size))
        torrent = create(source_path, root / "fixture.torrent", piece_length=32768)
        source = FileSource(torrent, source_path)
        try:
            for scenario, config in scenarios.items():
                for trial in range(trials):
                    order = list(range(len(config)))
                    rng.shuffle(order)
                    policies = [("heuristic", ThroughputPolicy), ("bandit", BanditPolicy)]
                    rng.shuffle(policies)
                    for name, policy_type in policies:
                        servers, peers = [], []
                        try:
                            for index in order:
                                rate, latency, corrupt = config[index]
                                server = SeedServer(torrent, UnreliableSource(source) if corrupt else source,
                                                    rate=rate, latency=latency)
                                port = await server.start()
                                servers.append(server)
                                peers.append(("127.0.0.1", port))
                            output = root / f"{scenario}-{trial}-{name}.bin"
                            try:
                                report = await download(torrent, peers, output, concurrency=2,
                                                        policy=policy_type(), use_trackers=False,
                                                        timeout=3, piece_timeout=10)
                            except DownloadError as error:
                                report = error.report
                            row = {"scenario": scenario, "trial": trial, "policy": name,
                                   "peer_order": order.copy(), "metrics": report}
                            rows.append(row)
                            if progress:
                                progress(scenario, trial + 1, name, report)
                        finally:
                            await asyncio.gather(*(server.close() for server in servers))
        finally:
            source.close()
    summary = []
    for scenario in scenarios:
        for policy in ("heuristic", "bandit"):
            subset = [r["metrics"] for r in rows if r["scenario"] == scenario and r["policy"] == policy]
            successful = [r for r in subset if r["complete"]]
            item = {"scenario": scenario, "policy": policy,
                    "successes": len(successful), "trials": trials}
            for key in ("completion_seconds", "cpu_seconds", "protocol_overhead_bytes",
                        "wasted_payload_bytes", "connections", "policy_seconds"):
                values = [r[key] for r in successful]
                item[f"median_{key}"] = statistics.median(values) if values else None
                item[f"stdev_{key}"] = statistics.stdev(values) if len(values) > 1 else None
            summary.append(item)
    return {"schema_version": 1, "seed": seed, "trials": trials, "size_bytes": size,
            "python": platform.python_version(), "platform": platform.platform(),
            "concurrency": 2, "piece_length": 32768, "pipeline": 8,
            "scenarios": {key: [{"bytes_per_second": r, "delay_per_block_seconds": l,
                                  "corrupt": c} for r, l, c in values]
                          for key, values in scenarios.items()},
            "note": "Local loopback trials with application-level rate/delay injection. Not public-swarm evidence.",
            "summary": summary, "runs": rows}
