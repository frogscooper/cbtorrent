import argparse
import asyncio
import json
import sys
from pathlib import Path

from .benchmark import run_benchmark
from .client import DownloadError, download
from .metainfo import Torrent, create
from .metrics import Metrics
from .policy import AdaptivePolicy, BanditPolicy, RecoveryPolicy, ThroughputPolicy, TimeBudgetPolicy
from .seeder import FileSource, SeedServer
from .tracker import announce


def endpoint(value):
    try:
        host, port = value.rsplit(":", 1)
        port = int(port)
        if not host or not 1 <= port <= 65535:
            raise ValueError
        return host.strip("[]"), port
    except ValueError:
        raise argparse.ArgumentTypeError("peer must be HOST:PORT (IPv6: [ADDRESS]:PORT)")


async def seed_file(args):
    torrent = Torrent.load(args.torrent)
    source = FileSource(torrent, args.file)
    metrics = Metrics()
    metrics.start()
    server = SeedServer(torrent, source, metrics=metrics, max_clients=args.max_clients)
    started = set()
    next_updates = {}
    port = 0

    async def update(url, event):
        try:
            result = await announce(url, torrent.info_hash, server.peer_id, port=port,
                                    uploaded=metrics.uploaded_bytes, event=event)
            started.add(url)
            next_updates[url] = asyncio.get_running_loop().time() + result.interval
        except (OSError, ValueError, asyncio.TimeoutError) as error:
            print(f"Tracker: {error}", file=sys.stderr)
            next_updates[url] = asyncio.get_running_loop().time() + 60

    try:
        port = await server.start(args.listen_host, args.port)
        print(f"Seeding {torrent.name} on {args.listen_host}:{port}", flush=True)
        urls = torrent.trackers[:8] if not args.no_trackers else ()
        await asyncio.gather(*(update(url, "started") for url in urls))
        while True:
            await asyncio.sleep(1)
            for url in urls:
                if next_updates[url] <= asyncio.get_running_loop().time():
                    await update(url, "" if url in started else "started")
    finally:
        await server.close()
        source.close()
        if started:
            await asyncio.gather(*(update(url, "stopped") for url in tuple(started)))


def build_parser():
    parser = argparse.ArgumentParser(description="Python BitTorrent client with measured peer policies")
    commands = parser.add_subparsers(dest="command", required=True)
    get = commands.add_parser("download", help="download and share verified pieces")
    get.add_argument("torrent", type=Path)
    get.add_argument("--peer", type=endpoint, action="append", default=[])
    get.add_argument("--output", type=Path, required=True)
    get.add_argument("--timeout", type=float, default=15.0)
    get.add_argument("--piece-timeout", type=float, default=120.0)
    get.add_argument("--pipeline", type=int, default=8)
    get.add_argument("--concurrency", type=int, default=4)
    get.add_argument("--max-connections", type=int, default=16)
    get.add_argument("--policy", choices=("heuristic", "bandit", "adaptive", "recovery", "timed"), default="heuristic")
    get.add_argument("--resume", action="store_true")
    get.add_argument("--no-trackers", action="store_true")
    get.add_argument("--listen-host", default="0.0.0.0")
    get.add_argument("--port", type=int, default=0)
    get.add_argument("--progress", action="store_true")
    get.add_argument("--report", type=Path)
    seed = commands.add_parser("seed", help="verify and seed a complete file until Ctrl+C")
    seed.add_argument("torrent", type=Path)
    seed.add_argument("--file", type=Path, required=True)
    seed.add_argument("--listen-host", default="0.0.0.0")
    seed.add_argument("--port", type=int, default=6881)
    seed.add_argument("--max-clients", type=int, default=32)
    seed.add_argument("--no-trackers", action="store_true")
    make = commands.add_parser("create", help="create single-file v1 torrent metadata")
    make.add_argument("file", type=Path)
    make.add_argument("--output", type=Path, required=True)
    make.add_argument("--tracker", action="append", default=[])
    make.add_argument("--piece-length", type=int, default=256 * 1024)
    info = commands.add_parser("inspect", help="show torrent metadata")
    info.add_argument("torrent", type=Path)
    bench = commands.add_parser("benchmark", help="paired local-TCP heuristic/bandit trials")
    bench.add_argument("--trials", type=int, default=3)
    bench.add_argument("--size-mib", type=float, default=1)
    bench.add_argument("--seed", type=int, default=2026)
    bench.add_argument("--suite", choices=("baseline", "development", "validation",
                                         "recovery-development", "recovery-validation", "time-validation"), default="baseline")
    bench.add_argument("--policies", default="heuristic,bandit,adaptive")
    bench.add_argument("--report", type=Path)
    return parser


def write_report(path, report):
    if path:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("x", encoding="utf-8") as stream:
            json.dump(report, stream, indent=2)
            stream.write("\n")


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    # Keep the initial milestone's "cbtorrent FILE --peer ..." invocation working.
    if argv and argv[0] not in ("download", "seed", "create", "inspect", "benchmark") and not argv[0].startswith("-"):
        argv.insert(0, "download")
    args = build_parser().parse_args(argv)
    try:
        report_path = getattr(args, "report", None)
        if report_path and report_path.exists():
            raise FileExistsError(f"report already exists: {report_path}")
        if args.command == "download":
            torrent = Torrent.load(args.torrent)
            args.output.parent.mkdir(parents=True, exist_ok=True)
            policy = {"heuristic": ThroughputPolicy, "bandit": BanditPolicy,
                      "adaptive": AdaptivePolicy, "recovery": RecoveryPolicy,
                      "timed": TimeBudgetPolicy}[args.policy]()
            def progress(done, total):
                print(f"{done}/{total} verified bytes", file=sys.stderr)
            report = asyncio.run(download(
                torrent, args.peer, args.output, timeout=args.timeout, piece_timeout=args.piece_timeout,
                pipeline=args.pipeline, concurrency=args.concurrency, max_connections=args.max_connections,
                resume=args.resume, policy=policy, use_trackers=not args.no_trackers,
                listen_host=args.listen_host, listen_port=args.port,
                progress=progress if args.progress else None))
        elif args.command == "seed":
            asyncio.run(seed_file(args))
            return 0
        elif args.command == "create":
            torrent = create(args.file, args.output, piece_length=args.piece_length, trackers=args.tracker)
            print(f"Created {args.output} ({torrent.info_hash.hex()})")
            return 0
        elif args.command == "inspect":
            torrent = Torrent.load(args.torrent)
            report = dict(name=torrent.name, length=torrent.length, piece_length=torrent.piece_length,
                          pieces=len(torrent.hashes), info_hash=torrent.info_hash.hex(), trackers=torrent.trackers)
        else:
            def progress(scenario, trial, policy, metrics):
                print(f"{scenario} trial {trial} {policy}: {metrics['elapsed_seconds']:.3f}s", file=sys.stderr)
            report = asyncio.run(run_benchmark(trials=args.trials, size=int(args.size_mib * 1024 * 1024),
                                              seed=args.seed, progress=progress, suite=args.suite,
                                              policies=tuple(args.policies.split(","))))
        write_report(report_path, report)
        print(json.dumps(report["summary"] if args.command == "benchmark" and report_path else report, indent=2))
        return 0
    except DownloadError as error:
        try:
            write_report(getattr(args, "report", None), error.report)
        except OSError as report_error:
            print(f"Could not save report: {report_error}", file=sys.stderr)
        print(json.dumps(error.report, indent=2))
        print(str(error), file=sys.stderr)
        return 1
    except (OSError, ValueError, OverflowError) as error:
        print(str(error), file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
