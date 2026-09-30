import argparse
import asyncio
import json
import sys
from pathlib import Path

from .benchmark import run_benchmark
from .client import DownloadError, download
from .dht import DhtDiscovery
from .metainfo import Torrent, create
from .magnet import Magnet, load_source, download_magnet
from .metrics import Metrics
from .policy import (AdaptivePolicy, BanditPolicy, OptimisticPolicy, RecoveryPolicy,
                     ThroughputPolicy, TimeBudgetPolicy)
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
    discovery = None

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
        if not args.no_dht and not torrent.private:
            discovery = DhtDiscovery(torrent, port, metrics,
                                     bootstrap=args.dht_bootstrap, bind_host=args.listen_host)
            discovery.start()
        print(f"Seeding {torrent.name} on {args.listen_host}:{port}", flush=True)
        urls = torrent.trackers[:8] if not args.no_trackers else ()
        await asyncio.gather(*(update(url, "started") for url in urls))
        while True:
            await asyncio.sleep(1)
            for url in urls:
                if next_updates[url] <= asyncio.get_running_loop().time():
                    await update(url, "" if url in started else "started")
    finally:
        if discovery is not None:
            await discovery.close()
        await server.close()
        source.close()
        if started:
            await asyncio.gather(*(update(url, "stopped") for url in tuple(started)))


HEADLESS_COMMANDS = frozenset({"download", "seed", "create", "inspect", "benchmark", "gui"})


def normalize_argv(argv):
    """Rewrite argv so GUI is the default launch path.

    Empty argv → gui (idle). Bare torrent path → gui with that torrent.
    Explicit headless commands and -h/--help are left unchanged.
    """
    argv = list(argv)
    if not argv:
        return ["gui"]
    if argv[0] in ("-h", "--help"):
        return argv
    if argv[0] not in HEADLESS_COMMANDS and not argv[0].startswith("-"):
        return ["gui"] + argv
    return argv


def build_parser():
    parser = argparse.ArgumentParser(
        description=(
        "Desktop GUI is the default (run with no arguments).\n"
        "Use a subcommand for headless/CLI: download, seed, create, inspect, benchmark, gui."))
    commands = parser.add_subparsers(dest="command", required=False)
    get = commands.add_parser("download", help="download and share verified pieces")
    get.add_argument("torrent", help=".torrent path or quoted v1 magnet URI")
    get.add_argument("--metadata-timeout", type=float, default=60.0)
    get.add_argument("--peer", type=endpoint, action="append", default=[])
    get.add_argument("--output", type=Path, required=True,
                     help="destination file, or new root directory for a multi-file torrent")
    get.add_argument("--timeout", type=float, default=15.0)
    get.add_argument("--piece-timeout", type=float, default=120.0)
    get.add_argument("--pipeline", type=int, default=8)
    get.add_argument("--concurrency", type=int, default=4)
    get.add_argument("--max-connections", type=int, default=16)
    get.add_argument("--peer-retries", type=int, default=2,
                     help="reconnections per peer after temporary failures (0..8)")
    get.add_argument("--retry-delay", type=float, default=0.5,
                     help="initial retry backoff in seconds")
    get.add_argument("--no-endgame", action="store_true")
    get.add_argument("--endgame-delay", type=float, default=1.0,
                     help="seconds without block progress before a tail helper")
    get.add_argument("--endgame-budget", type=int, default=131072,
                     help="maximum reserved helper-request bytes per download")
    get.add_argument("--policy", choices=("heuristic", "bandit", "adaptive", "optimistic", "recovery", "timed"), default="heuristic")
    get.add_argument("--resume", action="store_true")
    get.add_argument("--no-trackers", action="store_true")
    get.add_argument("--listen-host", default="0.0.0.0")
    get.add_argument("--port", type=int, default=0)
    get.add_argument("--progress", action="store_true")
    get.add_argument("--report", type=Path)
    seed = commands.add_parser("seed", help="verify and seed a file or directory until Ctrl+C")
    seed.add_argument("torrent", type=Path)
    seed.add_argument("--file", type=Path, required=True,
                      help="complete file, or root directory containing the torrent files")
    seed.add_argument("--listen-host", default="0.0.0.0")
    seed.add_argument("--port", type=int, default=6881)
    seed.add_argument("--max-clients", type=int, default=32)
    seed.add_argument("--no-trackers", action="store_true")
    make = commands.add_parser("create", help="create v1 torrent metadata from a file or directory")
    make.add_argument("file", type=Path)
    make.add_argument("--output", type=Path, required=True)
    make.add_argument("--tracker", action="append", default=[])
    make.add_argument("--node", type=endpoint, action="append", default=[],
                      help="include a DHT bootstrap HOST:PORT in the torrent (repeatable)")
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
    gui = commands.add_parser("gui", help="open the desktop progress window (default when no command)")
    gui.add_argument("torrent", type=lambda value: value if value.lower().startswith("magnet:") else Path(value), nargs="?")
    gui.add_argument("--metadata-timeout", type=float, default=60.0)
    gui.add_argument("--peer", type=endpoint, action="append", default=[])
    gui.add_argument("--output", type=Path)
    gui.add_argument("--policy", choices=("heuristic", "bandit", "adaptive", "optimistic", "recovery", "timed"), default="heuristic")
    gui.add_argument("--timeout", type=float, default=15.0)
    gui.add_argument("--piece-timeout", type=float, default=120.0)
    gui.add_argument("--pipeline", type=int, default=8)
    gui.add_argument("--concurrency", type=int, default=4)
    gui.add_argument("--max-connections", type=int, default=16)
    gui.add_argument("--resume", action="store_true")
    gui.add_argument("--no-trackers", action="store_true")
    gui.add_argument("--listen-host", default="0.0.0.0")
    gui.add_argument("--port", type=int, default=0)
    for command in (get, seed, gui):
        command.add_argument("--no-dht", action="store_true", help="disable IPv4 DHT discovery and announcements")
        command.add_argument("--dht-bootstrap", type=endpoint, action="append", default=None,
                             help="DHT bootstrap HOST:PORT (repeatable; replaces built-in bootstrap nodes)")
    return parser


def write_report(path, report):
    if path:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("x", encoding="utf-8") as stream:
            json.dump(report, stream, indent=2)
            stream.write("\n")


def main(argv=None):
    argv = normalize_argv(sys.argv[1:] if argv is None else argv)
    args = build_parser().parse_args(argv)
    if not args.command:
        args = build_parser().parse_args(["gui"])
    try:
        report_path = getattr(args, "report", None)
        if report_path and report_path.exists():
            raise FileExistsError(f"report already exists: {report_path}")
        if args.command == "gui":
            from .gui import run as run_gui
            return run_gui(
                args.torrent, args.output, peers=args.peer, resume=args.resume,
                use_trackers=not args.no_trackers, listen_host=args.listen_host,
                use_dht=not args.no_dht, dht_bootstrap=args.dht_bootstrap,
                listen_port=args.port, timeout=args.timeout,
                piece_timeout=args.piece_timeout, pipeline=args.pipeline,
                concurrency=args.concurrency, max_connections=args.max_connections,
                policy_name=args.policy, metadata_timeout=args.metadata_timeout)
        if args.command == "download":
            torrent = load_source(args.torrent)
            args.output.parent.mkdir(parents=True, exist_ok=True)
            policy = {"heuristic": ThroughputPolicy, "bandit": BanditPolicy,
                      "adaptive": AdaptivePolicy, "optimistic": OptimisticPolicy,
                      "recovery": RecoveryPolicy, "timed": TimeBudgetPolicy}[args.policy]()
            def progress(done, total):
                print(f"{done}/{total} verified bytes", file=sys.stderr)
            runner = download_magnet if isinstance(torrent, Magnet) else download
            extra = {"metadata_timeout": args.metadata_timeout} if isinstance(torrent, Magnet) else {}
            report = asyncio.run(runner(
                torrent, args.peer, args.output, timeout=args.timeout, piece_timeout=args.piece_timeout,
                pipeline=args.pipeline, concurrency=args.concurrency, max_connections=args.max_connections,
                resume=args.resume, policy=policy, use_trackers=not args.no_trackers,
                use_dht=not args.no_dht, dht_bootstrap=args.dht_bootstrap,
                peer_retries=args.peer_retries, retry_delay=args.retry_delay,
                endgame=not args.no_endgame, endgame_delay=args.endgame_delay,
                endgame_budget=args.endgame_budget,
                listen_host=args.listen_host, listen_port=args.port,
                progress=progress if args.progress else None, **extra))
        elif args.command == "seed":
            asyncio.run(seed_file(args))
            return 0
        elif args.command == "create":
            torrent = create(args.file, args.output, piece_length=args.piece_length,
                             trackers=args.tracker, nodes=args.node)
            print(f"Created {args.output} ({torrent.info_hash.hex()})")
            return 0
        elif args.command == "inspect":
            torrent = Torrent.load(args.torrent)
            report = dict(name=torrent.name, length=torrent.length, piece_length=torrent.piece_length,
                          pieces=len(torrent.hashes), info_hash=torrent.info_hash.hex(), trackers=torrent.trackers,
                          private=torrent.private, nodes=torrent.nodes)
            if torrent.multi_file:
                report["files"] = [dict(path="/".join(f.path), length=f.length, offset=f.offset)
                                   for f in torrent.files]
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
