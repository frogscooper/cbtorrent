# cbtorrent

A Python BitTorrent client built from the protocol up, with an experimental
online-learning policy for peer management. The two objectives are **download
completion time** and **overhead**. The default policy is a measurable heuristic;
the optional bandit must earn its place through comparison.

Python 3.11+, standard-library runtime, MIT licensed. This replaces the earlier
`mltorrent` handshake/ranker prototype, which remains in Git history.

## Quick start

From a checkout:

```powershell
python -m unittest discover -s tests -v
python -m cbtorrent --help
```

Optional installation in a virtual environment:

```powershell
python -m venv .venv
.venv\Scripts\python -m pip install -e .
.venv\Scripts\cbtorrent --help
```

On Linux/macOS, use `.venv/bin/python` and `.venv/bin/cbtorrent`.

Download a **single-file v1** torrent using its trackers:

```powershell
python -m cbtorrent download example.torrent --output downloads/example.bin --progress
```

Use explicit peers, choose the learning policy, and save measurements:

```powershell
python -m cbtorrent download example.torrent --peer 127.0.0.1:6881 --no-trackers --output downloads/example.bin --policy bandit --report reports/run-01.json
```

To resume, repeat the download command with `--resume`. Every existing piece is
rehashed; corrupted/missing pieces are downloaded again. The output directory is
created by the CLI, existing completed files are never overwritten, and unfinished
work lives at `{output}.part`. Reports are also exclusive writes, so use a new
report filename for each run. Ctrl+C closes sockets and retains partial work.

The CLI listens on `0.0.0.0` with an automatically assigned port while downloading,
sharing verified pieces with incoming connections. Set `--port 6881` for a stable
announced port or `--listen-host 127.0.0.1` for local experiments. There is no
automatic NAT port mapping.

## Desktop GUI

A minimal tkinter progress window (stdlib only) drives the same in-process download:

```powershell
python -m cbtorrent gui example.torrent --output downloads/example.bin --peer 127.0.0.1:6881 --no-trackers
```

Shows piece progress, down/up rates, peer count, ETA, and a peer list. **Cancel**
(or Esc) stops the session cleanly and keeps the `.part` file; **Open folder**
reveals the output directory. Requires a Python build with tkinter.

## Create and seed

```powershell
python -m cbtorrent create sample.bin --output sample.torrent --tracker https://tracker.example/announce
python -m cbtorrent inspect sample.torrent
python -m cbtorrent seed sample.torrent --file sample.bin --port 6881
```

For a local two-terminal test, omit `--tracker` when creating the torrent, seed
with `--listen-host 127.0.0.1 --no-trackers`, then download using
`--peer 127.0.0.1:6881 --no-trackers` to a different output file. The seed command
verifies the complete file before serving and runs until Ctrl+C. A download exits
on completion; use `seed` to continue sharing afterward.

## Implemented

- Strict, bounded bencoding; single-file v1 metainfo and canonical info hashing.
- TCP handshakes, bitfields/have, choke state, and pipelined 16 KiB block requests.
- Concurrent pieces, rarest-first selection over known connected peers, bounded
  connection caching, peer-failure fallback, SHA-1 verification, and safe resume.
- An incoming upload listener during downloads and a standalone seed server, with
  bounded clients/queues and cancellation of pending upload requests.
- HTTP(S) trackers, chunked replies, compact IPv4/IPv6 and dictionary peer lists;
  IPv4 UDP trackers; startup, periodic, completion, and shutdown announces.
- Explicit peer endpoints (including IPv6), configurable timeouts and concurrency.
- JSON metrics, per-peer observations, a heuristic, and an optional online bandit.
- An opt-in adaptive service-time model (`--policy adaptive`) with recent learning.
- Real loopback TCP/HTTP/UDP tests and paired policy benchmarks.

Run `python -m cbtorrent download --help` for tuning options. Defaults: 4 concurrent
piece transfers, up to 16 cached outbound connections, 8 pipelined requests per
peer, 15-second I/O timeout, and 120-second total piece deadline. Up to 200 peer
candidates and the first 8 unique tracker URLs are considered. Incoming clients
have a separate cap equal to `--max-connections`.

## Policy experiment

See repository docs under `benchmarks/` for heuristic/bandit/adaptive/recovery/timed
policy details and held-out results. The heuristic remains the default.

## Reproducible benchmark

```powershell
python -m cbtorrent benchmark --trials 5 --size-mib 1 --seed 2026 --report benchmarks/comparison.json
```

## Contributing

See [AGENTS.md](AGENTS.md) for architecture and shared-agent working guidance.
CI runs the suite and installed CLI on Windows and Linux, Python 3.11 and 3.13.

Protocol references: [BEP 3](https://www.bittorrent.org/beps/bep_0003.html),
[BEP 23](https://www.bittorrent.org/beps/bep_0023.html), and
[BEP 15](https://www.bittorrent.org/beps/bep_0015.html).
