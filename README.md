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

Plain `python -m cbtorrent` (or `cbtorrent`) opens the desktop GUI. Headless
downloads require an explicit `download` subcommand.

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

A minimal tkinter progress window (stdlib only) drives the same in-process download.
**GUI is the default:** no args opens an idle window. The `gui` subcommand remains
an explicit alias. Headless scripts must say `download` (the old bare-path-as-download
shortcut is gone).

```powershell
python -m cbtorrent
python -m cbtorrent gui
python -m cbtorrent example.torrent --output downloads/example.bin --peer 127.0.0.1:6881 --no-trackers
python -m cbtorrent gui example.torrent --output downloads/example.bin --peer 127.0.0.1:6881 --no-trackers
```

A bare `.torrent` path opens the GUI preloaded with that torrent (honoring
`--output` / `--peer` / `--policy` / `--no-trackers` and other gui flags). Without
a display or tkinter, the process exits non-zero and points you at
`cbtorrent download` — there is no silent CLI fallback.

Toolbar: **Add torrent…**, policy menu (heuristic default), **Start** / **Stop**.
Shows piece progress, down/up rates, peer count, ETA, and a peer list. Esc stops
a running download and keeps the `.part` file. Requires a Python build with tkinter.

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
- Concurrent pieces, rarest-first selection, bounded connection caching, resume.
- Incoming upload listener during downloads and a standalone seed server.
- HTTP(S)/UDP trackers; explicit peers; JSON metrics and peer policies.
- Opt-in adaptive / optimistic / recovery / timed policies; heuristic remains default.
- Desktop GUI by default (`cbtorrent` / `cbtorrent gui`) with live observe snapshots.

See `benchmarks/` for policy experiment docs and held-out results. Run
`python -m cbtorrent download --help` for tuning options.

## Contributing

See [AGENTS.md](AGENTS.md) for architecture and shared-agent working guidance.
CI runs the suite and installed CLI on Windows and Linux, Python 3.11 and 3.13.

Protocol references: [BEP 3](https://www.bittorrent.org/beps/bep_0003.html),
[BEP 23](https://www.bittorrent.org/beps/bep_0023.html), and
[BEP 15](https://www.bittorrent.org/beps/bep_0015.html).
