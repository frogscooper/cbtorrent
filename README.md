# cbtorrent

A from-scratch Python BitTorrent v1 client with measured peer selection.
The goals are shorter download completion time and lower overhead, judged
with the same protocol engine and the same resource caps.

Python 3.11+, standard-library runtime (tkinter for the desktop UI), MIT license.
This replaces an earlier `mltorrent` handshake prototype that still exists in Git history.

## What this is

`cbtorrent` is an experimental research client, not a replacement for a mature torrent app.
Much of the implementation was written with AI coding assistants. Review the code and the
benchmarks yourself before you treat any policy result as settled.

**Default download policy: `heuristic`.** Experimental online-learning policies
(`bandit`, `adaptive`, `optimistic`, `recovery`, `timed`) are opt-in through `--policy`.
They share the same engine as the heuristic. A local loopback win is not evidence of
public-swarm improvement. Details and held-out results live under [`benchmarks/`](benchmarks/).

## Quick start

```bash
python -m unittest discover -s tests -v
python -m cbtorrent --help
```

With no arguments, `python -m cbtorrent` (or the `cbtorrent` console script) opens the
desktop GUI. Headless work uses an explicit subcommand such as `download`.

Optional editable install:

```bash
python -m venv .venv
```

Activate the venv, then:

```bash
python -m pip install -e .
cbtorrent --help
```

### Desktop GUI

Stdlib tkinter only. Idle window when argv is empty. `gui` remains an explicit alias.
A bare `.torrent` path opens the GUI with that torrent preloaded (flags such as
`--output`, `--peer`, `--policy`, and `--no-trackers` still apply).

```bash
python -m cbtorrent
python -m cbtorrent gui
python -m cbtorrent example.torrent --output downloads/example.bin
python -m cbtorrent gui example.torrent --output downloads/example.bin --peer 127.0.0.1:6881 --no-trackers
```

If there is no display, no tkinter, or `Tk()` fails, the process exits non-zero and
points you at `cbtorrent download`. There is no silent CLI fallback.

Toolbar: **Add** | **Remove** | **Pause** | **Resume** | **Policy** (heuristic is the
default for new torrents and the session). Upper pane is the queue
(`#` | Name | Progress | ↓ | ↑ | Status) in session order. Selecting a row shows
detail: progress bar, rates, peers. Esc pauses the selected downloading torrent and
keeps the `.part` file. At most one download runs at a time.

Queue and status persist to `~/.cbtorrent/session.json` (Windows:
`%USERPROFILE%\.cbtorrent\session.json`). On open, the GUI restores the queue and
auto-resumes at most one torrent that was downloading.

### Headless download

Single-file and multi-file BitTorrent v1 torrents are supported.

```bash
python -m cbtorrent download example.torrent --output downloads/example.bin --progress
```

Explicit peers, an opt-in policy, and a metrics report:

```bash
python -m cbtorrent download example.torrent \
  --peer 127.0.0.1:6881 --no-trackers --no-dht \
  --output downloads/example.bin \
  --policy bandit \
  --report reports/run-01.json
```

Resume with `--resume` (every existing piece is rehashed). The CLI creates the output
directory, never overwrites a completed file, and keeps unfinished work at
`<output>.part`. Report paths are exclusive writes: pick a new filename per run.
Ctrl+C closes sockets and keeps partial work.

While downloading, the client listens on `0.0.0.0` with an ephemeral port and shares
verified pieces with incoming peers. Use `--port 6881` for a stable announced port, or
`--listen-host 127.0.0.1` for local experiments. There is no automatic NAT mapping.

### Multi-file directories

`create` accepts a directory and streams its files in sorted relative-path order.
Pieces can span files; zero-length files are included. Empty directories are not
represented in v1 metainfo, and a source directory must contain at least one file.

```sh
python -m cbtorrent create my-folder --output folder.torrent
python -m cbtorrent inspect folder.torrent
python -m cbtorrent download folder.torrent --output downloads/my-folder --progress
python -m cbtorrent seed folder.torrent --file downloads/my-folder
```

For a multi-file torrent, `--output` is the new directory containing the manifest
paths, and `seed --file` points directly to that directory. `inspect` lists each
file's path, length, and byte offset. The GUI downloads every file by default to
`downloads/<torrent-name>` and shows the file count in the selected torrent's
heading. Per-file selection is not implemented.

Downloads use a single `<output>.part` spool for the concatenated payload.
Resume rehashes its pieces, including pieces crossing file boundaries. At
completion, verified data is copied to a private staging directory, then linked
into an exclusively created destination directory. Existing destinations,
including empty directories, are refused. This requires hard-link support and
roughly twice the payload size in free disk space during publication. Copying
and linking yield between chunks/files so cancellation can clean up normally.

Publication of an entire directory is not atomic. A normal failure or cancellation
removes entries created by that attempt and retains the verified spool. A process
crash or power loss during publication can leave an incomplete destination or
staging directory. Move that destination aside before resuming from `.part`;
the client never merges into or overwrites an existing directory.

Manifest paths must be relative UTF-8 components. Traversal, separators within
components, Windows device names, and duplicate, case/Unicode-equivalent or
file/directory-conflicting targets are rejected. Multi-file sources and payload
paths refuse symlinks and Windows reparse points. Limits are 10,000 files,
64 components per path, 16 MiB metainfo, and the existing 16 MiB piece cap.
Local payloads must not be modified by another process while creating, downloading,
publishing, or seeding. Seeding validates all file lengths and piece hashes before
advertising availability, using at most one open payload file at a time.

### Create, inspect, seed

```bash
python -m cbtorrent create sample.bin --output sample.torrent --tracker https://tracker.example/announce
python -m cbtorrent inspect sample.torrent
python -m cbtorrent seed sample.torrent --file sample.bin --port 6881
```

Local two-terminal smoke test: omit `--tracker` when creating, seed with
`--listen-host 127.0.0.1 --no-trackers --no-dht`, then download with
`--peer 127.0.0.1:6881 --no-trackers --no-dht` to a different output path. `seed` verifies the
complete file before serving and runs until Ctrl+C. A download exits when the file is
done; keep seeding with `seed` if you want to stay available.

### DHT discovery

The CLI and desktop GUI enable IPv4 DHT for public torrents. It discovers peers
by info hash and announces the verified-piece listener's **TCP** port, using
[BEP 5](https://www.bittorrent.org/beps/bep_0005.html) `get_peers` and
`announce_peer`. Trackers and explicit peers remain available when DHT fails.
New peers enter the same bounded scheduler and hash verification as tracker peers.

```bash
python -m cbtorrent download example.torrent --output downloads/example.bin --no-trackers
python -m cbtorrent seed example.torrent --file downloads/example.bin --no-trackers
```

Use `--no-dht` on `download`, `seed`, or `gui` to disable it. **`--no-trackers`
alone does not disable DHT.** `--dht-bootstrap HOST:PORT` is repeatable and replaces
all default bootstrap endpoints. Otherwise, the torrent's `nodes` contacts are
used when present, falling back to `dht.transmissionbt.com:6881` and
`router.utorrent.com:6881`. `create --node HOST:PORT` embeds an explicit bootstrap
contact without adding built-in public routers to generated torrents.
The Python `download()` API preserves its prior network behavior: opt in with
`use_dht=True`; `dht_bootstrap=[]` disables bootstrap DNS entirely. Policy benchmarks
explicitly disable DHT, and DHT tests use only local UDP/TCP fixtures.

DHT starts alongside downloads and refreshes every five minutes. A download with
no usable peers waits for the initial lookup, bounded by the smaller of `--timeout`
and 15 seconds, then reports failure if none are usable. Existing transfers do not
wait for DHT. The node uses an ephemeral UDP port on `--listen-host`; announcements
use the separately bound TCP port. Both sockets close on completion or cancellation.
An IPv6-only listening address leaves IPv4 DHT unavailable; explicit IPv6 peers and
trackers still work. There is no automatic NAT mapping.

Replies must match both the transaction and source endpoint. Announces require a
secret, expiring token bound to the source IP and info hash. Requests and replies
are capped at 1200 bytes; lookups cap referrals at 64, queries at 32, parallel lookup
queries at 3, and collected peers at 200. A node has at most 8 pending requests,
256 routing contacts, and 128 stored info hashes with 50 peers each. Incoming query
replies are limited to 50 per second with a burst of 100. Routing contacts expire
after 15 minutes and announced peers after 30 minutes. Cancellation drains pending
queries; a lookup deadline retains any peers already discovered.

Metainfo with `info.private=1` never starts DHT or announces its hash through it,
as required by [BEP 27](https://www.bittorrent.org/beps/bep_0027.html). This does not
claim full private-tracker support: tracker-tier switching and tracker-only peer
provenance enforcement are still outside this implementation.

Reports add `dht_sent_bytes` and `dht_received_bytes` for UDP payload bytes, including
unmatched/malformed received datagrams; IP/UDP headers and DNS traffic are excluded.
`dht_requests` counts sent queries, `dht_failures` counts failed queries, DNS/bind
failures and whole-lookup deadline expirations (these can overlap), and `dht_peers`
counts unique peers per lookup, including peers already seen in previous lookups.
`dht_errors` contains setup errors. Existing `wire_*` and `protocol_overhead_bytes`
retain their peer-TCP-only meanings; DHT overhead is separate. Completion time
includes any initial wait for discovery.

## Peer policies

| Name | Role |
| --- | --- |
| `heuristic` | **Default.** Explore unseen peers, then prefer verified bytes per second with failure penalties. |
| `bandit` | Opt-in UCB1-style bandit on goodput and byte efficiency. Not a trained contextual model. |
| `adaptive` | Opt-in EWLS service-time model with limited late exploration and optional tail deferral. |
| `optimistic` | Opt-in adaptive variant with more optimistic exploration. |
| `recovery` | Opt-in revisits of overlooked peers under a byte budget; can help or hurt. |
| `timed` | Opt-in recovery plus a predicted time allowance for revisits. |

All policies use one protocol engine, one piece scheduler, and the same connection caps.
Corrupt, choked, disconnected, and timed-out peers are retired for that run. Policy state
is scoped to the download. Experiment plans, ablations, and acceptance gates:
[ML experiment](benchmarks/ML_EXPERIMENT.md),
[recovery experiment](benchmarks/RECOVERY_EXPERIMENT.md),
[time-budget experiment](benchmarks/TIME_BUDGET_EXPERIMENT.md).

Paired local TCP comparison:

```bash
python -m cbtorrent benchmark --trials 5 --size-mib 1 --seed 2026 --report benchmarks/comparison.json
```

These fixtures are regression and research tools. They do not model real congestion, NAT,
public churn, or disk contention. Do not treat a localhost win as a public-swarm claim.

## Implemented

- Bounded bencoding; single-file and multi-file v1 metainfo; canonical info hashing
- TCP handshake, bitfield/have, choke, pipelined 16 KiB requests
- Concurrent pieces, rarest-first among known peers, bounded connection cache, safe resume
- Incoming upload listener during downloads; standalone seed server
- HTTP(S) and IPv4 UDP trackers; explicit IPv4/IPv6 peers
- Bounded IPv4 DHT discovery, announcements, and KRPC query responses
- JSON metrics and per-peer observations
- Desktop GUI as the default launch path with multi-torrent queue and session persistence; headless subcommands for scripts and CI

Defaults for downloads: 4 concurrent pieces, up to 16 outbound connections, 8 pipelined
requests per peer, 15 s I/O timeout, 120 s piece deadline. Up to 200 peer candidates and
the first 8 unique tracker URLs. Incoming clients use a separate cap equal to
`--max-connections`. Run `python -m cbtorrent download --help` for the full flag list.

## Limits

Not present: magnet links, PEX, uTP, encryption, v2 torrents, endgame
duplication, automatic NAT mapping, classic tit-for-tat upload slots. Outbound connections
are download-oriented; uploads use the incoming listener. Peers retired after failure are
not retried in that run. Storage and hashing run on the event loop. Publication uses an
exclusive hard link in the same directory when the filesystem supports it; otherwise the
completed `.part` file remains. Local fixtures are validated; independent-client and
public-swarm interoperability are still open work.

DHT state is scoped to the running download/seed session. Persistent routing tables,
full bucket refresh/replacement probing, BEP 42 node-ID hardening, IPv6 DHT, and TCP
DHT `PORT` exchange are not implemented. This is a bounded discovery implementation,
not a complete long-lived DHT router. Magnet metadata exchange is a separate feature.

## Contributing

See [AGENTS.md](AGENTS.md) for architecture notes and shared-agent working rules.
CI runs the unit suite and the installed CLI on Windows and Linux (Python 3.11 and 3.13).

Protocol references: [BEP 3](https://www.bittorrent.org/beps/bep_0003.html),
[BEP 23](https://www.bittorrent.org/beps/bep_0023.html),
[BEP 15](https://www.bittorrent.org/beps/bep_0015.html).
