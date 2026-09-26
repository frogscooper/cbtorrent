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
work lives at `<output>.part`. Reports are also exclusive writes, so use a new
report filename for each run. Ctrl+C closes sockets and retains partial work.

The CLI listens on `0.0.0.0` with an automatically assigned port while downloading,
sharing verified pieces with incoming connections. Set `--port 6881` for a stable
announced port or `--listen-host 127.0.0.1` for local experiments. There is no
automatic NAT port mapping.

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

`heuristic` explores unseen peers, then selects by observed verified bytes per
second, penalized by failures. `bandit` uses a UCB1-style exploration bonus and a
bounded reward:

```text
reward = max(0, goodput / (goodput + 1 MiB/s) - 0.2 * (1 - useful_bytes / wire_bytes))
score  = mean_reward + 0.25 * sqrt(2 * log(total_samples) / peer_samples)
```

A sample covers one scheduled piece attempt, including connection setup when
needed. This is a non-contextual multi-armed bandit using goodput and byte
efficiency as proxies for the objectives, not a trained contextual model or a
guarantee of faster completion. State is scoped to the download and does not use
IP-prefix identity, future peer performance, or benchmark ground truth.

The policies share the same protocol engine, piece scheduler, and resource caps.
Corrupt, choked, disconnected, and timed-out peers are retired for the run.
Changing peers between pieces can reuse cached connections.

`adaptive` learns how long a verified piece takes using a small exponentially
weighted least-squares model. It discounts old samples when rates change instead
of averaging all historical throughput. Initial exploration is limited near the
end of a download. An availability-aware tail planner can briefly defer an idle
slow peer if an active fast peer is predicted to finish the remaining work sooner;
it rechecks at 50 ms intervals and refuses to wait on stale progress. The model
trains only on successfully committed data and uses no future swarm information.

```powershell
python -m cbtorrent download example.torrent --output downloads/example.bin --policy adaptive
```

The original heuristic remains the default. See
[the ML experiment](benchmarks/ML_EXPERIMENT.md) for the evaluation plan, results,
ablations, and scope of any improvement claims.

`--policy recovery` additionally revisits overlooked peers and detects sustained
changes in their service cost. Revisits use cached connections and useful pieces,
reserve at most 1/16 of bytes already verified, and stop near completion. Peers
that stay unchanged are revisited less often. Two large, consistent prediction
errors reset stale history; one outlier does not. This can discover recovering
peers but can also slow downloads when probes find no improvement. The byte
allocation limit is not a wall-time limit; normal transfer deadlines still apply.
See [the recovery experiment](benchmarks/RECOVERY_EXPERIMENT.md) for fresh held-out
results and comparison with an EWMA using the same probing rules.

## Reproducible benchmark

```powershell
python -m cbtorrent benchmark --trials 5 --size-mib 1 --seed 2026 --report benchmarks/comparison.json
```

The benchmark uses real TCP transfers on loopback with application-level rates
and delays. It compares mixed-speed, uniform-speed, and corrupt-peer scenarios.
Both policies receive the same shuffled peer ordering per trial; execution order
is randomized and policy state starts fresh. Reports retain raw trials,
environment/configuration, successes, medians, and sample standard deviations.

Use `--suite development` or `--suite validation` for the distinct ML scenarios.
The validation suite varies piece size, rates, and speed-change schedules.
For ablations, `--policies heuristic,adaptive,adaptive-no-defer,planned-heuristic`
compares recent learning with and without tail deferral and against a planner
using lifetime throughput. Reports include paired bootstrap intervals and a source
hash. Per-scenario intervals are descriptive and not multiple-comparison corrected.

The subsequent `recovery-development` and `recovery-validation` suites compare
`heuristic,adaptive,recovery,recovery-no-probe,ewma-probe`. Schema 3 reports include
comparisons against each available heuristic/adaptive/EWMA baseline, missing pair
counts, and maximum as well as median overhead/waste differences. Duplicate trial
keys are rejected. `recovery-no-probe` isolates the change detector; `ewma-probe`
uses identical probe eligibility, backoff, and byte limits with a simple moving
average. Both are benchmark-only ablations.

These small local scenarios are regression/research fixtures. They do not model
real TCP congestion, NAT, public-swarm churn, or disk contention. Seeder CPU is
included because fixtures share the process. Timing depends on the OS scheduler.
Do not infer public-swarm improvement from a local win.

## Measurement definitions

| Field | Meaning |
| --- | --- |
| `completion_seconds` | Wall time through discovery, transfer, verification, disk flush, and exclusive publication; null on failure. |
| `elapsed_seconds`, `cpu_seconds` | Wall and process CPU time at the completion/failure snapshot. |
| `verified_bytes` | Newly received payload committed after a matching hash. |
| `resumed_bytes` | Existing payload that passed revalidation; kept separate from network traffic. |
| `wire_sent_bytes`, `wire_received_bytes` | Peer-protocol TCP bytes queued for sending or consumed by parsing, including handshake/framing. |
| `payload_received_bytes`, `uploaded_bytes` | Received block data and sent upload block data. |
| `protocol_overhead_bytes` | Peer wire bytes in both directions minus block payload in both directions. |
| `wasted_payload_bytes` | Received payload minus newly verified bytes, including corrupt/retried data. |
| `tracker_response_bytes` | HTTP response bodies or UDP response datagrams, separately from peer traffic. |
| `connections`, `peer_failures`, `hash_failures` | Outbound connection attempts, retired sessions, and integrity failures. |
| `policy_seconds` | Time spent selecting peers. |
| `policy_update_seconds` | Additional time spent training an online policy. |
| `policy_deferrals` | Scheduling decisions that briefly waited for in-flight work. |
| `policy_diagnostics` | Recovery-policy counters: revisit selections (`probes`), conservative `reserved_probe_bytes`, successfully committed `training_bytes`, and confirmed `model_resets`. Initial peer discovery is excluded from probes. Reservations are not refunded after failure and do not represent duplicate/wasted payload. |

Metrics are a snapshot before post-completion tracker announcements and final
connection cleanup. They exclude TCP/IP headers, retransmissions, unconsumed
socket buffers, tracker request/header bytes, and memory use. Sent byte counts
measure queued bytes, not acknowledged delivery. Packet capture and separate
memory profiling are needed for complete host/network accounting.

## Current boundaries

This is an experimental client, not a replacement for a mature torrent client.
There is no magnet/DHT/PEX, uTP, encryption, multi-file/v2 torrent support, endgame
duplication, automatic NAT mapping, or upload-slot tit-for-tat. Uploads currently
use the incoming listener; outbound connections remain download-only. Cached
outbound availability is refreshed when scheduled, not by an always-running
reader. Peers retired after a failure are not retried within that run. UDP tracker
packets are not retransmitted within one announce attempt, and tracker tiers are
flattened. Exhausting usable candidates fails with resumable work rather than
waiting indefinitely for a future announce.

Storage/hash operations run on the event loop; very large/slow disks need a
future I/O worker design. Publication uses an exclusive hard link in the same
directory (supported by NTFS and typical Linux filesystems). Unsupported filesystems
retain the completed `.part` file. Only local fixtures have been validated so far;
independent-client/public-swarm interoperability remains to be tested.

## Contributing

See [AGENTS.md](AGENTS.md) for architecture and shared-agent working guidance.
CI runs the suite and installed CLI on Windows and Linux, Python 3.11 and 3.13.
Useful next work: bidirectional outbound sessions, independent interoperability,
endgame with an explicit duplicate-byte budget, bounded peer retries, disk I/O
workers, and richer held-out churn/latency experiments.

Protocol references: [BEP 3](https://www.bittorrent.org/beps/bep_0003.html),
[BEP 23](https://www.bittorrent.org/beps/bep_0023.html), and
[BEP 15](https://www.bittorrent.org/beps/bep_0015.html).
