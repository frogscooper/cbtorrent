# Peer recovery and endgame ablation

Run `python benchmarks/run_resilience.py --trials 3 --report benchmarks/new-name.json`
from the checkout. It reuses the independent TCP peers in `tests/test_lifecycle.py`.
Both modes use the same heuristic and engine. One disables retries/endgame; the
other enables two retries and endgame. Mode order is randomized within each pair.
All 24 attempts are retained in `resilience-ablation-20260930.json`.

The fixtures use one 65,762-byte piece, a 400 ms I/O deadline, 10 ms initial
backoff and 30 ms endgame delay, with one normal slot and two connection slots.
These accelerated timers differ from production defaults. This is a mechanism
experiment, not a public-swarm or independent-client speed comparison.

| Scenario | Without features, completed | With features, completed | Median completion without / with |
| --- | --- | --- | --- |
| Stable peer | 3/3 | 3/3 | 3.48 / 3.54 ms |
| Disconnect once | 0/3 | 3/3 | failed / 23.3 ms |
| Stalled final block | 3/3 | 3/3 | 416 / 39.5 ms |
| Always disconnected | 0/3 | 0/3 | failed / failed |

In the stalled-tail fixture the helper requested 226 bytes. Median payload fell
from 131,298 to 65,762 bytes and wasted payload from 65,536 to zero. Both modes
produced the exact source bytes. Protocol overhead fell from 617 to 514 bytes.

Recovery costs extra traffic when a peer never returns: the unrecoverable case
used three connection attempts rather than one, and median overhead rose from
322 to 966 bytes. That cost is deliberately bounded; successful pieces never
reset a peer's failure allowance. Stable-peer overhead stayed at 311 bytes.

Three trials are a small descriptive sample. Their results do not establish
general superiority, congestion behavior, or a benefit from any ML policy.
Correctness gates live in the lifecycle tests: cancellation/backoff, retry and
connection limits, shared-buffer commits, mixed corruption, disk failure, and
legal late replies after a cancel.
