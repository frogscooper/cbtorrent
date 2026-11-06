# Learned streaming policy: simulated and live results

`--policy learned` (see README "Learned streaming policy") against the default
`heuristic`. Four reports, all from code commit `c620eec`:

| Report | What | Pairs |
| --- | --- | ---: |
| [swarm-sim-heldout-20261104.json](swarm-sim-heldout-20261104.json) | Loopback swarm with delay, held-out seed | 5 |
| [learned-regression-20261104.json](learned-regression-20261104.json) | Standard `cbtorrent benchmark` scenarios | 9 |
| [live-learned-20261104.json](live-learned-20261104.json) | Debian and Arch, public swarms, 3 rounds | 6 |
| [live-learned-order-check-20261104.json](live-learned-order-check-20261104.json) | Same, 1 round, heuristic run first | 2 |

## 1. Simulated swarm (held out)

```bash
python benchmarks/run_swarm_sim.py --trials 5 --seed 20261104 --report benchmarks/swarm-sim-heldout-20261104.json
```

30 seeds per trial with log-uniform upload rates (20 KiB/s to 2 MiB/s) and
30–300 ms round trips, plus 8 dead addresses, a 48 MiB payload, and 256 KiB pieces.
The policy's constants were tuned on other seeds; seed 20261104 was run once,
after tuning. `wide-static` is a control with no learning (12 slots, 64 requests per peer).

| Trial | heuristic | wide-static | learned |
| ---: | ---: | ---: | ---: |
| 1 | 28.57 s | 14.60 s | 6.90 s |
| 2 | 34.12 s | 15.11 s | 12.63 s |
| 3 | 28.35 s | 14.70 s | 13.12 s |
| 4 | 20.56 s | 10.31 s | 7.76 s |
| 5 | 30.46 s | 15.14 s | 7.77 s |

All 15 downloads were complete and matched byte for byte. `learned` was the fastest in 5/5
trials (median ratio vs heuristic 0.37). Cost: abandoned partial pieces wasted
65–623 KB per 48 MiB (up to 1.3%). Heuristic and wide-static wasted 0.

## 2. Standard benchmark scenarios

```bash
python -m cbtorrent benchmark --policies heuristic,learned --trials 3 --report benchmarks/learned-regression-20261104.json
```

| Scenario | heuristic (3 trials) | learned (3 trials) | Result |
| --- | --- | --- | --- |
| uniform | 1.000, 0.994, 0.997 s | 0.685, 0.684, 0.686 s | learned 31% faster |
| mixed | 0.725, 0.731, 0.714 s | 0.780, 0.925, 0.776 s | **learned 14% slower** |
| corrupt-peer | 1.203, 1.202, 1.207 s | 1.211, 1.206, 1.200 s | equal; bad peer banned |

All 18 runs succeeded. The `mixed` slowdown is real in this fixture: it is a
sub-second download, and the first slot epoch and the exploration pieces cost more
there than they save. Overhead differed by 136 bytes at most.

## 3. Live public swarms

```bash
python benchmarks/run_live.py --torrent debian.torrent --torrent arch.torrent \
  --policies heuristic,learned --rounds 3 --deadline 1800 --report benchmarks/live-learned-20261104.json
```

Same torrents, machine, and network as [LIVE_EXPERIMENT.md](LIVE_EXPERIMENT.md):
Debian 13.7 netinst (756 MiB) and Arch 2026.11.01 (1.23 GiB, DHT and PEX only).
Home broadband, Windows 11, Python 3.13, 2026-11-04 18:24–19:18 US Eastern. The router has
no port mapping, so no inbound peers (0 uploads in every trial).

| Torrent | Round | heuristic | learned | Ratio |
| --- | ---: | ---: | ---: | ---: |
| Debian | 1 | 465.7 s | 38.7 s | 0.083 |
| Debian | 2 | 332.0 s | 38.1 s | 0.115 |
| Debian | 3 | 216.2 s | 55.3 s | 0.256 |
| Arch | 1 | 304.0 s | 78.3 s | 0.257 |
| Arch | 2 | 383.4 s | 63.8 s | 0.166 |
| Arch | 3 | 319.5 s | 53.7 s | 0.168 |
| Debian | check | 286.2 s | 56.3 s | 0.197 |
| Arch | check | 362.8 s | 153.6 s | 0.423 |

All 16 trials completed and verified, with no hash failures and no deadlines. `learned` was faster
in **8 of 8 pairs**. The median ratio was 0.17 for the 6 main pairs and 0.18 for all 8,
so a download took about a sixth of the time.

**Order check.** The seeded shuffle put `learned` first in all 6 main pairs (a
1-in-64 chance). A swarm can change between two back-to-back downloads, so the
check round used seed 20261107, which runs `heuristic` first on both torrents.
`learned` still won both pairs. Its Arch time (153.6 s) was its slowest, so going
second did not help it.

Other measurements, per download (median of the 3 main rounds):

| | heuristic | learned |
| --- | ---: | ---: |
| Connections, Debian / Arch | 101 / 97 | 45 / 43 |
| Wasted payload, Debian / Arch | 0 / 0 | 32 KB / 852 KB |

`learned` finishes before it needs to try as many addresses. Its waste is at most
0.07% of the Arch image.

## What this shows and what it doesn't

- On these two well-seeded swarms, from this connection, `learned` was 2.4–12×
  faster than the default in every pair. The gain is far larger than the
  round-to-round noise, which reached 2.2× for heuristic.
- The likely mechanism matches the simulation. Heuristic's four slots, each with an
  8-request queue that drains at every piece boundary, cap throughput per
  connection by round-trip time. Streaming plus learned windows removes that cap.
- **Not shown:** behavior on small or poorly seeded swarms, on other networks, with
  inbound peers, or over many days. The `mixed` fixture shows that `learned` can
  be slower on very short downloads.
- `policy_diagnostics` is not kept in live reports (`KEPT_METRICS`), so the live
  slot counts and abandon counts are not recorded.

**Conclusion:** keep `heuristic` as the default for now and offer `learned` as an
opt-in. Make it the default only after a live run on more swarms and at least one
other network.
