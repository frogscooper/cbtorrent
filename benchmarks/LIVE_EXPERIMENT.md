# Live swarm comparison: heuristic vs adaptive

First policy comparison on real public swarms. Raw report:
[live-swarm-20261103.json](live-swarm-20261103.json) (every trial, failure phases,
DHT error samples, code commit `7c91ca7`, which has the same `cbtorrent/` code as 0.3.1).

```bash
python benchmarks/run_live.py --torrent debian.torrent --torrent arch.torrent \
  --policies heuristic,adaptive --rounds 3 --deadline 1800 --report benchmarks/live-swarm-20261103.json
```

## Setup

- Torrents: `debian-13.7.0-amd64-netinst.iso` (756 MiB, 1 tracker) and
  `archlinux-2026.11.01-x86_64.iso` (1.23 GiB, no trackers, so DHT and PEX only).
- One home connection, Windows 11, Python 3.13, 2026-11-03 21:54–23:04 US Eastern.
- Default download settings. 3 rounds, policies in seeded random order within
  each round, a separate process per trial, 30-minute deadline.

## Results

All 12 of 12 trials completed and verified. No deadlines, crashes, or hash failures.

| Torrent | Policy | Completed | Completion seconds (rounds 1, 2, 3) | Median |
| --- | --- | ---: | --- | ---: |
| Debian | heuristic | 3/3 | 258.6, 367.5, 190.9 | 258.6 |
| Debian | adaptive | 3/3 | 268.7, 455.4, 248.5 | 268.7 |
| Arch | heuristic | 3/3 | 543.5, 391.6, 295.9 | 391.6 |
| Arch | adaptive | 3/3 | 418.0, 353.6, 427.0 | 418.0 |

Paired ratio (adaptive time ÷ heuristic time, same round and torrent): median
**1.14** over 6 pairs (1.04, 1.24, 1.30 on Debian; 0.77, 0.90, 1.44 on Arch).
Adaptive was faster in 2 of 6 pairs.

**Conclusion: keep `heuristic` as the default.** Adaptive showed no advantage. With 6
pairs and round-to-round swings of up to 1.9× for the same policy, this also does
not show that adaptive is worse. It shows that any difference is smaller than this
swarm noise.

## What else the run showed

- **Uploads were zero in all 12 trials** (`uploaded_bytes` = 0), although the
  incoming listener runs during downloads. No peer connected to us. The likely cause is
  that the home router does not forward the listen port, and cbtorrent has no NAT mapping
  (UPnP/NAT-PMP). That makes cbtorrent a leecher in practice.
- **Most peer failures are TCP connect failures:** 121–473 per policy across three runs,
  against 20–42 handshake failures. Most discovered addresses are unreachable (dead or
  behind NAT). Missing encryption/uTP shows up at most in the smaller handshake count.
  Dial-ahead (#21) is what keeps these from stalling the start.
- DHT alone was enough: the Arch swarm, without trackers, completed every time.
- Wasted payload stayed under 1.5 MB per download (0 on Debian).

## Limits

Two swarms, one network, one evening, 3 rounds. Swarm speed changed between rounds
more than between policies. Do not generalize this to other swarms or networks.
A stronger test needs more rounds, more torrents, and runs at different times.
