# Initial policy comparison

Command: `python -m cbtorrent benchmark --trials 5 --size-mib 1 --seed 2026 --report benchmarks/comparison-2026-09-26.json`

Windows, Python 3.13.2; real loopback TCP; 1 MiB payload, 32 KiB pieces,
two concurrent transfers, eight pipelined blocks. Five trials per policy per
scenario, with paired peer ordering and randomized policy execution order.
All 30 downloads completed and verified successfully.

| Scenario | Heuristic median (s) | Bandit median (s) | Framing overhead, both (bytes) | Wasted payload, both (bytes) |
| --- | ---: | ---: | ---: | ---: |
| Mixed speed | 0.731 | 0.792 | 2,673 | 0 |
| Uniform speed | 0.992 | 0.993 | 2,673 | 0 |
| Corrupt peer | 1.230 | 1.227 | 2,733 | 32,768 |

The bandit was about 8.4% slower in the mixed-speed median and offered no byte
overhead improvement. The small differences in the other scenarios do not show
a useful advantage. **Keep the heuristic as default.** This is an initial local
measurement, not evidence about public swarms or a tuned final learning policy.

The [raw report](comparison-2026-09-26.json) includes every trial, sample standard
deviations, CPU/decision time, failures, observations, and configuration. CPU
measurement includes the local seeders and has coarse resolution on this host.
Application-level pacing is not a model of real TCP congestion. More scenarios,
larger files, independent processes, and held-out churn tests are needed before
policy tuning or performance claims.

`smoke.json` records the earlier one-trial 128 KiB smoke test; use the five-trial
report above for this comparison. Reports are retained as measurement artifacts,
not expected timing assertions in CI.
