# Bounded peer revisits and change detection

## Design

The previous adaptive model cannot discover that an unused slow peer recovered.
The opt-in `recovery` policy keeps per-peer online service-time regression, adds
two-sample change confirmation, and periodically revisits stale cached peers.
It disables the previous tail deferral mechanism, whose value was not established.

- Initial discovery follows adaptive's existing rule.
- Revisits reserve at most **1/16 of bytes already verified**. They transfer useful
  unclaimed pieces, not duplicate/test payload. Initial discovery is excluded.
- Only cached, eligible peers can be probed. Reserve the full nominal piece size
  before selection; failed/unused reservations are not refunded.
- A peer must be stale for at least 16 verified completions. Unchanged probes
  double that interval up to 256; a greater-than-twofold change restores 16.
- No probes with at most four times concurrency unclaimed pieces. Predicted probe
  duration must also fit within 20% of estimated remaining work at the best idle
  peer's speed, divided by concurrency. This prediction is only a gate: **there
  is no hard probe wall-time guarantee**. Existing network deadlines still apply.
- Two successive service-cost observations outside a factor of two of a frozen
  pre-change prediction, in the same direction, discard old regression history.
  Both confirming samples seed the replacement estimate. One outlier alone does
  not reset history. This is change detection, not a calibrated statistical test.
- Failed hashes, disk writes, cancellations, and uncommitted payload never train.
  State is local to one download and bounded by the engine's 200-peer limit.

`ewma-probe` uses a simple EWMA of duration per byte with identical revisit rules.
`recovery-no-probe` isolates the responsive model without revisits. Existing
`heuristic` and `adaptive` implementations remain available unchanged.

## Development history

Seed 1603, 3 trials, 3 MiB, five development scenarios, five policies. First
iteration revisited unchanged peers every 16 completions. It recovered useful
speed but regressed stationary performance, motivating capped exponential backoff.
The second iteration adds that backoff. Preserve both raw reports rather than
presenting only the better iteration. Reporting/diagnostics and validation cases
were added after those processes started; source hashes identify their snapshots.

All 75 downloads completed in each development run. Mean paired completion-time
improvement relative to the heuristic (positive is faster):

| Development case | Before backoff | With backoff |
| --- | ---: | ---: |
| Stable mix | -3.81% | -4.07% |
| Recovery | +20.27% | +20.33% |
| Rate switch | +24.39% | +29.18% |
| Very slow peer | +1.11% | +0.04% |
| Equal peers | +0.08% | +0.08% |

Backoff limits repeated unchanged probes but did **not** remove the stable-mix
regression in these short downloads. Three trials are insufficient to attribute
differences between iterations to backoff rather than scheduling noise. Some
development trials overlapped local unit tests; held-out timing runs are isolated
from local tests and other benchmark processes. Raw development evidence:
[first iteration](recovery-development-01.json),
[backoff iteration](recovery-development-02.json).

## Frozen validation plan

Freeze parameters after development. Run `recovery-validation` with seed **2809**,
**7 trials**, **3 MiB**, and all five policies. Eight new scenarios vary early/late
recovery, repeated rate changes, stationary mixes, high per-block delay, corruption,
and a slow outlier. Piece sizes are 16/32/64 KiB, with three or four peers. Same
peer order per trial; randomized policy execution order. No model receives future
rates, scenario identities, or fixture event times.

Report all scenarios against both the original heuristic and the previous
adaptive policy, plus the EWMA baseline. Include failures/missing pairs and all
raw metrics, not just successful timing medians. Bootstrap intervals use 2,000
paired resamples and are descriptive, without multiple-comparison correction.
No parameter changes after looking at validation; subsequent changes require a
new evaluation. The heuristic stays the default regardless of local gains.

These are real loopback TCP transfers with application-level pacing, not public
swarm or TCP congestion emulation. Policy selection/training overhead is measured
separately, but process CPU includes seeders. Tests assert safety and deterministic
learning behavior; CI does not assert noisy wall-clock speedup thresholds.

```powershell
python -m cbtorrent benchmark --suite recovery-validation --trials 7 --size-mib 3 --seed 2809 --policies heuristic,adaptive,recovery,recovery-no-probe,ewma-probe --report benchmarks/my-recovery-validation.json
```

## Results

Validation pending.
