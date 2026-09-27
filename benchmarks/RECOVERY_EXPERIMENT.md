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
- Failed hashes, failed disk writes, and uncommitted cancelled payload never train.
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

All **280/280 downloads** completed and passed hash verification: eight scenarios,
seven trials, five policies. No policy parameters changed after validation began.
The tested source hash matches the published engine/model/benchmark files:
`3e3e42f38fad9d79a247211f6d68954c9d8b4d6e9fed1e73a8287f5c7e1bb955`.

Mean paired relative completion-time improvement for `recovery` (positive means
faster). Intervals in the adaptive column are descriptive paired-bootstrap 95%
intervals; the raw report retains intervals for every comparison.

| Held-out case | vs heuristic | vs previous adaptive (95% interval) | vs EWMA with probes |
| --- | ---: | ---: | ---: |
| Late recovery | -3.16% | -2.14% (-3.54 to -0.70) | -1.14% |
| Early recovery | **+26.44%** | **+26.28% (+25.71 to +26.76)** | **+18.40%** |
| Repeated changes | -7.41% | +0.16% (-3.18 to +2.90) | +2.12% |
| Stationary wide mix | -0.07% | -0.94% (-1.61 to -0.20) | -0.06% |
| Stationary equal peers | +0.29% | +0.21% (+0.02 to +0.43) | +0.19% |
| High latency | -1.23% | -1.51% (-1.88 to -1.18) | +0.10% |
| Corrupt fast peer | -1.47% | -1.99% (-4.81 to +0.80) | +0.36% |
| Slow outlier | -2.15% | -1.98% (-3.04 to -1.00) | -0.16% |

The useful gain is specific to discovering a peer that recovers early enough to
repay exploration and confirmation. It is **not a generally superior default**.
Revisits cost time when no useful improvement is found; rapid reversals can also
favor the lifetime-average heuristic over both recent-learning variants. Keep
these regressions visible rather than averaging unlike scenarios into a headline
"overall win." The heuristic remains the default; `recovery` is experimental.

In early recovery, the responsive model without revisits was essentially tied
with previous adaptive (+0.16%), while the EWMA with revisits gained +9.65% and
the full policy gained +26.28%. This supports keeping both rediscovery and faster
adaptation in this experimental option; probing alone captured less of the gain.

Measured wire bytes in each direction, protocol overhead, wasted payload, and
connection counts were **identical in every paired trial** against the heuristic,
previous adaptive policy, and EWMA baseline. This does not include TCP/IP headers
or retransmissions. The recovery policy selected 169 revisits across its 56 runs;
each reservation respected the verified-byte budget.

Median selection plus training time per download: heuristic **0.75 ms**, previous
adaptive **3.09 ms**, recovery **4.22 ms**, EWMA with probes **3.29 ms**. Recovery's
maximum was **10.65 ms**. These costs are measured, not claimed to be zero; no
whole-process CPU improvement is claimed.

Raw evidence: [complete held-out report](recovery-validation-01.json). Reproduce
with the frozen command above, using a new report filename. Wall-clock results
depend on the host scheduler and do not establish public-swarm performance.

## Regression validation

The suite has 76 tests, including deterministic recovery feedback, two-sided
change detection, isolated outliers, 10,000 jittered/varied-size observations,
2,000 adversarial scheduling decisions over 200 peers, reservation/backoff/tail
and cache bounds, duplicate/missing comparison pairs, and real TCP tests for
corruption, stalls, choking, malformed peers, resume, complementary availability,
connection caps, disk failures, cancellation, and the CLI in separate processes.

An initial Windows 3.11 CI run exposed a timing-dependent cancellation test:
50 ms was not a reliable way to ensure a piece had not yet completed. The test now
uses an explicit event after real block reception, holding that block before
verification until cancellation. It checks that no model is trained, client
sockets close, and server tasks finish. No model or benchmark parameters changed
for this test repair. CI performance assertions remain deterministic rather than
requiring fragile elapsed-time thresholds.
