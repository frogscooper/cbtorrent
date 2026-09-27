# Time-aware exploration

## Fixed candidate and evaluation gates

The previous recovery policy limits revisit bytes but can still spend too long
on peers that remain slow. The candidate retains that policy's predictor, change
detector, byte limit, tail guard, cache restriction, and revisit backoff. Only
revisit admission and outcome accounting change; initial discovery is unchanged.

At admission, estimate total service work as verified transfer seconds so far
plus the best eligible idle peer's predicted duration times unclaimed piece count.
Allow at most **2%** of this estimate for completed debits plus pending probe
reservations. A reservation is the candidate peer's predicted duration, padded
by its relative model error, minus the best idle peer's predicted duration,
clamped to zero. It is charged before another concurrent slot can select a probe.

At attempt completion, release the reservation and debit actual attempt duration
minus that earlier baseline prediction, clamped to zero. Failed hashes, timeouts,
disk errors, and cancellation all settle their ledger entry without training on
uncommitted data. A faster-than-expected probe frees time credit. Overspending
blocks subsequent probes. This is **predicted service-time admission control**,
not a 2% cap on measured download delay or a measured counterfactual. Existing
transfer deadlines remain the hard time bounds; the baseline prediction can err.

Development uses the previously examined recovery-validation cases as development
data, with seed 3301, three trials, 3 MiB, and heuristic/recovery/timed/EWMA policies.
Those cases are no longer a holdout for this work. The 2% parameter is not fitted
to future fixture rates. Deterministic tests include both affordable recovery and
expensive peers; a budget can deliberately miss an unaffordable recovery.

Before fresh validation, freeze the following acceptance criteria for exposing
the candidate in the download CLI (the heuristic remains the default):

1. Every download completes and verifies; no added measured peer bytes, wasted
   payload, or connections in any paired trial relative to recovery.
2. No scenario has a mean paired completion regression greater than 2% relative
   to recovery, and at least one stationary/late-recovery scenario improves by 1%.
3. Retain at least 80% of recovery's mean paired gain over the heuristic in the
   fresh early-recovery case (when recovery's gain is positive).
4. Median policy selection/accounting/training time is at most twice recovery's.

These are engineering gates on this finite suite, not statistical guarantees.
Report all means, descriptive bootstrap intervals, failures, maximum observed
completion times, byte counts, decision costs, and gate outcomes. Seven trials
are too few for a reliable p95/p99 claim; raw observations remain available.
If any gate fails, retain the candidate as benchmark-only research, without
silently loosening the gates or making it the default.

Fresh validation: seed **4927**, seven trials, 3 MiB, four policies, eight new
scenarios in `time-validation`. Same peer order within each trial; randomized
policy execution order. No tuning after validation begins. All traffic stays on
loopback with application-level pacing; public-swarm performance, shared network
congestion, and independent-process CPU behavior remain unmeasured.

Validation varies concurrency across 1, 2, and 3 slots, with three to five peers
and 16/32/64 KiB pieces. The top-level report concurrency is null for mixed-slot
suites; each scenario records its actual concurrency. No local unit tests or
other benchmarks run concurrently with the held-out timing experiment.

```powershell
python -m cbtorrent benchmark --suite time-validation --trials 7 --size-mib 3 --seed 4927 --policies heuristic,recovery,timed,ewma-probe --report benchmarks/my-time-validation.json
```

## Results

Development completed **96/96** verified downloads. Mean paired improvement
relative to recovery (positive is faster): late recovery +0.26%, early recovery
-0.05%, repeated changes +4.16%, stationary wide mix +2.15%, stationary equal
peers +0.79%, high latency -0.64%, corrupt fast peer +2.65%, slow outlier +0.99%.
This is exploratory evidence with only three trials per case. The subsequent
fixture/concurrency/reporting additions change the recorded source hash but do
not change the candidate's 2% parameter or its decisions. Raw development report:
[timed-development-01.json](timed-development-01.json).

Fresh validation completed **224/224** verified downloads (eight scenarios,
seven trials, four policies). All four predeclared gates passed. The candidate
is exposed as optional `--policy timed`; the default remains `heuristic`.
No model, decision parameters, or benchmark fixtures changed after validation
started. The evaluated source hash matches the policy/engine/benchmark files:
`c6030b049a1b079eb6db6f2c39d223ffc8dcc4ec6bd23446be4a012d44342d92`.

Mean paired relative completion improvement for `timed`, positive is faster.
Recovery-column intervals are descriptive paired-bootstrap 95% intervals from
2,000 resamples, without multiple-comparison correction. All baseline intervals
are retained in the [raw report](timed-validation-01.json).

| Held-out case | vs recovery (95% interval) | vs heuristic | vs EWMA with probes |
| --- | ---: | ---: | ---: |
| Stable mix | +2.39% (+1.63 to +3.19) | +0.19% | +2.74% |
| Early recovery | -0.04% (-0.51 to +0.48) | +26.73% | +16.82% |
| Late recovery | +2.65% (-0.07 to +4.86) | -0.83% | +2.87% |
| Sustained delay | -0.03% (-0.28 to +0.22) | -1.22% | -0.08% |
| Corrupt peer with alternatives | +1.63% (+0.53 to +2.74) | +0.03% | +1.81% |
| Repeated speed reversals | +2.62% (+1.74 to +3.53) | +2.49% | -5.41% |
| Three concurrent transfers | +2.43% (+1.21 to +3.48) | -1.31% | +2.58% |
| One transfer, slow alternative | +5.39% (+5.28 to +5.52) | +0.12% | +5.56% |

The candidate retained **99.91%** of recovery's mean early-recovery gain over
the heuristic. Its worst mean difference from recovery was -0.0355%, within the
predeclared 2% regression allowance. These results support the added budget over
recovery on this suite, not universal superiority: the heuristic still wins in
some cases and the simple EWMA wins by 5.41% under repeated reversals.

Every timed/recovery pair had identical measured sent/received peer bytes,
protocol overhead, wasted payload, and connection counts. Time/byte admission
limits held in all 56 timed runs and all outstanding time reservations cleared.
Timed selected **119** revisits versus recovery's **168**. Median combined
selection/training/accounting cost was **2.763 ms** versus **2.432 ms** (1.136x),
within the 2x gate; maxima were 6.144 ms and 5.518 ms. The heuristic's median was
0.450 ms. No whole-process CPU improvement is claimed.

Worst observed completion times in seconds (seven trials each, not a p95/p99):

| Case | Timed maximum | Recovery maximum |
| --- | ---: | ---: |
| Stable mix | 2.129 | 2.178 |
| Early recovery | 2.307 | 2.300 |
| Late recovery | 2.389 | 2.383 |
| Sustained delay | 3.092 | 3.090 |
| Corrupt peer with alternatives | 2.152 | 2.208 |
| Repeated speed reversals | 1.957 | 1.990 |
| Three concurrent transfers | 1.330 | 1.377 |
| One transfer, slow alternative | 3.215 | 3.397 |

Reproduce the gate decision with:

```powershell
python benchmarks/evaluate_time_budget.py benchmarks/timed-validation-01.json
```

Regression coverage includes concurrent accounting, success refunds, stale
baseline snapshots, budget exhaustion, invalid inputs, 3,000 adversarial decisions
over 200 peers with variable sizes and failed attempts, and actual TCP corruption,
stalling, choking, cancellation, disk errors, and CLI transfers. Accounting on a
failed attempt never becomes a successful training sample. Gate-checker tests
also reject missing/duplicate runs, failed downloads, added bytes, excessive
slowdowns, lost recovery gains, and excessive policy cost.
