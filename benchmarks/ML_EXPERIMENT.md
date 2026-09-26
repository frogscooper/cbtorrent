# Adaptive service-time prediction

Status: development and separate validation complete. The candidate improves
completion time in two held-out rate-change scenarios; it is essentially tied
elsewhere and slightly slower in the recovering-peer scenario. The original
throughput heuristic remains the default.

## Hypothesis and fixed design

The old UCB policy spends too many requests exploring slow peers, and its bounded
reward is only indirectly related to completion time. Lifetime throughput also
adapts slowly after a previously fast peer slows down.

The candidate predicts transfer duration with online exponentially weighted
least squares: `seconds = cost * (bytes / 16384)`. Each verified piece updates
three scalar sufficient statistics with forgetting factor **0.8**. Connection
setup is excluded from these transfer observations. Smaller predicted service
time wins; peers are explored once while enough unclaimed work remains.

An optional tail planner compares an idle peer with an already-active peer that
has all missing pieces. It defers only when a conservative predicted finish on
the active peer is at least 10% sooner than an optimistic prediction on the idle
one. Deferral is rechecked every 50 ms and stops if progress becomes stale. It
does not issue duplicate requests. Missing-piece context is bounded to the tail.

This is a small online regression model, not a neural network, a pretrained
model, or a claim of contextual-bandit regret bounds. It is inspired by standard
[exponentially weighted least squares](https://ethz.ch/content/dam/ethz/special-interest/mavt/dynamic-systems-n-control/idsc-dam/Lectures/System-Modeling/Slides_HS17/Lecture10.pdf).

## Evaluation plan (fixed before validation)

- Development: seed 313, 3 paired trials per scenario, 1 MiB files, 5 scenarios.
- Validation: seed 941, 7 paired trials per scenario, 1.5 MiB files, 8 distinct
  scenarios with 3–4 peers, 16/32/64 KiB pieces, different pacing/delays, speed
  changes, and a corrupt peer.
- Compare original heuristic, adaptive, adaptive without deferral, and the same
  planner using lifetime throughput instead of the learned recent cost model.
- Use identical peer order and rate-change schedule within each pair; randomize
  policy execution order. No scenario rates or future changes enter the policy.
- Report every scenario, successful/failed trials, byte overhead, wasted payload,
  CPU, selection/training time, and paired completion-time differences.
- Descriptive paired-bootstrap 95% intervals use 2,000 resamples. They are not
  corrected for multiple comparisons; do not turn exploratory intervals into a
  universal performance claim.
- Do not tune parameters after reading validation. Any subsequent policy change
  requires a new validation design/seed and an explicit history of what changed.

The report records a SHA-256 of the relevant source files. The development run
preceded a context-allocation optimization (no model/decision parameter changes).
All runs use localhost application-level pacing and the same process for seeders
and client; real TCP congestion and public-swarm generalization remain untested.

## Validation results

All **224/224 downloads** completed and verified: 8 scenarios × 7 trials × 4
policies. Parameters were not changed after validation started. The recorded
source hash matches the tested policy/engine/benchmark files:
`0918d3335f7987f2c4a942fdac54a44209d03ae4e17dc51405c6444f42725b2c`.

Positive values below mean shorter completion time than the original heuristic.
The statistic is the mean of paired relative improvements, not a ratio of medians.

| Held-out scenario | Adaptive improvement | Descriptive 95% interval |
| --- | ---: | ---: |
| Wide speed mix | +0.14% | −0.02% to +0.28% |
| Narrow speed mix | +0.10% | −0.46% to +0.63% |
| Equal peers | +0.09% | −0.15% to +0.36% |
| Delayed rate swap | **+9.43%** | +8.15% to +10.64% |
| Recovering peer | **−0.33%** | −0.63% to −0.07% |
| Two peers slow down | **+7.84%** | +5.01% to +10.06% |
| Corrupt fast peer | +0.48% | −0.18% to +1.64% |
| High per-block delay | −0.07% | −0.27% to +0.12% |

Peer bytes sent/received, protocol overhead, wasted payload, and connection counts
were **identical in every adaptive/heuristic paired trial**. Median policy selection plus
training time was **1.04 ms per download**, versus **0.26 ms** for the heuristic;
the adaptive maximum was 1.82 ms. This adds small decision overhead, not zero
overhead. Whole-process CPU was noisy/coarse and included seeders, so no CPU
efficiency improvement is claimed.

## What caused the improvement?

| Variant | Delayed rate swap | Two peers slow down |
| --- | ---: | ---: |
| Adaptive | +9.43% | +7.84% |
| Adaptive without tail deferral | +9.47% | +8.00% |
| Tail planner with lifetime-throughput estimates | +0.94% | +0.51% |

The useful change is **recent service-time learning**, not the tail planner.
Tail deferral occurred only once across the 56 adaptive validation downloads, so
these experiments do not establish a benefit from that additional mechanism.
The raw reports keep both ablations visible rather than attributing every gain
to the full policy.

The regression is intentionally simple: with equal-size pieces it behaves much
like an exponentially weighted duration estimate. No evidence here establishes
that a more complex ML model would be better. The remaining exploration weakness
is visible in the recovering-peer case: a previously slow, unused peer can become
fast without being sampled again soon enough. This is useful follow-up work,
not a reason to claim broad swarm superiority from the current result.

Reproduce validation:

```powershell
python -m cbtorrent benchmark --suite validation --trials 7 --size-mib 1.5 --seed 941 --policies heuristic,adaptive,adaptive-no-defer,planned-heuristic --report benchmarks/my-validation.json
```

Raw evidence: [development](ml-development-01.json) and
[validation](ml-validation-01.json). Runtime timing is not deterministic even
though the fixture configuration, data seed, pairing, and ordering are recorded.
