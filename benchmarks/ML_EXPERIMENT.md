# Adaptive service-time prediction

Status: development complete; separate validation pending. The default remains
the original throughput heuristic until the PR is reviewed.

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
