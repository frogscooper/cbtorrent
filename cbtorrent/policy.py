"""The policy boundary takes observations only, never future peer performance."""
from dataclasses import dataclass
from typing import Protocol, Sequence
import math


@dataclass
class Observation:
    verified_bytes: int = 0
    seconds: float = 0.0
    failures: int = 0
    samples: int = 0
    reward_sum: float = 0.0
    wire_bytes: int = 0

    def record(self, verified_bytes, seconds, wire_bytes, failed=False):
        self.verified_bytes += verified_bytes
        self.seconds += seconds
        self.wire_bytes += wire_bytes
        self.failures += int(failed)
        self.samples += 1
        speed = verified_bytes / max(seconds, 0.001)
        # Bounded goodput reward with an explicit penalty for non-useful bytes.
        efficiency = verified_bytes / max(wire_bytes, 1)
        self.reward_sum += max(0.0, speed / (speed + 1024 * 1024) - 0.2 * (1 - efficiency))


class PeerPolicy(Protocol):
    def choose(self, peers: Sequence[tuple[str, int]], observations: dict) -> tuple[str, int]: ...


class ThroughputPolicy:
    """Explore each peer once, then use observed verified throughput."""

    def choose(self, peers, observations):
        unseen = [peer for peer in peers if peer not in observations]
        if unseen:
            return unseen[0]
        return max(peers, key=lambda peer: observations[peer].verified_bytes /
                   max(observations[peer].seconds, 0.001) /
                   (1 + observations[peer].failures))


class BanditPolicy:
    """UCB1-style online learning with a bounded throughput/overhead reward.

    This is a non-contextual multi-armed bandit, not a pretrained predictor.
    State is per download; unseen peers receive initial exploration.
    """

    def __init__(self, exploration=0.25):
        if not math.isfinite(exploration) or exploration < 0:
            raise ValueError("exploration must be finite and nonnegative")
        self.exploration = exploration

    def choose(self, peers, observations):
        for peer in peers:
            if peer not in observations or observations[peer].samples == 0:
                return peer
        total = max(2, sum(item.samples for item in observations.values()))
        return max(peers, key=lambda peer: observations[peer].reward_sum / observations[peer].samples
                   + self.exploration * math.sqrt(2 * math.log(total) / observations[peer].samples))


@dataclass
class ActiveTransfer:
    index: int
    size: int
    received: int
    last_progress: float


@dataclass(frozen=True)
class SchedulingContext:
    now: float
    unclaimed_count: int
    missing: dict[int, int]
    available: dict[tuple[str, int], frozenset[int]]
    active: dict[tuple[str, int], ActiveTransfer]
    concurrency: int
    piece_size: int = 16384


@dataclass
class ServiceModel:
    """Exponentially weighted least squares: seconds = cost * 16-KiB units.

    Fit only verified transfer durations, excluding connection setup. The small
    model has constant memory and does not need a numerical dependency.
    """
    forgetting: float = 0.8
    xx: float = 0.0
    xy: float = 0.0
    yy: float = 0.0
    mass: float = 0.0
    samples: int = 0

    def update(self, size, seconds):
        if size <= 0 or not math.isfinite(seconds) or seconds <= 0:
            raise ValueError("training requires positive bytes and finite positive time")
        x = size / 16384
        decay = self.forgetting
        self.xx = decay * self.xx + x * x
        self.xy = decay * self.xy + x * seconds
        self.yy = decay * self.yy + seconds * seconds
        self.mass = decay * self.mass + 1
        self.samples += 1

    def predict(self, size):
        if not self.samples:
            return None
        return max(0.0001, self.xy / self.xx * size / 16384)

    def relative_error(self):
        if self.samples < 2:
            return 0.25
        residual = max(0.0, self.yy - self.xy * self.xy / self.xx)
        return min(1.0, math.sqrt(residual / max(self.yy, 1e-12)))


class AdaptivePolicy:
    """Recent learned service costs, bounded exploration, and tail deferral.

    The regression is an online prediction model, not a contextual-bandit regret
    guarantee. The planner uses only observed availability and in-flight progress.
    """

    def __init__(self, *, forgetting=0.8, defer=True):
        if not math.isfinite(forgetting) or not 0 < forgetting <= 1:
            raise ValueError("forgetting must be in (0, 1]")
        self.forgetting = forgetting
        self.defer = defer
        self.models = {}
        self.fallback = ThroughputPolicy()

    def observe(self, peer, size, seconds):
        model = self.models.setdefault(peer, ServiceModel(self.forgetting))
        model.update(size, seconds)

    def choose(self, peers, observations):
        unseen = [peer for peer in peers if peer not in self.models]
        if unseen:
            return unseen[0]
        return min(peers, key=lambda p: self.models[p].predict(16384))

    def choose_with_context(self, peers, observations, context):
        known = [p for p in peers if p in self.models]
        unseen = [p for p in peers if p not in self.models]
        # Explore once while enough work remains to amortize discovering a peer.
        # No late UCB optimism sends a tail piece back to a proven slow peer.
        if unseen and (not known or context.unclaimed_count > 2 * context.concurrency):
            return unseen[0]
        if not known:
            return self.fallback.choose(peers, observations)
        peer = min(known, key=lambda p: self.models[p].predict(16384))
        if not self.defer or not context.active or context.unclaimed_count > context.concurrency:
            return peer
        useful = context.available.get(peer, frozenset()) & context.missing.keys()
        if not useful:
            return peer
        model = self.models[peer]
        # Compare a conservative busy-peer prediction to an optimistic idle one.
        idle_time = min(model.predict(context.missing[i]) for i in useful)
        idle_time *= max(0.5, 1 - model.relative_error())
        for other, active in context.active.items():
            busy_model = self.models.get(other)
            if busy_model is None or busy_model.samples < 2:
                continue
            if not context.missing.keys() <= context.available.get(other, frozenset()):
                continue
            if context.now - active.last_progress > max(0.1, 2 * busy_model.predict(16384)):
                continue  # A silent/stalled peer must not hold up fallback work.
            remaining_bytes = max(0, active.size - active.received) + sum(context.missing.values())
            busy_time = busy_model.predict(remaining_bytes) * (1 + busy_model.relative_error())
            if busy_time < 0.9 * idle_time:
                return None
        return peer


class PlannedHeuristic(AdaptivePolicy):
    """Evaluation ablation: the same planner with lifetime throughput estimates."""

    def choose_with_context(self, peers, observations, context):
        for peer, observation in observations.items():
            if observation.verified_bytes:
                model = ServiceModel()
                model.update(observation.verified_bytes, observation.seconds)
                model.samples = observation.samples
                self.models[peer] = model
        return super().choose_with_context(peers, observations, context)

    def observe(self, peer, size, seconds):
        pass


@dataclass
class ResponsiveModel(ServiceModel):
    """Reset stale history only after two consistent, large prediction errors.

    A single outlier still receives the ordinary exponentially weighted update.
    The second sample is compared to the frozen pre-change prediction, avoiding
    a moving threshold. No timing or peer identity from a fixture is used.
    """
    pending: tuple | None = None
    resets: int = 0

    def update(self, size, seconds):
        if size <= 0 or not math.isfinite(seconds) or seconds <= 0:
            raise ValueError("training requires positive bytes and finite positive time")
        cost = seconds / (size / 16384)
        reference = self.pending[0] if self.pending else self.predict(16384)
        direction = 0 if reference is None else (1 if cost > 2 * reference else
                                                 -1 if cost < reference / 2 else 0)
        if self.pending and direction == self.pending[1] and direction:
            _, _, old_size, old_seconds = self.pending
            self.xx = self.xy = self.yy = self.mass = 0.0
            self.samples -= 1  # Reinsert the first change sample, once.
            super().update(old_size, old_seconds)
            self.pending = None
            self.resets += 1
        else:
            self.pending = (reference, direction, size, seconds) if direction else None
        super().update(size, seconds)


@dataclass
class EWMAModel:
    """Stronger simple baseline: an EWMA of seconds per 16 KiB."""
    forgetting: float = 0.8
    cost: float | None = None
    samples: int = 0

    def update(self, size, seconds):
        if size <= 0 or not math.isfinite(seconds) or seconds <= 0:
            raise ValueError("training requires positive bytes and finite positive time")
        cost = seconds / (size / 16384)
        self.cost = cost if self.cost is None else self.forgetting * self.cost + (1 - self.forgetting) * cost
        self.samples += 1

    def predict(self, size):
        return None if self.cost is None else max(0.0001, self.cost * size / 16384)

    def relative_error(self):
        return 0.25


class RecoveryPolicy(AdaptivePolicy):
    """Revisit stale cached peers with a verified-byte exploration budget.

    At most 1/16 of bytes already verified may be committed to revisits. They
    fetch useful, unclaimed pieces, never duplicates. This is a byte allocation
    bound, not a guarantee on elapsed time: a probe may still stall until the
    engine's existing deadlines. Initial discovery is outside this budget.
    """

    def __init__(self, *, model_factory=ResponsiveModel, probe=True):
        super().__init__(defer=False)
        self.model_factory = model_factory
        self.probe = probe
        self.epoch = 0
        self.last_seen = {}
        self.verified_bytes = 0
        self.probe_bytes = 0
        self.probes = 0
        self.probe_intervals = {}
        self.pending_probes = {}

    def diagnostics(self):
        return dict(probes=self.probes, reserved_probe_bytes=self.probe_bytes,
                    training_bytes=self.verified_bytes,
                    model_resets=sum(getattr(m, "resets", 0) for m in self.models.values()))

    def observe(self, peer, size, seconds):
        model = self.models.setdefault(peer, self.model_factory(self.forgetting))
        model.update(size, seconds)
        expected = self.pending_probes.pop(peer, None)
        if expected is not None:
            cost = seconds / (size / 16384)
            interval = self.probe_intervals.get(peer, 16)
            # Unchanged peers need fewer revisits. A material change restores
            # the short interval so a second observation can confirm recovery.
            self.probe_intervals[peer] = min(256, interval * 2) if expected / 2 <= cost <= 2 * expected else 16
        self.epoch += 1
        self.last_seen[peer] = self.epoch
        self.verified_bytes += size

    def choose_with_context(self, peers, observations, context):
        best = super().choose_with_context(peers, observations, context)
        if (not self.probe or best not in self.models
                or context.unclaimed_count <= 4 * context.concurrency
                or self.probe_bytes + context.piece_size > self.verified_bytes / 16):
            return best
        best_time = self.models[best].predict(context.piece_size)
        # Avoid an expensive known slow probe when too little work remains to
        # amortize it. This is a prediction gate, not a runtime time budget.
        horizon = best_time * context.unclaimed_count / context.concurrency
        stale = [p for p in peers if p != best and p in self.models
                 and p in context.available  # Only cached sessions; no reconnection cost.
                 and self.epoch - self.last_seen.get(p, self.epoch) >= self.probe_intervals.get(p, 16)
                 and self.models[p].predict(context.piece_size) <= 0.2 * horizon]
        if not stale:
            return best
        peer = min(stale, key=lambda p: self.last_seen[p])
        self.last_seen[peer] = self.epoch  # Reserve before another concurrent decision.
        self.pending_probes[peer] = self.models[peer].predict(16384)
        self.probe_bytes += context.piece_size
        self.probes += 1
        return peer
