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
