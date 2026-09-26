"""Online linear ranker for BitTorrent peers."""
from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from pathlib import Path


def ip_prefix_hash(ip: str) -> float:
    parts = ip.split(".")
    if len(parts) != 4:
        return 0.0
    key = int(parts[0]) * 256 + int(parts[1])
    return ((key * 2654435761) & 0xFFFFFFFF) / 0xFFFFFFFF


def features(ip: str, port: int, hist_rate: float, fail_rate: float, overlap: float, rtt_ms: float) -> list[float]:
    return [
        1.0,
        min(port, 65535) / 65535.0,
        math.tanh(hist_rate / 50.0),
        min(fail_rate, 1.0),
        min(max(overlap, 0.0), 1.0),
        math.tanh(rtt_ms / 400.0),
        ip_prefix_hash(ip),
    ]


@dataclass
class PeerRanker:
    weights: list[float] = field(default_factory=lambda: [0.0, 0.1, 1.2, -1.5, 0.8, -0.7, 0.05])
    lr: float = 0.05
    history: dict = field(default_factory=dict)

    def prefix(self, ip: str) -> str:
        parts = ip.split(".")
        return ".".join(parts[:2]) if len(parts) == 4 else ip

    def stats(self, ip: str) -> tuple[float, float]:
        h = self.history.get(self.prefix(ip), {"ok": 0, "fail": 0, "rate": 0.0})
        n = h["ok"] + h["fail"]
        fail = h["fail"] / n if n else 0.2
        return h["rate"], fail

    def score(self, ip: str, port: int, overlap: float = 0.5, rtt_ms: float = 150.0) -> float:
        rate, fail = self.stats(ip)
        x = features(ip, port, rate, fail, overlap, rtt_ms)
        return sum(w * xi for w, xi in zip(self.weights, x))

    def rank(self, peers: list[tuple[str, int]], overlap_fn=None) -> list[tuple[str, int]]:
        def ov(ip):
            return overlap_fn(ip) if overlap_fn else 0.5
        return sorted(peers, key=lambda p: self.score(p[0], p[1], ov(p[0])), reverse=True)

    def observe_success(self, ip: str, pieces_per_sec: float, rtt_ms: float, port: int, overlap: float = 0.5):
        self._update(ip, port, overlap, rtt_ms, label=+1, rate=pieces_per_sec)
        h = self.history.setdefault(self.prefix(ip), {"ok": 0, "fail": 0, "rate": 0.0})
        h["ok"] += 1
        h["rate"] = 0.7 * h["rate"] + 0.3 * pieces_per_sec

    def observe_failure(self, ip: str, port: int, rtt_ms: float = 999.0):
        self._update(ip, port, 0.0, rtt_ms, label=-1, rate=0.0)
        h = self.history.setdefault(self.prefix(ip), {"ok": 0, "fail": 0, "rate": 0.0})
        h["fail"] += 1

    def _update(self, ip, port, overlap, rtt_ms, label: int, rate: float):
        fail = self.stats(ip)[1]
        x = features(ip, port, rate, fail, overlap, rtt_ms)
        yhat = sum(w * xi for w, xi in zip(self.weights, x))
        if label * yhat <= 0.5:
            for i in range(len(self.weights)):
                self.weights[i] += self.lr * label * x[i]

    def save(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"weights": self.weights, "history": self.history}, indent=2))

    @classmethod
    def load(cls, path: Path) -> "PeerRanker":
        if not path.exists():
            return cls()
        data = json.loads(path.read_text())
        return cls(weights=data["weights"], history=data.get("history", {}))
