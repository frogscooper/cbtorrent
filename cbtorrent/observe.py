"""Live download snapshots for UIs. No protocol logic: metrics only."""
from __future__ import annotations

from dataclasses import dataclass, replace
from time import perf_counter


@dataclass(frozen=True)
class PeerSnapshot:
    address: str
    state: str
    down_rate: float = 0.0
    verified_bytes: int = 0
    failures: int = 0


@dataclass(frozen=True)
class DownloadSnapshot:
    name: str
    length: int
    done_bytes: int
    verified_bytes: int
    resumed_bytes: int
    uploaded_bytes: int
    payload_received_bytes: int
    elapsed_seconds: float
    peer_count: int
    peers: tuple[PeerSnapshot, ...] = ()
    status: str = "running"
    error: str | None = None
    down_rate: float = 0.0
    up_rate: float = 0.0
    eta_seconds: float | None = None

    @property
    def percent(self) -> float:
        if self.length <= 0:
            return 0.0
        return min(100.0, 100.0 * self.done_bytes / self.length)


def format_bytes(n: float) -> str:
    n = max(0.0, float(n))
    for unit, scale in (("GiB", 1024 ** 3), ("MiB", 1024 ** 2), ("KiB", 1024), ("B", 1)):
        if n >= scale or unit == "B":
            value = n / scale
            return f"{value:.0f} B" if unit == "B" else f"{value:.1f} {unit}"
    return "0 B"


def format_rate(bps: float) -> str:
    if bps < 0 or not bps:
        return "0 B/s"
    return f"{format_bytes(bps)}/s"


def format_eta(seconds: float | None) -> str:
    if seconds is None or seconds < 0 or seconds != seconds:  # NaN
        return "unknown"
    if seconds == float("inf"):
        return "unknown"
    total = int(seconds + 0.5)
    if total < 60:
        return f"{total}s"
    minutes, sec = divmod(total, 60)
    if minutes < 60:
        return f"{minutes}m {sec:02d}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes:02d}m"


def format_percent(done: int, total: int) -> str:
    if total <= 0:
        return "0%"
    return f"{min(100.0, 100.0 * done / total):.1f}%"


def peer_address(address: tuple[str, int]) -> str:
    host, port = address
    if ":" in host and not host.startswith("["):
        return f"[{host}]:{port}"
    return f"{host}:{port}"


def build_snapshot(
    *,
    name: str,
    length: int,
    metrics,
    observations: dict,
    sessions: dict,
    active: dict,
    retired: set,
    status: str = "running",
    error: str | None = None,
) -> DownloadSnapshot:
    """Assemble a UI-facing snapshot from download-local state."""
    peers = []
    known = set(observations) | set(sessions) | set(active) | set(retired)
    for address in sorted(known, key=peer_address):
        observation = observations.get(address)
        if address in retired:
            state = "failed"
        elif address in active:
            state = "downloading"
        elif address in sessions:
            peer = sessions[address]
            state = "choked" if getattr(peer, "choked", False) else "connected"
        else:
            state = "idle"
        verified = observation.verified_bytes if observation else 0
        seconds = observation.seconds if observation else 0.0
        failures = observation.failures if observation else 0
        rate = verified / max(seconds, 0.001) if verified else 0.0
        peers.append(PeerSnapshot(
            address=peer_address(address),
            state=state,
            down_rate=rate,
            verified_bytes=verified,
            failures=failures,
        ))
    done = metrics.resumed_bytes + metrics.verified_bytes
    elapsed = 0.0
    if hasattr(metrics, "_started"):
        elapsed = max(0.0, perf_counter() - metrics._started)
    connected = sum(1 for p in peers if p.state in ("downloading", "connected", "choked"))
    return DownloadSnapshot(
        name=name,
        length=length,
        done_bytes=done,
        verified_bytes=metrics.verified_bytes,
        resumed_bytes=metrics.resumed_bytes,
        uploaded_bytes=metrics.uploaded_bytes,
        payload_received_bytes=metrics.payload_received_bytes,
        elapsed_seconds=elapsed,
        peer_count=connected,
        peers=tuple(peers),
        status=status,
        error=error,
    )


class RateTracker:
    """Derive down/up rates and ETA from successive snapshots."""

    def __init__(self):
        self._prev: DownloadSnapshot | None = None

    def update(self, snapshot: DownloadSnapshot) -> DownloadSnapshot:
        down_rate = up_rate = 0.0
        eta = None
        prev = self._prev
        if prev is not None:
            dt = snapshot.elapsed_seconds - prev.elapsed_seconds
            if dt > 0:
                down_rate = max(0.0, (snapshot.payload_received_bytes - prev.payload_received_bytes) / dt)
                up_rate = max(0.0, (snapshot.uploaded_bytes - prev.uploaded_bytes) / dt)
        # Prefer lifetime average when the sample window is too short / empty.
        if down_rate <= 0 and snapshot.elapsed_seconds > 0 and snapshot.verified_bytes:
            down_rate = snapshot.verified_bytes / snapshot.elapsed_seconds
        remaining = max(0, snapshot.length - snapshot.done_bytes)
        if snapshot.status == "complete":
            eta = 0.0
            down_rate = down_rate  # keep last sample
        elif down_rate > 0 and remaining:
            eta = remaining / down_rate
        elif remaining == 0 and snapshot.length:
            eta = 0.0
        self._prev = snapshot
        return replace(snapshot, down_rate=down_rate, up_rate=up_rate, eta_seconds=eta)
