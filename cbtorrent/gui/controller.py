"""Background asyncio download driven from a UI thread."""
from __future__ import annotations

import asyncio
import threading
from dataclasses import replace
from pathlib import Path

from ..client import DownloadError, download
from ..metainfo import Torrent
from ..magnet import Magnet, download_magnet
from ..observe import DownloadSnapshot, RateTracker
from ..policy import ThroughputPolicy


class DownloadController:
    """Runs ``download()`` on a private event loop; UI polls ``snapshot``."""

    def __init__(self):
        self._lock = threading.Lock()
        self._cancel_requested = threading.Event()
        self._rates = RateTracker()
        self._snapshot: DownloadSnapshot | None = None
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._task: asyncio.Task | None = None
        self._error: str | None = None
        self.output: Path | None = None
        self.active_id: str | None = None
        self.resolved_torrent: Torrent | None = None

    @property
    def snapshot(self) -> DownloadSnapshot | None:
        with self._lock:
            return self._snapshot

    @property
    def busy(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def _set_snapshot(self, snapshot: DownloadSnapshot):
        with self._lock:
            self._snapshot = self._rates.update(snapshot)

    def _observe(self, snapshot: DownloadSnapshot):
        self._set_snapshot(snapshot)

    def start(self, torrent: Torrent | Magnet, peers, output: Path, *, resume=False,
              use_trackers=True, listen_host="0.0.0.0", listen_port=0,
              timeout=15.0, piece_timeout=120.0, pipeline=8, concurrency=4,
              max_connections=16, policy=None, item_id=None, use_dht=False, dht_bootstrap=None, metadata_timeout=60.0):
        if self.busy:
            raise RuntimeError("download already running")
        self._cancel_requested.clear()
        self.output = Path(output)
        self.active_id = item_id
        self.resolved_torrent = None if isinstance(torrent, Magnet) else torrent
        self._error = None
        self._rates = RateTracker()
        self._set_snapshot(DownloadSnapshot(
            name=torrent.name, length=torrent.length, done_bytes=0,
            verified_bytes=0, resumed_bytes=0, uploaded_bytes=0,
            payload_received_bytes=0, elapsed_seconds=0.0, peer_count=0,
            status="metadata" if isinstance(torrent, Magnet) else "starting"))
        policy = policy or ThroughputPolicy()
        peers = list(peers)

        def runner():
            loop = asyncio.new_event_loop()
            self._loop = loop
            asyncio.set_event_loop(loop)
            try:
                def resolved(meta):
                    self.resolved_torrent = meta
                runner = download_magnet if isinstance(torrent, Magnet) else download
                extra = {"metadata_timeout": metadata_timeout, "on_metadata": resolved} if isinstance(torrent, Magnet) else {}
                self._task = loop.create_task(runner(
                    torrent, peers, self.output, timeout=timeout,
                    piece_timeout=piece_timeout, pipeline=pipeline,
                    concurrency=concurrency, max_connections=max_connections,
                    resume=resume, policy=policy, use_trackers=use_trackers,
                    use_dht=use_dht, dht_bootstrap=dht_bootstrap,
                    listen_host=listen_host, listen_port=listen_port,
                    observe=self._observe, **extra))
                if self._cancel_requested.is_set():
                    self._task.cancel()
                loop.run_until_complete(self._task)
            except asyncio.CancelledError:
                current = self.snapshot
                if current is not None and current.status not in ("complete", "cancelled", "error"):
                    self._set_snapshot(replace(current, status="cancelled"))
            except DownloadError as error:
                self._error = str(error)
                current = self.snapshot
                if current is None or current.status in ("starting", "metadata", "running"):
                    self._set_snapshot(build_error_snapshot(self.resolved_torrent or torrent, error))
            except Exception as error:  # noqa: BLE001 - surface to UI
                self._error = str(error)
                current = self.snapshot
                if current is None:
                    self._set_snapshot(DownloadSnapshot(
                        name=torrent.name, length=torrent.length, done_bytes=0,
                        verified_bytes=0, resumed_bytes=0, uploaded_bytes=0,
                        payload_received_bytes=0, elapsed_seconds=0.0, peer_count=0,
                        status="error", error=str(error)))
                else:
                    self._set_snapshot(replace(current, status="error", error=str(error)))
            finally:
                loop.close()
                self._loop = None
                self._task = None

        self._thread = threading.Thread(target=runner, name="cbtorrent-download", daemon=True)
        self._thread.start()

    def cancel(self):
        self._cancel_requested.set()
        loop, task = self._loop, self._task
        if loop is not None and task is not None and not task.done():
            try:
                loop.call_soon_threadsafe(task.cancel)
            except RuntimeError:
                # The worker can finish and close its loop between these checks.
                pass

    def join(self, timeout=None):
        if self._thread is not None:
            self._thread.join(timeout)


def build_error_snapshot(torrent: Torrent, error: DownloadError) -> DownloadSnapshot:
    report = error.report or {}
    done = report.get("resumed_bytes", 0) + report.get("verified_bytes", 0)
    return DownloadSnapshot(
        name=torrent.name,
        length=torrent.length,
        done_bytes=done,
        verified_bytes=report.get("verified_bytes", 0),
        resumed_bytes=report.get("resumed_bytes", 0),
        uploaded_bytes=report.get("uploaded_bytes", 0),
        payload_received_bytes=report.get("payload_received_bytes", 0),
        elapsed_seconds=report.get("elapsed_seconds", 0.0) or 0.0,
        peer_count=0,
        status="error",
        error=str(error),
    )
