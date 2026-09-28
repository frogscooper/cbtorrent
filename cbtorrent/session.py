"""Multi-torrent queue with JSON session persistence.

Pure status and ordering; GUI binds this to DownloadController for I/O.
"""
from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path

STATUSES = frozenset({"queued", "downloading", "paused", "complete", "error"})


def default_session_path() -> Path:
    return Path.home() / ".cbtorrent" / "session.json"


@dataclass
class QueueItem:
    id: str
    torrent_path: Path
    output: Path
    policy: str
    status: str
    queue_order: float

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "path": str(self.torrent_path),
            "output": str(self.output),
            "policy": self.policy,
            "status": self.status,
            "queue_order": self.queue_order,
        }

    @classmethod
    def from_dict(cls, data: dict) -> QueueItem:
        status = str(data.get("status", "paused"))
        if status not in STATUSES:
            status = "paused"
        return cls(
            id=str(data["id"]),
            torrent_path=Path(data.get("path") or data.get("torrent_path")),
            output=Path(data["output"]),
            policy=str(data.get("policy") or "heuristic"),
            status=status,
            queue_order=float(data.get("queue_order", 0)),
        )


class Session:
    """Ordered torrent queue persisted to a JSON file."""

    def __init__(self, path: Path | None = None, *, default_policy: str = "heuristic"):
        self.path = Path(path) if path is not None else default_session_path()
        self.default_policy = default_policy or "heuristic"
        self._items: dict[str, QueueItem] = {}

    def items(self) -> list[QueueItem]:
        return sorted(self._items.values(), key=lambda item: item.queue_order)

    def get(self, item_id: str) -> QueueItem | None:
        return self._items.get(item_id)

    def downloading_id(self) -> str | None:
        for item in self.items():
            if item.status == "downloading":
                return item.id
        return None

    def load(self) -> int:
        """Load from disk. Keep at most one downloading intent (first in order)."""
        self._items.clear()
        if not self.path.exists():
            return 0
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, UnicodeDecodeError):
            return 0
        if not isinstance(raw, dict):
            return 0
        if isinstance(raw.get("default_policy"), str) and raw["default_policy"]:
            self.default_policy = raw["default_policy"]
        entries = raw.get("items") or []
        if not isinstance(entries, list):
            return 0
        loaded: list[QueueItem] = []
        for entry in entries:
            if not isinstance(entry, dict) or "id" not in entry or "output" not in entry:
                continue
            if not (entry.get("path") or entry.get("torrent_path")):
                continue
            try:
                loaded.append(QueueItem.from_dict(entry))
            except (TypeError, ValueError, KeyError):
                continue
        loaded.sort(key=lambda item: item.queue_order)
        seen_downloading = False
        for item in loaded:
            if item.status == "downloading":
                if seen_downloading:
                    item.status = "paused"
                else:
                    seen_downloading = True
            self._items[item.id] = item
        return len(self._items)

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": 1,
            "default_policy": self.default_policy,
            "items": [item.to_dict() for item in self.items()],
        }
        text = json.dumps(payload, indent=2) + "\n"
        fd, tmp_name = tempfile.mkstemp(
            dir=str(self.path.parent), prefix=".session-", suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                stream.write(text)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(tmp_name, self.path)
        except BaseException:
            try:
                os.unlink(tmp_name)
            except OSError:
                pass
            raise

    def add(self, torrent_path: Path, output: Path | None = None, *,
            policy: str | None = None, status: str = "queued") -> QueueItem:
        from .metainfo import Torrent

        torrent_path = Path(torrent_path)
        meta = Torrent.load(torrent_path)
        item_id = meta.info_hash.hex()
        if item_id in self._items:
            raise ValueError(f"torrent already in session: {item_id}")
        if status not in STATUSES:
            raise ValueError(f"invalid status: {status}")
        if output is None:
            output = Path("downloads") / meta.name
        else:
            output = Path(output)
        chosen = policy if policy is not None else self.default_policy
        order = max((item.queue_order for item in self._items.values()), default=0.0) + 1.0
        item = QueueItem(
            id=item_id,
            torrent_path=torrent_path,
            output=output,
            policy=chosen,
            status=status,
            queue_order=order,
        )
        self._items[item_id] = item
        self.save()
        return item

    def remove(self, item_id: str) -> None:
        item = self._require(item_id)
        if item.status == "downloading":
            item.status = "paused"
        del self._items[item_id]
        self.save()

    def pause(self, item_id: str) -> None:
        item = self._require(item_id)
        item.status = "paused"
        self.save()

    def resume(self, item_id: str) -> None:
        """Mark target downloading; pause any other downloading item first."""
        self._require(item_id)
        for other in self._items.values():
            if other.id != item_id and other.status == "downloading":
                other.status = "paused"
        self._items[item_id].status = "downloading"
        self.save()

    def reorder(self, item_id: str, before_id: str | None) -> None:
        """Move item_id to sit before before_id (None = end). Session order only."""
        self._require(item_id)
        if before_id is not None:
            self._require(before_id)
            if before_id == item_id:
                return
        ordered = [item for item in self.items() if item.id != item_id]
        moving = self._items[item_id]
        if before_id is None:
            ordered.append(moving)
        else:
            index = next(i for i, item in enumerate(ordered) if item.id == before_id)
            ordered.insert(index, moving)
        for n, item in enumerate(ordered, start=1):
            item.queue_order = float(n)
        self.save()

    def set_policy(self, item_id: str, name: str) -> None:
        self._require(item_id).policy = name
        self.save()

    def set_status(self, item_id: str, status: str) -> None:
        if status not in STATUSES:
            raise ValueError(f"invalid status: {status}")
        self._require(item_id).status = status
        self.save()

    def set_default_policy(self, name: str) -> None:
        self.default_policy = name
        self.save()

    def _require(self, item_id: str) -> QueueItem:
        item = self._items.get(item_id)
        if item is None:
            raise KeyError(item_id)
        return item
