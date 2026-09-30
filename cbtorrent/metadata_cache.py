"""Bounded, atomic cache of info dictionaries, never of discovery addresses."""
import os
import re
import stat
import tempfile
from contextlib import contextmanager
from hashlib import sha1
from pathlib import Path

from .extensions import MAX_METADATA
from .filepaths import open_payload, reject_symlinks
from .metainfo import Torrent

NAME = re.compile(r"[0-9a-f]{40}\.info\Z")


def verified_metadata(raw, digest):
    if not 1 <= len(raw) <= MAX_METADATA or sha1(raw).digest() != digest:
        raise ValueError("metadata failed info-hash verification")
    # The strict decoder accepts only canonical dictionaries. Keep the raw hash
    # check before manifest validation, and check reencoding as a second guard.
    torrent = Torrent.from_bytes(b"d4:info" + raw + b"e")
    if torrent.info_hash != digest or torrent.info_bytes != raw:
        raise ValueError("metadata info dictionary changed during validation")
    return torrent


def default_cache_path():
    return Path.home() / ".cbtorrent" / "metadata"


class MetadataCache:
    """Oldest writes evicted first; reads and directory scans have size limits.

    Methods are synchronous so callers can move disk work off their event loop.
    The process lock serializes publication and eviction across app instances.
    """
    def __init__(self, path, *, max_entries=128, max_bytes=64 * 1024 * 1024):
        if (type(max_entries) is not int or not 1 <= max_entries <= 128
                or type(max_bytes) is not int or not 1 <= max_bytes <= 64 * 1024 * 1024):
            raise ValueError("invalid metadata cache limits")
        self.path = Path(path)
        self.max_entries, self.max_bytes = max_entries, max_bytes

    def _target(self, digest):
        if not isinstance(digest, bytes) or len(digest) != 20:
            raise ValueError("cache key must be a v1 info hash")
        reject_symlinks(self.path)
        return self.path / (digest.hex() + ".info")

    def get(self, digest):
        target = self._target(digest)
        try:
            with open_payload(target) as stream:
                details = os.fstat(stream.fileno())
                if details.st_nlink != 1 or not 1 <= details.st_size <= min(MAX_METADATA, self.max_bytes):
                    raise ValueError("invalid cached metadata file")
                raw = stream.read(MAX_METADATA + 1)
        except FileNotFoundError:
            return None
        return verified_metadata(raw, digest)

    @contextmanager
    def _lock(self):
        path = self.path / ".lock"
        reject_symlinks(path)
        fd = os.open(path, os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
                     | getattr(os, "O_BINARY", 0) | getattr(os, "O_NONBLOCK", 0), 0o600)
        locked = False
        try:
            details = os.fstat(fd)
            if not stat.S_ISREG(details.st_mode) or details.st_nlink != 1:
                raise ValueError("invalid metadata cache lock")
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            locked = True
            yield
        finally:
            if locked:
                if os.name == "nt":
                    os.lseek(fd, 0, os.SEEK_SET)
                    msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
                else:
                    fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    def put(self, torrent):
        verified = verified_metadata(torrent.info_bytes, torrent.info_hash)
        if verified.private:
            raise ValueError("private metadata is not cached")
        raw = verified.info_bytes
        if len(raw) > self.max_bytes:
            raise ValueError("metadata exceeds cache capacity")
        target = self._target(verified.info_hash)
        self.path.mkdir(parents=True, exist_ok=True)
        reject_symlinks(self.path)
        with self._lock():
            entries = []
            with os.scandir(self.path) as directory:
                for count, entry in enumerate(directory, 1):
                    if count > 4096:
                        raise ValueError("metadata cache directory scan limit exceeded")
                    if not NAME.fullmatch(entry.name):
                        continue
                    # Windows DirEntry's cached find data omits the link count.
                    details = (self.path / entry.name).lstat()
                    if (not stat.S_ISREG(details.st_mode) or details.st_nlink != 1
                            or getattr(details, "st_file_attributes", 0)
                            & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)):
                        raise ValueError("cache contains a linked or special metadata file")
                    if entry.name != target.name:
                        entries.append((details.st_mtime_ns, entry.name, details.st_size))
            # Check before replacement; do not replace links even though replace
            # itself would not follow them. Unknown directory entries stay intact.
            reject_symlinks(target)
            if target.exists() and (not target.is_file() or target.stat().st_nlink != 1):
                raise ValueError("invalid cached metadata target")
            total = sum(item[2] for item in entries)
            entries.sort()
            while len(entries) >= self.max_entries or total + len(raw) > self.max_bytes:
                _, name, size = entries.pop(0)
                victim = self.path / name
                reject_symlinks(victim)
                victim.unlink()
                total -= size
            fd, name = tempfile.mkstemp(prefix=".metadata-", suffix=".tmp", dir=self.path)
            temporary = Path(name)
            try:
                with os.fdopen(fd, "wb") as stream:
                    stream.write(raw)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(temporary, target)
            finally:
                temporary.unlink(missing_ok=True)
