from dataclasses import dataclass
from hashlib import sha1
from pathlib import Path
import os

from .bencode import decode, encode
from .filepaths import component, open_payload, path_key, reject_symlinks

MAX_FILES = 10_000
MAX_LENGTH = (1 << 63) - 1


@dataclass(frozen=True)
class TorrentFile:
    path: tuple[str, ...]
    length: int
    offset: int


def manifest(entries):
    if not isinstance(entries, list) or not 1 <= len(entries) <= MAX_FILES:
        raise ValueError("torrent requires 1..10000 files")
    files, targets, directories = [], set(), set()
    offset = 0
    for entry in entries:
        if not isinstance(entry, dict):
            raise ValueError("invalid file entry")
        length, path = entry.get(b"length"), entry.get(b"path")
        if type(length) is not int or not 0 <= length <= MAX_LENGTH - offset:
            raise ValueError("invalid file length")
        if not isinstance(path, list) or not 1 <= len(path) <= 64:
            raise ValueError("file path requires 1..64 components")
        attr = entry.get(b"attr", b"")
        if not isinstance(attr, bytes):
            raise ValueError("invalid file attributes")
        if b"symlink path" in entry or b"l" in attr:
            raise ValueError("symlink entries are not supported")
        parts = tuple(component(p) for p in path)
        key = path_key(parts)
        parents = {key[:n] for n in range(1, len(key))}
        if key in targets or key in directories or parents & targets:
            raise ValueError("duplicate or conflicting torrent paths")
        targets.add(key)
        directories.update(parents)
        files.append(TorrentFile(parts, length, offset))
        offset += length
    return tuple(files), offset


@dataclass(frozen=True)
class Torrent:
    name: str
    length: int
    piece_length: int
    hashes: tuple[bytes, ...]
    info_hash: bytes
    trackers: tuple[str, ...] = ()
    files: tuple[TorrentFile, ...] = ()

    @property
    def multi_file(self):
        return bool(self.files)

    @classmethod
    def from_bytes(cls, data: bytes):
        root = decode(data)
        info = root.get(b"info") if isinstance(root, dict) else None
        if not isinstance(info, dict):
            raise ValueError("torrent requires an info dictionary")
        if b"meta version" in info:
            raise ValueError("only BitTorrent v1 is supported")
        if (b"files" in info) == (b"length" in info):
            raise ValueError("torrent requires exactly one of length or files")
        length, piece_length = info.get(b"length"), info.get(b"piece length")
        hashes, name = info.get(b"pieces"), info.get(b"name")
        files = ()
        if b"files" in info:
            files, length = manifest(info[b"files"])
        if type(length) is not int or not 0 <= length <= MAX_LENGTH:
            raise ValueError("invalid file length")
        if type(piece_length) is not int or not 1 <= piece_length <= 16 * 1024 * 1024:
            raise ValueError("piece length must be between 1 byte and 16 MiB")
        count = (length + piece_length - 1) // piece_length
        if not isinstance(hashes, bytes) or len(hashes) != count * 20:
            raise ValueError("piece hash count does not match file length")
        safe_name = component(name)
        trackers = []
        announce = root.get(b"announce")
        tiers = root.get(b"announce-list", [])
        if not isinstance(tiers, list):
            raise ValueError("invalid announce-list")
        for tier in tiers:
            if not isinstance(tier, list):
                raise ValueError("invalid tracker tier")
            for url in tier:
                if not isinstance(url, bytes):
                    raise ValueError("tracker URLs must be strings")
                trackers.append(url.decode("utf-8"))
        if announce is not None:
            if not isinstance(announce, bytes):
                raise ValueError("invalid announce URL")
            trackers.append(announce.decode("utf-8"))
        return cls(safe_name, length, piece_length,
                   tuple(hashes[i:i + 20] for i in range(0, len(hashes), 20)),
                   sha1(encode(info)).digest(), tuple(dict.fromkeys(trackers)), files)

    @classmethod
    def load(cls, path: Path):
        with path.open("rb") as stream:
            return cls.from_bytes(stream.read(16 * 1024 * 1024 + 1))

    def piece_size(self, index: int) -> int:
        if not 0 <= index < len(self.hashes):
            raise ValueError("invalid piece index")
        return min(self.piece_length, self.length - index * self.piece_length)


def create(source: Path, destination: Path, *, piece_length=256 * 1024, trackers=()):
    """Create v1 metainfo for a file or directory using a bounded piece buffer."""
    if not 1 <= piece_length <= 16 * 1024 * 1024:
        raise ValueError("piece length must be between 1 byte and 16 MiB")
    source, destination = Path(source), Path(destination)
    reject_symlinks(source)
    component(source.name.encode("utf-8"))
    entries = []
    if source.is_dir():
        directory_count = 0
        def walk_error(error):
            raise error
        for directory, dirs, names in os.walk(source, followlinks=False, onerror=walk_error):
            directory_count += 1
            if directory_count + len(dirs) > MAX_FILES:
                raise ValueError("source has too many directories")
            for name in dirs + names:
                candidate = Path(directory) / name
                reject_symlinks(candidate)
                component(name.encode("utf-8"))
            for name in names:
                candidate = Path(directory) / name
                with open_payload(candidate) as stream:
                    length = os.fstat(stream.fileno()).st_size
                entries.append({b"length": length,
                                b"path": [p.encode("utf-8") for p in candidate.relative_to(source).parts]})
                if len(entries) > MAX_FILES:
                    raise ValueError("source has too many files")
        entries.sort(key=lambda e: e[b"path"])
        files, length = manifest(entries)
        inputs = [(source.joinpath(*f.path), f.length) for f in files]
        layout = {b"files": entries}
    else:
        with open_payload(source) as stream:
            length = os.fstat(stream.fileno()).st_size
        inputs = [(source, length)]
        layout = {b"length": length}
    hashes, block = [], bytearray()
    if (length + piece_length - 1) // piece_length > (16 * 1024 * 1024) // 20:
        raise ValueError("piece hashes exceed metainfo size limit")
    for path, size in inputs:
        with open_payload(path) as stream:
            remaining = size
            while remaining:
                data = stream.read(min(remaining, piece_length - len(block)))
                if not data:
                    raise ValueError("source changed while creating torrent")
                block.extend(data)
                remaining -= len(data)
                if len(block) == piece_length:
                    hashes.append(sha1(block).digest())
                    block.clear()
            if stream.read(1):
                raise ValueError("source changed while creating torrent")
    if block:
        hashes.append(sha1(block).digest())
    info = {b"name": source.name.encode("utf-8"), **layout,
            b"piece length": piece_length, b"pieces": b"".join(hashes)}
    root = {b"info": info, b"created by": b"cbtorrent/0.2"}
    if trackers:
        root[b"announce"] = trackers[0].encode("utf-8")
        root[b"announce-list"] = [[url.encode("utf-8")] for url in trackers]
    raw = encode(root)
    torrent = Torrent.from_bytes(raw)
    with destination.open("xb") as stream:
        stream.write(raw)
    return torrent
