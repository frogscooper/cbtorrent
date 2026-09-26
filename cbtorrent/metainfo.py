from dataclasses import dataclass
from hashlib import sha1
from pathlib import Path

from .bencode import decode, encode


@dataclass(frozen=True)
class Torrent:
    name: str
    length: int
    piece_length: int
    hashes: tuple[bytes, ...]
    info_hash: bytes
    trackers: tuple[str, ...] = ()

    @classmethod
    def from_bytes(cls, data: bytes):
        root = decode(data)
        info = root.get(b"info") if isinstance(root, dict) else None
        if not isinstance(info, dict):
            raise ValueError("torrent requires an info dictionary")
        if b"files" in info or b"meta version" in info:
            raise ValueError("this milestone supports single-file BitTorrent v1 only")
        length, piece_length = info.get(b"length"), info.get(b"piece length")
        hashes, name = info.get(b"pieces"), info.get(b"name")
        if type(length) is not int or length < 0:
            raise ValueError("invalid file length")
        if type(piece_length) is not int or not 1 <= piece_length <= 16 * 1024 * 1024:
            raise ValueError("piece length must be between 1 byte and 16 MiB")
        count = (length + piece_length - 1) // piece_length
        if not isinstance(hashes, bytes) or len(hashes) != count * 20:
            raise ValueError("piece hash count does not match file length")
        if not isinstance(name, bytes) or not name:
            raise ValueError("torrent requires a name")
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
        return cls(name.decode("utf-8"), length, piece_length,
                   tuple(hashes[i:i + 20] for i in range(0, len(hashes), 20)),
                   sha1(encode(info)).digest(), tuple(dict.fromkeys(trackers)))

    @classmethod
    def load(cls, path: Path):
        with path.open("rb") as stream:
            return cls.from_bytes(stream.read(16 * 1024 * 1024 + 1))

    def piece_size(self, index: int) -> int:
        if not 0 <= index < len(self.hashes):
            raise ValueError("invalid piece index")
        return min(self.piece_length, self.length - index * self.piece_length)


def create(source: Path, destination: Path, *, piece_length=256 * 1024, trackers=()):
    """Create single-file v1 metainfo by streaming the source, without overwriting."""
    if not 1 <= piece_length <= 16 * 1024 * 1024:
        raise ValueError("piece length must be between 1 byte and 16 MiB")
    source, destination = Path(source), Path(destination)
    hashes = []
    length = 0
    with source.open("rb") as stream:
        while block := stream.read(piece_length):
            length += len(block)
            hashes.append(sha1(block).digest())
    info = {b"name": source.name.encode("utf-8"), b"length": length,
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
