from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from .bencode import decode, encode


@dataclass
class Torrent:
    announce: str
    announce_list: list[str]
    info_hash: bytes
    piece_length: int
    pieces: bytes
    name: str
    length: int
    files: list[tuple[str, int]]

    @property
    def num_pieces(self) -> int:
        return len(self.pieces) // 20


def load_torrent(path: str | Path) -> Torrent:
    meta = decode(Path(path).read_bytes())
    info = meta[b"info"]
    info_hash = hashlib.sha1(encode(info)).digest()
    announce = meta[b"announce"].decode()
    alist = []
    if b"announce-list" in meta:
        for tier in meta[b"announce-list"]:
            for url in tier:
                alist.append(url.decode())
    if announce not in alist:
        alist.insert(0, announce)
    name = info[b"name"].decode(errors="replace")
    piece_length = info[b"piece length"]
    pieces = info[b"pieces"]
    files: list[tuple[str, int]] = []
    if b"files" in info:
        total = 0
        for f in info[b"files"]:
            rel = "/".join(p.decode(errors="replace") for p in f[b"path"])
            files.append((rel, f[b"length"]))
            total += f[b"length"]
        length = total
    else:
        length = info[b"length"]
        files = [(name, length)]
    return Torrent(announce, alist, info_hash, piece_length, pieces, name, length, files)
