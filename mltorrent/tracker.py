from __future__ import annotations

import socket
import struct
import urllib.parse
import urllib.request
from .bencode import decode
from .torrent import Torrent


def compact_peers(blob: bytes) -> list[tuple[str, int]]:
    peers = []
    for i in range(0, len(blob), 6):
        chunk = blob[i : i + 6]
        if len(chunk) < 6:
            break
        ip = socket.inet_ntoa(chunk[:4])
        port = struct.unpack("!H", chunk[4:])[0]
        peers.append((ip, port))
    return peers


def announce(torrent: Torrent, peer_id: bytes, port: int = 6881, uploaded=0, downloaded=0, left=None, event="started") -> list[tuple[str, int]]:
    if left is None:
        left = torrent.length
    found: list[tuple[str, int]] = []
    for url in torrent.announce_list:
        if not url.startswith("http"):
            continue
        q = (
            f"info_hash={urllib.parse.quote_from_bytes(torrent.info_hash)}"
            f"&peer_id={urllib.parse.quote_from_bytes(peer_id)}"
            f"&port={port}&uploaded={uploaded}&downloaded={downloaded}"
            f"&left={left}&compact=1&event={event}&numwant=80"
        )
        sep = "&" if "?" in url else "?"
        try:
            with urllib.request.urlopen(url + sep + q, timeout=12) as resp:
                body = resp.read()
            data = decode(body)
            peers = data.get(b"peers", b"")
            if isinstance(peers, bytes):
                found.extend(compact_peers(peers))
            elif isinstance(peers, list):
                for p in peers:
                    found.append((p[b"ip"].decode(), p[b"port"]))
        except Exception as exc:
            print(f"[tracker] {url} failed: {exc}")
            continue
        if found:
            break
    return list(dict.fromkeys(found))
