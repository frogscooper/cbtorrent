from __future__ import annotations

import socket
import time

PSTR = b"BitTorrent protocol"


def handshake(sock: socket.socket, info_hash: bytes, peer_id: bytes, timeout: float = 8.0) -> bytes:
    sock.settimeout(timeout)
    msg = bytes([len(PSTR)]) + PSTR + b"\x00" * 8 + info_hash + peer_id
    sock.sendall(msg)
    header = _recv_exact(sock, 68)
    if header[0] != 19 or header[1:20] != PSTR:
        raise ConnectionError("bad handshake pstr")
    if header[28:48] != info_hash:
        raise ConnectionError("info_hash mismatch")
    return header[48:68]


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("eof")
        buf += chunk
    return buf


def connect_peer(ip: str, port: int, info_hash: bytes, peer_id: bytes, timeout: float = 6.0):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    t0 = time.perf_counter()
    sock.connect((ip, port))
    rtt = (time.perf_counter() - t0) * 1000
    remote = handshake(sock, info_hash, peer_id, timeout=timeout)
    return sock, remote, rtt
