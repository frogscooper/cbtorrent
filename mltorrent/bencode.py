"""Minimal bencode decoder/encoder (BEP 3)."""
from __future__ import annotations


def decode(data: bytes):
    val, idx = _decode(data, 0)
    return val


def _decode(data: bytes, i: int):
    if data[i : i + 1] == b"i":
        j = data.index(b"e", i)
        return int(data[i + 1 : j]), j + 1
    if data[i : i + 1] == b"l":
        out = []
        i += 1
        while data[i : i + 1] != b"e":
            v, i = _decode(data, i)
            out.append(v)
        return out, i + 1
    if data[i : i + 1] == b"d":
        out = {}
        i += 1
        while data[i : i + 1] != b"e":
            k, i = _decode(data, i)
            v, i = _decode(data, i)
            out[k] = v
        return out, i + 1
    colon = data.index(b":", i)
    n = int(data[i:colon])
    start = colon + 1
    return data[start : start + n], start + n


def encode(obj) -> bytes:
    if isinstance(obj, int) and not isinstance(obj, bool):
        return b"i" + str(obj).encode() + b"e"
    if isinstance(obj, bytes):
        return str(len(obj)).encode() + b":" + obj
    if isinstance(obj, str):
        return encode(obj.encode())
    if isinstance(obj, list):
        return b"l" + b"".join(encode(x) for x in obj) + b"e"
    if isinstance(obj, dict):
        items = sorted(((k if isinstance(k, bytes) else str(k).encode(), v) for k, v in obj.items()), key=lambda kv: kv[0])
        return b"d" + b"".join(encode(k) + encode(v) for k, v in items) + b"e"
    raise TypeError(type(obj))
