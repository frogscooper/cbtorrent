"""Strict, bounded bencoding. Canonical input makes info-hash reencoding safe."""


def encode(value):
    if type(value) is int:
        return b"i" + str(value).encode("ascii") + b"e"
    if isinstance(value, bytes):
        return str(len(value)).encode("ascii") + b":" + value
    if isinstance(value, list):
        return b"l" + b"".join(map(encode, value)) + b"e"
    if isinstance(value, dict) and all(isinstance(k, bytes) for k in value):
        return b"d" + b"".join(encode(k) + encode(value[k]) for k in sorted(value)) + b"e"
    raise TypeError("bencoding requires integers, bytes, lists, or byte-key dictionaries")


def decode(data: bytes, *, max_size: int = 16 * 1024 * 1024):
    if len(data) > max_size:
        raise ValueError("bencoded input exceeds size limit")
    position = 0

    def parse(depth=0):
        nonlocal position
        if depth > 64 or position >= len(data):
            raise ValueError("truncated or excessively nested bencoding")
        token = data[position:position + 1]
        position += 1
        if token == b"i":
            end = data.find(b"e", position)
            if end == -1:
                raise ValueError("unterminated integer")
            raw = data[position:end]
            digits = raw[1:] if raw.startswith(b"-") else raw
            if not digits.isdigit() or (len(digits) > 1 and digits.startswith(b"0")) or raw == b"-0":
                raise ValueError("noncanonical integer")
            position = end + 1
            return int(raw)
        if token in (b"l", b"d"):
            result = [] if token == b"l" else {}
            previous = None
            while position < len(data) and data[position:position + 1] != b"e":
                item = parse(depth + 1)
                if token == b"l":
                    result.append(item)
                else:
                    if not isinstance(item, bytes) or (previous is not None and item <= previous):
                        raise ValueError("dictionary keys must be unique, sorted bytes")
                    previous = item
                    result[item] = parse(depth + 1)
            if position >= len(data):
                raise ValueError("unterminated container")
            position += 1
            return result
        if token.isdigit():
            start = position - 1
            end = data.find(b":", start)
            raw = data[start:end] if end >= 0 else b""
            if not raw.isdigit() or (len(raw) > 1 and raw.startswith(b"0")):
                raise ValueError("invalid byte string length")
            size = int(raw)
            position = end + 1 + size
            if position > len(data):
                raise ValueError("truncated byte string")
            return data[end + 1:position]
        raise ValueError("invalid bencoding token")

    value = parse()
    if position != len(data):
        raise ValueError("trailing bencoded data")
    return value
