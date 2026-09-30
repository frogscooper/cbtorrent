"""BEP 10 negotiation and bounded BEP 9 metadata serving."""
import struct

from .bencode import decode, decode_prefix, encode

RESERVED = b"\x00\x00\x00\x00\x00\x10\x00\x00"
BLOCK = 16 * 1024
MAX_METADATA = 8 * 1024 * 1024
MAX_EXTENDED = BLOCK + 1024
METADATA_ID = 1  # Our incoming ID; the remote peer chooses its own incoming ID.


def extended(identifier, payload):
    body = bytes((20, identifier)) + payload
    return struct.pack("!I", len(body)) + body


def handshake(info=b""):
    fields = {b"m": {b"ut_metadata": METADATA_ID}, b"reqq": 4}
    if info:
        fields[b"metadata_size"] = len(info)
    return extended(0, encode(fields))


def negotiation(payload):
    fields = decode(payload, max_size=MAX_EXTENDED)
    if not isinstance(fields, dict) or not isinstance(fields.get(b"m", {}), dict):
        raise ValueError("invalid extension handshake")
    identifier = fields.get(b"m", {}).get(b"ut_metadata")
    if identifier is not None and (type(identifier) is not int or not 0 <= identifier <= 255):
        raise ValueError("invalid ut_metadata extension ID")
    size = fields.get(b"metadata_size")
    if size is not None and (type(size) is not int or not 1 <= size <= MAX_METADATA):
        raise ValueError("metadata size exceeds limit")
    return identifier, size


def metadata_message(payload):
    fields, end = decode_prefix(payload, max_size=MAX_EXTENDED)
    if not isinstance(fields, dict) or type(fields.get(b"msg_type")) is not int:
        raise ValueError("invalid metadata message")
    piece = fields.get(b"piece")
    if fields[b"msg_type"] in (0, 1, 2) and (type(piece) is not int or piece < 0):
        raise ValueError("invalid metadata piece")
    return fields, payload[end:]


class MetadataServer:
    def __init__(self, torrent):
        raw = torrent.info_bytes
        self.info = raw if not torrent.private and 0 < len(raw) <= MAX_METADATA else b""
        self.remote_id = 0
        self.requests = 0

    def receive(self, payload):
        if not payload or len(payload) > MAX_EXTENDED:
            raise ValueError("invalid extended message")
        identifier, body = payload[0], payload[1:]
        if identifier == 0:
            remote_id, _ = negotiation(body)
            if remote_id is not None:
                self.remote_id = remote_id
            return None
        if identifier != METADATA_ID:
            return None  # Unknown extensions are safely ignored.
        fields, data = metadata_message(body)
        if fields[b"msg_type"] != 0:
            return None
        if data:
            raise ValueError("metadata request has trailing data")
        count = (len(self.info) + BLOCK - 1) // BLOCK
        self.requests += 1
        if self.requests > 2 * count + 16:
            raise ValueError("metadata request budget exceeded")
        if not self.remote_id:
            return None
        piece = fields[b"piece"]
        if piece >= count:
            return extended(self.remote_id, encode({b"msg_type": 2, b"piece": piece}))
        block = self.info[piece * BLOCK:(piece + 1) * BLOCK]
        return extended(self.remote_id, encode({b"msg_type": 1, b"piece": piece,
                                               b"total_size": len(self.info)}) + block)
