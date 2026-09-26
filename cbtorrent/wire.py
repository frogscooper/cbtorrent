"""BEP 3 TCP messages with bounded reads and byte accounting."""
import asyncio
import struct

from .metrics import Metrics

PROTOCOL = b"\x13BitTorrent protocol"
BLOCK_SIZE = 16 * 1024


def message(kind: int, payload: bytes = b"") -> bytes:
    return struct.pack("!IB", 1 + len(payload), kind) + payload


class Peer:
    def __init__(self, reader, writer, torrent, metrics: Metrics, timeout: float):
        self.reader, self.writer = reader, writer
        self.torrent, self.metrics, self.timeout = torrent, metrics, timeout
        self.available = set()
        self.choked = True
        self.seen_message = False
        self.sent_bytes = 0
        self.received_bytes = 0

    async def read(self, size):
        try:
            data = await asyncio.wait_for(self.reader.readexactly(size), self.timeout)
        except asyncio.IncompleteReadError as error:
            self.metrics.wire_received_bytes += len(error.partial)
            self.received_bytes += len(error.partial)
            raise
        self.metrics.wire_received_bytes += len(data)
        self.received_bytes += len(data)
        return data

    async def send(self, data):
        self.writer.write(data)
        self.metrics.wire_sent_bytes += len(data)
        self.sent_bytes += len(data)
        await asyncio.wait_for(self.writer.drain(), self.timeout)

    async def handshake(self, peer_id):
        await self.send(PROTOCOL + bytes(8) + self.torrent.info_hash + peer_id)
        reply = await self.read(68)
        if reply[:20] != PROTOCOL or reply[28:48] != self.torrent.info_hash:
            raise ValueError("peer handshake does not match torrent")
        if reply[48:] == peer_id:
            raise ValueError("self connection")
        await self.send(message(2))

    async def receive(self):
        size = struct.unpack("!I", await self.read(4))[0]
        limit = max(BLOCK_SIZE + 9, 1 + (len(self.torrent.hashes) + 7) // 8)
        if size > limit:
            raise ValueError("oversized peer message")
        if size == 0:
            return None, b""
        data = await self.read(size)
        kind, payload = data[0], data[1:]
        if kind == 7 and len(payload) >= 8:
            self.metrics.payload_received_bytes += len(payload) - 8
        if kind in (0, 1, 2, 3):
            if payload:
                raise ValueError("invalid control message")
            if kind in (0, 1):
                self.choked = kind == 0
        elif kind == 4:
            if len(payload) != 4:
                raise ValueError("invalid have message")
            index = struct.unpack("!I", payload)[0]
            self.torrent.piece_size(index)
            self.available.add(index)
        elif kind == 5:
            count = len(self.torrent.hashes)
            if self.seen_message or len(payload) != (count + 7) // 8:
                raise ValueError("invalid bitfield")
            if count % 8 and payload[-1] & ((1 << (8 - count % 8)) - 1):
                raise ValueError("nonzero spare bitfield bits")
            self.available = {i for i in range(count) if payload[i // 8] & (128 >> (i % 8))}
        elif kind not in (6, 7, 8):
            raise ValueError("unsupported peer message")
        self.seen_message = True
        return kind, payload

    async def ready(self):
        while self.choked or not self.available:
            await self.receive()

    async def download_piece(self, index, pipeline, on_block=None):
        size = self.torrent.piece_size(index)
        blocks = [(offset, min(BLOCK_SIZE, size - offset)) for offset in range(0, size, BLOCK_SIZE)]
        pending = {}
        result = bytearray(size)
        cursor = received = 0
        while received < size:
            while not self.choked and cursor < len(blocks) and len(pending) < pipeline:
                offset, length = blocks[cursor]
                pending[offset] = length
                await self.send(message(6, struct.pack("!III", index, offset, length)))
                cursor += 1
            kind, payload = await self.receive()
            if kind == 0:
                # Reconnect and retry this piece: old requests may be discarded on choke.
                raise ConnectionError("peer choked during piece transfer")
            if kind == 7:
                if len(payload) < 8:
                    raise ValueError("truncated piece message")
                piece, offset = struct.unpack("!II", payload[:8])
                block = payload[8:]
                if piece != index or offset not in pending or len(block) != pending[offset]:
                    raise ValueError("unsolicited or incorrectly sized block")
                del pending[offset]
                result[offset:offset + len(block)] = block
                received += len(block)
                if on_block is not None:
                    on_block(received)
        return bytes(result)

    async def close(self):
        self.writer.close()
        try:
            await asyncio.wait_for(self.writer.wait_closed(), self.timeout)
        except (OSError, asyncio.TimeoutError):
            pass
