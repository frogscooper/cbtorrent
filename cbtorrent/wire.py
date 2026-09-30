"""BEP 3 TCP messages with bounded reads and byte accounting."""
import asyncio
import struct

from .metrics import Metrics
from .extensions import MAX_EXTENDED, RESERVED, MetadataServer, handshake as extension_handshake

PROTOCOL = b"\x13BitTorrent protocol"
BLOCK_SIZE = 16 * 1024


class PieceBuffer:
    """One bounded assembly shared by at most two endgame connections.

    All mutation runs on the event loop. Only client.py verifies and commits it.
    """
    def __init__(self, size):
        self.data = bytearray(size)
        self.blocks = tuple((offset, min(BLOCK_SIZE, size - offset))
                            for offset in range(0, size, BLOCK_SIZE))
        self.received = set()
        self.received_bytes = 0
        self.owners = set()
        self.raced = False
        self.invalid = False

    @property
    def missing_bytes(self):
        return len(self.data) - self.received_bytes

    def accept(self, offset, block):
        if offset in self.received:
            return False
        self.data[offset:offset + len(block)] = block
        self.received.add(offset)
        self.received_bytes += len(block)
        return True


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
        self.pending = {}
        self.extensions = MetadataServer(torrent) if hasattr(torrent, "info_bytes") else None

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
        await self.send(PROTOCOL + RESERVED + self.torrent.info_hash + peer_id)
        reply = await self.read(68)
        if reply[:20] != PROTOCOL or reply[28:48] != self.torrent.info_hash:
            raise ValueError("peer handshake does not match torrent")
        if reply[48:] == peer_id:
            raise ValueError("self connection")
        if reply[25] & 0x10:
            await self.send(extension_handshake(self.extensions.info))
        await self.send(message(2))

    async def receive(self):
        size = struct.unpack("!I", await self.read(4))[0]
        limit = max(MAX_EXTENDED + 1, 1 + (len(self.torrent.hashes) + 7) // 8)
        if size > limit:
            raise ValueError("oversized peer message")
        if size == 0:
            return None, b""
        data = await self.read(size)
        kind, payload = data[0], data[1:]
        if kind == 20:
            response = self.extensions.receive(payload)
            if response is not None:
                await self.send(response)
            return kind, payload  # Extension handshake may precede the bitfield.
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

    def cancel_block(self, index, offset):
        length = self.pending.get((index, offset))
        if length is None:
            return
        data = message(8, struct.pack("!III", index, offset, length))
        self.writer.write(data)
        self.metrics.wire_sent_bytes += len(data)
        self.sent_bytes += len(data)
        self.metrics.cancel_requests += 1

    async def download_piece(self, index, pipeline, on_block=None, *, buffer=None,
                             on_data=None, endgame=False):
        buffer = buffer or PieceBuffer(self.torrent.piece_size(index))
        cursor = 0
        try:
            while buffer.missing_bytes:
                while not self.choked and cursor < len(buffer.blocks) and len(self.pending) < pipeline:
                    offset, length = buffer.blocks[cursor]
                    cursor += 1
                    if offset in buffer.received:
                        continue
                    self.pending[index, offset] = length
                    if endgame:
                        self.metrics.endgame_requested_bytes += length
                    await self.send(message(6, struct.pack("!III", index, offset, length)))
                kind, payload = await self.receive()
                if kind == 0:
                    raise ConnectionError("peer choked during piece transfer")
                if kind == 7:
                    if len(payload) < 8:
                        raise ValueError("truncated piece message")
                    piece, offset = struct.unpack("!II", payload[:8])
                    block = payload[8:]
                    if piece != index or len(block) != self.pending.get((piece, offset)):
                        raise ValueError("unsolicited or incorrectly sized block")
                    del self.pending[piece, offset]
                    if buffer.accept(offset, block):
                        if on_block is not None:
                            on_block(buffer.received_bytes)
                        if on_data is not None:
                            on_data(offset)
                    elif buffer.raced:
                        self.metrics.endgame_duplicate_bytes += len(block)
            return bytes(buffer.data)
        finally:
            # Late replies remain legal only on this attempt. The client closes
            # interrupted/raced connections before reusing the peer address.
            try:
                for piece, offset in tuple(self.pending):
                    self.cancel_block(piece, offset)
                if self.pending:
                    await asyncio.wait_for(self.writer.drain(), min(self.timeout, 0.25))
            except (OSError, asyncio.TimeoutError):
                pass
            self.pending.clear()

    async def close(self):
        self.writer.close()
        try:
            await asyncio.wait_for(self.writer.wait_closed(), self.timeout)
        except (OSError, asyncio.TimeoutError):
            pass
