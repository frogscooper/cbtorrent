"""BEP 3 TCP messages with bounded reads and byte accounting."""
import asyncio
import struct
from time import perf_counter

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
        self.last_progress = perf_counter()  # when any owner last added a block
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
        self.last_progress = perf_counter()
        return True


def message(kind: int, payload: bytes = b"") -> bytes:
    return struct.pack("!IB", 1 + len(payload), kind) + payload


class Peer:
    def __init__(self, reader, writer, torrent, metrics: Metrics, timeout: float, *, pex=None):
        self.reader, self.writer = reader, writer
        self.torrent, self.metrics, self.timeout = torrent, metrics, timeout
        self.available = set()
        self.choked = True
        self.seen_message = False
        self.sent_bytes = 0
        self.received_bytes = 0
        self.pending = {}
        self.extensions = MetadataServer(torrent) if hasattr(torrent, "info_bytes") else None
        self.pex = pex
        self.handshaken = False

    async def read(self, size):
        try:
            async with asyncio.timeout(self.timeout):
                data = await self.reader.readexactly(size)
        except asyncio.IncompleteReadError as error:
            self.metrics.wire_received_bytes += len(error.partial)
            self.received_bytes += len(error.partial)
            raise
        self.metrics.wire_received_bytes += len(data)
        self.received_bytes += len(data)
        return data

    async def send(self, data):
        # Writing to a dropped connection only makes asyncio log
        # "socket.send() raised exception." for every further write.
        if self.writer.is_closing():
            raise ConnectionResetError("connection already closed")
        self.writer.write(data)
        self.metrics.wire_sent_bytes += len(data)
        self.sent_bytes += len(data)
        async with asyncio.timeout(self.timeout):
            await self.writer.drain()

    async def handshake(self, peer_id):
        await self.send(PROTOCOL + RESERVED + self.torrent.info_hash + peer_id)
        reply = await self.read(68)
        if reply[:20] != PROTOCOL or reply[28:48] != self.torrent.info_hash:
            raise ValueError("peer handshake does not match torrent")
        if reply[48:] == peer_id:
            raise ValueError("self connection")
        if reply[25] & 0x10:
            await self.send(extension_handshake(self.extensions.info, pex=self.pex is not None))
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
            if self.pex is not None:
                if payload[0] == 0:
                    self.pex.negotiate(payload[1:], self.extensions.remote_id)
                    await self.pex_update()
                elif payload[0] == 2:
                    self.metrics.pex_received_bytes += size + 4
                    self.metrics.pex_messages_received += 1
                    self.pex.receive(payload[1:])
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

    async def pex_update(self):
        if self.pex is not None:
            data = self.pex.outgoing()
            if data is not None:
                self.metrics.pex_sent_bytes += len(data)
                self.metrics.pex_messages_sent += 1
                await self.send(data)

    async def ready(self):
        while self.choked or not self.available:
            await self.receive()

    def cancel_block(self, index, offset):
        length = self.pending.get((index, offset))
        if length is None or self.writer.is_closing():
            return  # nothing to cancel on a dropped connection
        data = message(8, struct.pack("!III", index, offset, length))
        self.writer.write(data)
        self.metrics.wire_sent_bytes += len(data)
        self.sent_bytes += len(data)
        self.metrics.cancel_requests += 1

    async def download_piece(self, index, pipeline, on_block=None, *, buffer=None,
                             on_data=None, endgame=False, stall_delay=None):
        """Fetch the missing blocks of one piece into buffer.

        An endgame helper (endgame=True with a stall_delay) first probes with
        only the tail block, which an in-order owner would deliver last. It
        races the remaining blocks only if the whole piece then makes no
        progress for stall_delay. A short pause from a live owner therefore
        costs one duplicate block instead of the entire piece.
        """
        buffer = buffer or PieceBuffer(self.torrent.piece_size(index))
        blocks = buffer.blocks[::-1] if endgame else buffer.blocks
        probing = endgame and stall_delay is not None
        probe_sent = False
        cursor = 0
        try:
            while buffer.missing_bytes:
                if probing and probe_sent and not self.pending:
                    wait = stall_delay - (perf_counter() - buffer.last_progress)
                    if wait > 0:
                        await asyncio.sleep(wait)
                        continue
                    probing = False
                limit = 0 if probing and probe_sent else (1 if probing else pipeline)
                while not self.choked and cursor < len(blocks) and len(self.pending) < limit:
                    offset, length = blocks[cursor]
                    cursor += 1
                    # Check again just before asking: another owner may have
                    # delivered this block while we waited.
                    if offset in buffer.received:
                        continue
                    probe_sent = True
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
                    async with asyncio.timeout(min(self.timeout, 0.25)):
                        await self.writer.drain()
            except (OSError, asyncio.TimeoutError):
                pass
            self.pending.clear()

    async def close(self):
        self.writer.close()
        try:
            async with asyncio.timeout(self.timeout):
                await self.writer.wait_closed()
        except (OSError, asyncio.TimeoutError):
            pass
