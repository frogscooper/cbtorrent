"""A bounded TCP seed server, also used to share verified partial downloads."""
import asyncio
import math
import os
import struct
from bisect import bisect_right
from collections import deque
from hashlib import sha1
from pathlib import Path

from .metrics import Metrics
from .filepaths import open_payload, reject_symlinks
from .wire import BLOCK_SIZE, PROTOCOL, Peer, message
from .extensions import RESERVED, handshake as extension_handshake


class FileSource:
    def __init__(self, torrent, path):
        self.torrent = torrent
        self.path = Path(path)
        self.stream = None
        self.ends = tuple(f.offset + f.length for f in torrent.files)
        self.verified = set()
        try:
            if torrent.multi_file:
                reject_symlinks(self.path)
                if not self.path.is_dir():
                    raise ValueError("multi-file seed source must be a directory")
                for file in torrent.files:
                    with open_payload(self.path.joinpath(*file.path)) as stream:
                        if os.fstat(stream.fileno()).st_size != file.length:
                            raise ValueError("seed file size does not match torrent")
            else:
                self.stream = self.path.open("rb")
                if os.fstat(self.stream.fileno()).st_size != torrent.length:
                    raise ValueError("seed file size does not match torrent")
            for index, expected in enumerate(torrent.hashes):
                data = self._read_at(index * torrent.piece_length, torrent.piece_size(index))
                if sha1(data).digest() != expected:
                    raise ValueError(f"seed file failed hash check at piece {index}")
                self.verified.add(index)
        except BaseException:
            self.close()
            raise

    def read(self, index, offset, length):
        if index not in self.verified or offset < 0 or not 0 < length <= BLOCK_SIZE:
            raise ValueError("invalid or unavailable seed block")
        if offset + length > self.torrent.piece_size(index):
            raise ValueError("seed request crosses piece boundary")
        data = self._read_at(index * self.torrent.piece_length + offset, length)
        if len(data) != length:
            raise OSError("seed file changed during transfer")
        return data

    def _read_at(self, start, length):
        if not self.torrent.multi_file:
            self.stream.seek(start)
            return self.stream.read(length)
        # Zero-length files have repeated ends. bisect_right skips them and
        # finds the first file containing this byte without scanning the list.
        index = bisect_right(self.ends, start)
        data = bytearray()
        while len(data) < length and index < len(self.torrent.files):
            file = self.torrent.files[index]
            take = min(length - len(data), file.offset + file.length - start)
            if take:
                with open_payload(self.path.joinpath(*file.path)) as stream:
                    if os.fstat(stream.fileno()).st_size != file.length:
                        raise OSError("seed file changed during transfer")
                    stream.seek(start - file.offset)
                    block = stream.read(take)
                    if len(block) != take:
                        raise OSError("seed file changed during transfer")
                    data.extend(block)
                start += take
            index += 1
        return bytes(data)

    def close(self):
        if self.stream is not None:
            self.stream.close()


class SeedServer:
    def __init__(self, torrent, source, *, metrics=None, peer_id=None, max_clients=32,
                 timeout=30.0, rate=0, latency=0.0, pex_factory=None):
        if max_clients < 1 or timeout <= 0 or rate < 0 or latency < 0:
            raise ValueError("invalid seed server limits")
        if not all(math.isfinite(x) for x in (timeout, rate, latency)):
            raise ValueError("seed limits must be finite")
        self.torrent, self.source = torrent, source
        self.metrics = metrics or Metrics()
        self.peer_id = peer_id or b"-CB0002-" + os.urandom(12)
        self.max_clients, self.timeout = max_clients, timeout
        self.rate, self.latency = rate, latency
        self.pex_factory = pex_factory if not torrent.private else None
        self.server = None
        self.tasks = set()
        self.peers = set()
        self.errors = []

    async def start(self, host="127.0.0.1", port=0):
        self.server = await asyncio.start_server(self._accept, host, port)
        return self.server.sockets[0].getsockname()[1]

    def _accept(self, reader, writer):
        if len(self.tasks) >= self.max_clients:
            writer.close()
            return
        task = asyncio.create_task(self._serve(reader, writer))
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)

    async def _serve(self, reader, writer):
        peer = Peer(reader, writer, self.torrent, self.metrics, self.timeout,
                    pex=self.pex_factory(writer) if self.pex_factory else None)
        queue = deque()
        wake = asyncio.Event()
        interested = False
        uploader = None
        try:
            reply = await peer.read(68)
            if reply[:20] != PROTOCOL or reply[28:48] != self.torrent.info_hash or reply[48:] == self.peer_id:
                raise ValueError("invalid incoming handshake")
            await peer.send(PROTOCOL + RESERVED + self.torrent.info_hash + self.peer_id)
            peer.handshaken = True
            if reply[25] & 0x10:
                await peer.send(extension_handshake(peer.extensions.info, pex=peer.pex is not None))
            bitfield = bytearray((len(self.torrent.hashes) + 7) // 8)
            for index in self.source.verified:
                bitfield[index // 8] |= 128 >> (index % 8)
            await peer.send(message(5, bitfield))
            self.peers.add(peer)

            async def upload():
                while True:
                    await wake.wait()
                    while queue:
                        request = queue[0]
                        index, offset, length = request
                        if self.latency or self.rate:
                            await asyncio.sleep(self.latency + (length / self.rate if self.rate else 0))
                        # A cancel or not-interested can remove the queued request while asleep.
                        if not interested or not queue or queue[0] != request:
                            continue
                        queue.popleft()
                        data = self.source.read(index, offset, length)
                        await peer.send(message(7, struct.pack("!II", index, offset) + data))
                        self.metrics.uploaded_bytes += len(data)
                    wake.clear()

            async def receive():
                nonlocal interested
                while True:
                    kind, payload = await peer.receive()
                    if kind == 2:
                        interested = True
                        await peer.send(message(1))
                    elif kind == 3:
                        interested = False
                        queue.clear()
                    elif kind in (6, 8):
                        if len(payload) != 12:
                            raise ValueError("malformed request/cancel")
                        request = struct.unpack("!III", payload)
                        index, offset, length = request
                        if not 0 < length <= BLOCK_SIZE or offset + length > self.torrent.piece_size(index):
                            raise ValueError("invalid block request bounds")
                        if kind == 8:
                            try:
                                queue.remove(request)
                            except ValueError:
                                pass
                        elif interested and index in self.source.verified:
                            if len(queue) >= 128:
                                raise ValueError("peer exceeded request queue limit")
                            if request not in queue:
                                queue.append(request)
                                wake.set()
                    elif kind == 7:
                        raise ValueError("unsolicited upload to seed")

            uploader = asyncio.create_task(upload())
            receiver = asyncio.create_task(receive())
            try:
                done, pending = await asyncio.wait((uploader, receiver), return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    task.result()
            finally:
                for task in (uploader, receiver):
                    task.cancel()
                await asyncio.gather(uploader, receiver, return_exceptions=True)
        except (OSError, ValueError, asyncio.TimeoutError, asyncio.IncompleteReadError) as error:
            if len(self.errors) < 20:
                self.errors.append(str(error))
        finally:
            self.peers.discard(peer)
            await peer.close()

    async def have(self, index):
        results = await asyncio.gather(*(peer.send(message(4, struct.pack("!I", index)))
                                         for peer in tuple(self.peers)), return_exceptions=True)
        return results

    async def close(self):
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
        tasks = tuple(self.tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
