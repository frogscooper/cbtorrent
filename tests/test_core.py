import asyncio
import hashlib
import struct
import tempfile
import unittest
from pathlib import Path

from cbtorrent.bencode import decode, encode
from cbtorrent.client import DownloadError, download
from cbtorrent.metainfo import Torrent
from cbtorrent.policy import Observation, ThroughputPolicy


def metadata(data, piece_length=32768):
    info = {b"name": b"fixture.bin", b"length": len(data), b"piece length": piece_length,
            b"pieces": b"".join(hashlib.sha1(data[i:i + piece_length]).digest()
                                 for i in range(0, len(data), piece_length))}
    return Torrent.from_bytes(encode({b"info": info}))


class CoreTests(unittest.TestCase):
    def test_bencode_roundtrip(self):
        value = {b"a": [0, -12, b"\x00\xff"], b"b": {}}
        self.assertEqual(decode(encode(value)), value)

    def test_rejects_malformed_bencode(self):
        for raw in (b"i-0e", b"i01e", b"i+1e", b"01:a", b"3:ab", b"leextra",
                    b"d1:bi1e1:ai2ee", b"d1:ai1e1:ai2ee", b"l", b"i2", b"deX"):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                decode(raw)

    def test_depth_limit(self):
        with self.assertRaises(ValueError):
            decode(b"l" * 70 + b"e" * 70)

    def test_info_hash_and_final_piece(self):
        info = {b"length": 3, b"name": b"x", b"piece length": 2,
                b"pieces": hashlib.sha1(b"ab").digest() + hashlib.sha1(b"c").digest()}
        torrent = Torrent.from_bytes(encode({b"info": info}))
        self.assertEqual(torrent.info_hash, hashlib.sha1(encode(info)).digest())
        self.assertEqual(torrent.piece_size(1), 1)

    def test_invalid_hash_count(self):
        with self.assertRaises(ValueError):
            Torrent.from_bytes(encode({b"info": {b"name": b"x", b"length": 10,
                                                b"piece length": 2, b"pieces": b""}}))

    def test_policy_explores_then_uses_verified_throughput(self):
        a, b = ("a", 1), ("b", 2)
        policy = ThroughputPolicy()
        stats = {a: Observation(100, 1)}
        self.assertEqual(policy.choose([a, b], stats), b)
        stats[b] = Observation(200, 1)
        self.assertEqual(policy.choose([a, b], stats), b)
        stats[b].failures = 3
        self.assertEqual(policy.choose([a, b], stats), a)


class TransferTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.output = Path(self.directory.name) / "download.bin"
        self.data = bytes(range(256)) * 301
        self.torrent = metadata(self.data)
        self.servers = []
        self.handlers = set()
        self.server_received = 0
        self.server_sent = 0

    async def asyncTearDown(self):
        for server in self.servers:
            server.close()
            await server.wait_closed()
        for task in list(self.handlers):
            task.cancel()
        await asyncio.gather(*self.handlers, return_exceptions=True)
        self.directory.cleanup()

    async def seed(self, *, corrupt=False, stall=False, wrong_hash=False, oversized=False,
                   reverse_blocks=False, choke=False):
        async def handle(reader, writer):
            task = asyncio.current_task()
            self.handlers.add(task)

            async def read(size):
                data = await reader.readexactly(size)
                self.server_received += len(data)
                return data

            async def send(data):
                self.server_sent += len(data)
                writer.write(data)
                await writer.drain()

            try:
                handshake = await read(68)
                expected = b"\x13BitTorrent protocol" + bytes(8) + self.torrent.info_hash
                self.assertEqual(handshake[:48], expected)
                await send(expected[:28] + (bytes(20) if wrong_hash else self.torrent.info_hash) + b"S" * 20)
                if wrong_hash:
                    return
                if oversized:
                    await send(struct.pack("!I", 0xFFFFFFFF))
                    return
                if stall:
                    await reader.read()
                    return
                count = len(self.torrent.hashes)
                bits = bytearray((count + 7) // 8)
                for index in range(count):
                    bits[index // 8] |= 128 >> (index % 8)
                await send(struct.pack("!IB", len(bits) + 1, 5) + bits)
                await send(b"\x00\x00\x00\x01\x01")
                responses = []
                while True:
                    size = struct.unpack("!I", await read(4))[0]
                    packet = await read(size)
                    if packet and packet[0] == 6:
                        if choke:
                            await send(b"\x00\x00\x00\x01\x00")
                            return
                        index, offset, length = struct.unpack("!III", packet[1:])
                        self.assertLessEqual(length, 16384)
                        start = index * self.torrent.piece_length + offset
                        block = self.data[start:start + length]
                        if corrupt:
                            block = bytes([block[0] ^ 255]) + block[1:]
                        response = struct.pack("!IBII", len(block) + 9, 7, index, offset) + block
                        if reverse_blocks:
                            responses.append(response)
                            block_count = (self.torrent.piece_size(index) + 16383) // 16384
                            if len(responses) == block_count:
                                for response in reversed(responses):
                                    await send(response)
                                responses.clear()
                        else:
                            await send(response)
            except (asyncio.IncompleteReadError, ConnectionError):
                pass
            finally:
                writer.close()
                await writer.wait_closed()
                self.handlers.discard(task)

        server = await asyncio.start_server(handle, "127.0.0.1", 0)
        self.servers.append(server)
        return "127.0.0.1", server.sockets[0].getsockname()[1]

    async def test_real_tcp_download_and_exact_accounting(self):
        address = await self.seed()
        report = await download(self.torrent, [address], self.output)
        await asyncio.sleep(0.01)
        self.assertEqual(self.output.read_bytes(), self.data)
        self.assertFalse(self.output.with_suffix(".bin.part").exists())
        self.assertEqual(report["verified_bytes"], len(self.data))
        self.assertEqual(report["wasted_payload_bytes"], 0)
        self.assertEqual(report["wire_received_bytes"], self.server_sent)
        self.assertEqual(report["wire_sent_bytes"], self.server_received)
        self.assertEqual(report["protocol_overhead_bytes"], self.server_sent + self.server_received - len(self.data))
        self.assertGreater(report["completion_seconds"], 0)

    async def test_corrupt_peer_fails_over(self):
        bad, good = await self.seed(corrupt=True), await self.seed()
        report = await download(self.torrent, [bad, good], self.output)
        self.assertEqual(self.output.read_bytes(), self.data)
        self.assertEqual(report["hash_failures"], 1)
        self.assertEqual(report["peer_failures"], 1)
        self.assertEqual(report["wasted_payload_bytes"], self.torrent.piece_length)

    async def test_out_of_order_pipelined_blocks(self):
        address = await self.seed(reverse_blocks=True)
        report = await download(self.torrent, [address], self.output, pipeline=8)
        self.assertTrue(report["complete"])
        self.assertEqual(self.output.read_bytes(), self.data)

    async def test_choke_fails_over_without_writing_unverified_data(self):
        bad, good = await self.seed(choke=True), await self.seed()
        report = await download(self.torrent, [bad, good], self.output)
        self.assertEqual(report["peer_failures"], 1)
        self.assertEqual(self.output.read_bytes(), self.data)

    async def test_does_not_overwrite_partial_file(self):
        part = self.output.with_suffix(".bin.part")
        part.write_bytes(b"prior run")
        with self.assertRaises(DownloadError):
            await download(self.torrent, [("localhost", 1)], self.output)
        self.assertEqual(part.read_bytes(), b"prior run")

    async def test_stall_is_bounded_and_preserves_partial(self):
        address = await self.seed(stall=True)
        with self.assertRaises(DownloadError) as caught:
            await download(self.torrent, [address], self.output, timeout=0.05, piece_timeout=0.2)
        self.assertFalse(caught.exception.report["complete"])
        self.assertIsNone(caught.exception.report["completion_seconds"])
        self.assertFalse(self.output.exists())
        self.assertTrue(self.output.with_suffix(".bin.part").exists())

    async def test_rejects_wrong_handshake(self):
        address = await self.seed(wrong_hash=True)
        with self.assertRaises(DownloadError):
            await download(self.torrent, [address], self.output)

    async def test_rejects_oversized_message(self):
        address = await self.seed(oversized=True)
        with self.assertRaises(DownloadError):
            await download(self.torrent, [address], self.output)

    async def test_does_not_overwrite_files(self):
        self.output.write_bytes(b"keep")
        with self.assertRaises(FileExistsError):
            await download(self.torrent, [("localhost", 1)], self.output)
        self.assertEqual(self.output.read_bytes(), b"keep")


if __name__ == "__main__":
    unittest.main()
