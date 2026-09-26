import asyncio
import socket
import struct
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from urllib.parse import unquote_to_bytes

from cbtorrent.bencode import encode
from cbtorrent.client import download
from cbtorrent.metainfo import create
from cbtorrent.seeder import FileSource, SeedServer
from cbtorrent.tracker import announce, compact_peers, parse_response


class ResponseTests(unittest.TestCase):
    def test_compact_ipv4_and_ipv6(self):
        raw = socket.inet_aton("127.0.0.1") + struct.pack("!H", 6881)
        raw6 = socket.inet_pton(socket.AF_INET6, "::1") + struct.pack("!H", 6882)
        result = parse_response(encode({b"interval": 60, b"peers": raw, b"peers6": raw6}))
        self.assertEqual(result.peers, (("127.0.0.1", 6881), ("::1", 6882)))

    def test_dictionary_peers(self):
        result = parse_response(encode({b"interval": 60, b"peers": [{b"ip": b"localhost", b"port": 42}]}))
        self.assertEqual(result.peers, (("localhost", 42),))

    def test_failure_and_malformed_responses(self):
        for value in ({b"failure reason": b"denied"}, {b"interval": 0},
                      {b"interval": 60, b"peers": b"bad"},
                      {b"interval": 60, b"peers": [{b"ip": b"a", b"port": 0}]}, []):
            with self.subTest(value=value), self.assertRaises(ValueError):
                parse_response(encode(value))

    def test_zero_ports_are_not_candidates(self):
        self.assertEqual(compact_peers(bytes(6)), [])


class TrackerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.servers, self.handlers, self.events = [], set(), []
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)

    async def asyncTearDown(self):
        for server in self.servers:
            server.close()
            await server.wait_closed()
        tasks = tuple(self.handlers)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self.temp.cleanup()

    async def http(self, peers=b"", *, chunked=False, stall=False, oversize=False, truncated=False):
        async def handle(reader, writer):
            task = asyncio.current_task()
            self.handlers.add(task)
            try:
                headers = await reader.readuntil(b"\r\n\r\n")
                target = headers.split(b" ")[1]
                query = target.split(b"?", 1)[1]
                values = {key.decode(): unquote_to_bytes(value) for key, value in
                          (part.split(b"=", 1) for part in query.split(b"&"))}
                self.events.append(values)
                if stall:
                    await reader.read()
                    return
                body = encode({b"interval": 30, b"peers": peers})
                if oversize:
                    response = b"HTTP/1.1 200 OK\r\nContent-Length: 999999999\r\n\r\n"
                elif truncated:
                    response = b"HTTP/1.1 200 OK\r\nContent-Length: 999\r\n\r\nabc"
                elif chunked:
                    response = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"
                    response += f"{len(body):X}\r\n".encode() + body + b"\r\n0\r\n\r\n"
                else:
                    response = f"HTTP/1.1 200 OK\r\nContent-Length: {len(body)}\r\n\r\n".encode() + body
                writer.write(response)
                await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()
                self.handlers.discard(task)
        server = await asyncio.start_server(handle, "127.0.0.1", 0)
        self.servers.append(server)
        return f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}/announce?passkey=abc"

    async def test_http_preserves_binary_identity_and_query(self):
        url = await self.http()
        info_hash = bytes(range(20))
        peer_id = b" /+?&%" + bytes(range(14))
        response = await announce(url, info_hash, peer_id, port=6881, left=123)
        self.assertEqual(response.interval, 30)
        self.assertEqual(self.events[0]["info_hash"], info_hash)
        self.assertEqual(self.events[0]["peer_id"], peer_id)
        self.assertEqual(self.events[0]["passkey"], b"abc")
        self.assertEqual(self.events[0]["left"], b"123")

    async def test_chunked_http_response(self):
        response = await announce(await self.http(chunked=True), bytes(20), b"P" * 20, port=1)
        self.assertEqual(response.peers, ())

    async def test_tracker_timeout_is_bounded(self):
        with self.assertRaises(asyncio.TimeoutError):
            await announce(await self.http(stall=True), bytes(20), b"P" * 20, port=1, timeout=0.05)

    async def test_oversized_http_body_is_rejected_without_reading(self):
        with self.assertRaises(ValueError):
            await announce(await self.http(oversize=True), bytes(20), b"P" * 20, port=1)

    async def test_truncated_tracker_is_a_recoverable_error(self):
        with self.assertRaises(ValueError):
            await announce(await self.http(truncated=True), bytes(20), b"P" * 20, port=1)

    async def test_discovery_and_tracker_lifecycle(self):
        path = self.root / "source"
        data = b"tracker discovery" * 4000
        path.write_bytes(data)
        torrent = create(path, self.root / "test.torrent", piece_length=32768)
        source = FileSource(torrent, path)
        seed = SeedServer(torrent, source)
        try:
            port = await seed.start()
            url = await self.http(socket.inet_aton("127.0.0.1") + struct.pack("!H", port))
            torrent = replace(torrent, trackers=(url,))
            output = self.root / "result"
            report = await download(torrent, [], output)
            self.assertEqual(output.read_bytes(), data)
            self.assertTrue(report["complete"])
            self.assertEqual([row["event"] for row in self.events], [b"started", b"completed", b"stopped"])
            self.assertEqual(self.events[-1]["left"], b"0")
            self.assertEqual(int(self.events[-1]["downloaded"]), len(data))
        finally:
            await seed.close()
            source.close()

    async def test_udp_connect_announce_and_transaction_validation(self):
        packets = []
        class Tracker(asyncio.DatagramProtocol):
            def connection_made(self, transport):
                self.transport = transport
            def datagram_received(self, data, address):
                packets.append(data)
                action = int.from_bytes(data[8:12], "big")
                transaction = data[12:16]
                if action == 0:
                    self.transport.sendto(struct.pack("!I", 0) + b"BAD!" + b"C" * 8, address)
                    self.transport.sendto(struct.pack("!I", 0) + transaction + b"C" * 8, address)
                else:
                    response = struct.pack("!I", 1) + transaction + struct.pack("!III", 45, 2, 3)
                    response += socket.inet_aton("127.0.0.1") + struct.pack("!H", 6881)
                    self.transport.sendto(response, address)
        loop = asyncio.get_running_loop()
        transport, _ = await loop.create_datagram_endpoint(Tracker, local_addr=("127.0.0.1", 0))
        try:
            port = transport.get_extra_info("sockname")[1]
            response = await announce(f"udp://127.0.0.1:{port}/announce", b"H" * 20, b"P" * 20,
                                      port=6882, downloaded=123, uploaded=12, left=456)
            self.assertEqual(response.peers, (("127.0.0.1", 6881),))
            self.assertEqual(response.interval, 45)
            self.assertEqual(packets[0][:8], struct.pack("!Q", 0x41727101980))
            self.assertEqual(packets[1][:8], b"C" * 8)
            self.assertEqual(packets[1][16:36], b"H" * 20)
            self.assertEqual(struct.unpack("!QQQ", packets[1][56:80]), (123, 456, 12))
            self.assertEqual(len(packets[1]), 98)
        finally:
            transport.close()
