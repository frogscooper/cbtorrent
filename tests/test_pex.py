"""BEP 11 encoding, hostile hints, rate bounds and loopback discovery."""
import asyncio
import ipaddress
import struct
import tempfile
import unittest
from pathlib import Path

from cbtorrent.bencode import decode, encode
from cbtorrent.client import DownloadError, download
from cbtorrent.extensions import RESERVED, extended, handshake
from cbtorrent.metainfo import create, Torrent
from cbtorrent.pex import MAX_PEX, PexSession, parse
from cbtorrent.seeder import FileSource, SeedServer
from cbtorrent.wire import PROTOCOL, message


def compact(address):
    host, port = address
    return ipaddress.ip_address(host).packed + struct.pack("!H", port)


class PexTests(unittest.TestCase):
    def test_v4_v6_flags_and_drops(self):
        v4, v6 = ("127.0.0.2", 1), ("::1", 2)
        raw = encode({b"added": compact(v4), b"added.f": b"\x10",
                      b"added6": compact(v6), b"added6.f": b"\x02",
                      b"dropped": compact(("127.0.0.3", 3))})
        self.assertEqual(parse(raw), ((v4, v6), (("127.0.0.3", 3),)))

    def test_bad_types_lengths_flags_duplicates_and_counts_are_rejected(self):
        peer = compact(("127.0.0.2", 1))
        cases = [[], {}, {b"added": 1}, {b"added": b"x"},
                 {b"added6": b"x" * 17}, {b"added": peer, b"added.f": b""},
                 {b"added": peer * 2}, {b"added": peer, b"dropped": peer}]
        for fields in cases:
            with self.subTest(fields=fields), self.assertRaises(ValueError):
                parse(encode(fields))
        many = b"".join(compact((f"127.0.0.{i}", 1)) for i in range(1, 52))
        self.assertEqual(len(parse(encode({b"added": many}))[0]), 51)
        with self.assertRaises(ValueError):
            parse(encode({b"added": many}), initial=False)
        with self.assertRaises(ValueError):
            parse(encode({b"added": b"x" * MAX_PEX}))

    def test_unroutable_addresses_are_filtered_and_public_source_cannot_redirect_locally(self):
        addresses = [("0.0.0.0", 1), ("224.0.0.1", 1), ("127.0.0.2", 0),
                     ("169.254.1.1", 1), ("8.8.4.4", 1), ("127.0.0.2", 1)]
        received = []
        session = PexSession(("8.8.8.8", 1), discover=received.extend)
        session.receive(encode({b"added": b"".join(compact(a) for a in addresses)}))
        self.assertEqual(received, [("8.8.4.4", 1)])

    def test_negotiation_ids_disable_renumber_and_do_not_collide(self):
        session = PexSession(("127.0.0.1", 1))
        for identifier in (-1, 256, b"2"):
            with self.subTest(identifier=identifier), self.assertRaises(ValueError):
                session.negotiate(encode({b"m": {b"ut_pex": identifier}}))
        session.negotiate(encode({b"m": {b"ut_pex": 7}}))
        session.negotiate(encode({b"m": {b"ut_metadata": 3}}))
        self.assertEqual(session.remote_id, 7)
        session.negotiate(encode({b"m": {b"ut_pex": 0}}))
        self.assertIsNone(session.outgoing())
        with self.assertRaises(ValueError):
            session.negotiate(encode({b"m": {b"ut_pex": 3}}), metadata_id=3)

    def test_receive_burst_budget_and_refill(self):
        now = [0]
        session = PexSession(("127.0.0.1", 1), clock=lambda: now[0])
        packet = encode({b"added": b""})
        session.receive(packet)
        session.receive(packet)
        with self.assertRaises(ValueError):
            session.receive(packet)
        now[0] = 60
        session.receive(packet)

    def test_announcements_are_connected_only_include_drops_and_respect_minute(self):
        now = [0]
        peers = [("127.0.0.2", 2)]
        session = PexSession(("127.0.0.1", 1), connected=lambda: peers, clock=lambda: now[0])
        session.negotiate(encode({b"m": {b"ut_pex": 7}}))
        packet = session.outgoing()
        self.assertEqual(packet[4:6], b"\x14\x07")
        self.assertEqual(parse(packet[6:]), ((peers[0],), ()))
        peers.clear()
        now[0] = 59.9
        self.assertIsNone(session.outgoing())
        now[0] = 60
        self.assertEqual(parse(session.outgoing()[6:]), ((), (("127.0.0.2", 2),)))
        self.assertEqual(session.sent, set())

    def test_outgoing_public_peer_does_not_receive_private_contacts(self):
        peers = [("127.0.0.2", 2), ("8.8.4.4", 1)]
        session = PexSession(("8.8.8.8", 1), connected=lambda: peers)
        session.negotiate(encode({b"m": {b"ut_pex": 9}}))
        self.assertEqual(parse(session.outgoing()[6:])[0], (("8.8.4.4", 1),))


class PexDiscoveryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.path = self.root / "source"
        self.data = bytes(range(251)) * 300
        self.path.write_bytes(self.data)
        self.torrent = create(self.path, self.root / "source.torrent", piece_length=32768)
        self.servers, self.sources, self.handlers = [], [], set()
        self.advertised = asyncio.Event()
        self.handshakes = []

    async def asyncTearDown(self):
        for server in self.servers:
            if isinstance(server, SeedServer):
                await server.close()
            else:
                server.close()
                await server.wait_closed()
        tasks = tuple(self.handlers)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        for source in self.sources:
            source.close()
        self.temp.cleanup()

    async def good_seed(self):
        source = FileSource(self.torrent, self.path)
        self.sources.append(source)
        server = SeedServer(self.torrent, source)
        self.servers.append(server)
        port = await server.start("127.0.0.2")
        return "127.0.0.2", port

    async def sharer(self, referrals):
        async def handle(reader, writer):
            task = asyncio.current_task()
            self.handlers.add(task)
            try:
                request = await reader.readexactly(68)
                self.assertEqual(request[28:48], self.torrent.info_hash)
                writer.write(PROTOCOL + RESERVED + self.torrent.info_hash + b"s" * 20)
                writer.write(extended(0, encode({b"m": {b"ut_metadata": 3, b"ut_pex": 7}})))
                writer.write(message(5, b"\x00"))
                writer.write(message(1))
                writer.write(extended(2, encode({b"added": b"".join(compact(a) for a in referrals)})))
                await writer.drain()
                self.advertised.set()
                while True:
                    size = struct.unpack("!I", await reader.readexactly(4))[0]
                    if not size:
                        continue
                    packet = await reader.readexactly(size)
                    if packet[:2] == b"\x14\x00":
                        self.handshakes.append(decode(packet[2:]))
            except (asyncio.IncompleteReadError, ConnectionError):
                pass
            finally:
                writer.close()
                try:
                    await writer.wait_closed()
                except OSError:
                    pass
                self.handlers.discard(task)
        server = await asyncio.start_server(handle, "127.0.0.1", 0)
        self.servers.append(server)
        return "127.0.0.1", server.sockets[0].getsockname()[1]

    async def test_pex_only_referral_wakes_scheduler_without_tracker_or_dht(self):
        good = await self.good_seed()
        first = await self.sharer([good])
        report = await asyncio.wait_for(download(self.torrent, [first], self.root / "out",
                                                use_trackers=False, use_dht=False,
                                                endgame=False, concurrency=2, timeout=1), 3)
        self.assertEqual((self.root / "out").read_bytes(), self.data)
        self.assertEqual(report["pex_peers"], 1)
        self.assertEqual(report["pex_messages_received"], 1)
        self.assertGreater(report["pex_received_bytes"], 0)
        self.assertEqual(report["peer_failures"], 0)
        self.assertEqual(report["verified_bytes"], len(self.data))

    async def test_disabled_or_private_torrents_neither_advertise_nor_use_pex(self):
        for private in (False, True):
            with self.subTest(private=private):
                if private:
                    info = decode(self.torrent.info_bytes)
                    info[b"private"] = 1
                    self.torrent = Torrent.from_bytes(encode({b"info": info}))
                good = await self.good_seed()
                first = await self.sharer([good])
                with self.assertRaises(DownloadError) as caught:
                    await download(self.torrent, [first], self.root / f"disabled-{private}",
                                   use_trackers=False, use_dht=False, use_pex=private,
                                   peer_retries=0, timeout=0.1)
                self.assertEqual(caught.exception.report["pex_peers"], 0)
                self.assertEqual(caught.exception.report["pex_messages_received"], 0)
                self.assertNotIn(b"ut_pex", self.handshakes[-1][b"m"])

    async def test_one_source_can_add_only_25_candidates(self):
        referrals = []
        def reject(reader, writer):
            writer.close()
        for i in range(2, 42):
            server = await asyncio.start_server(reject, f"127.0.0.{i}", 0)
            self.servers.append(server)
            referrals.append((f"127.0.0.{i}", server.sockets[0].getsockname()[1]))
        first = await self.sharer(referrals)
        with self.assertRaises(DownloadError) as caught:
            await asyncio.wait_for(download(self.torrent, [first], self.root / "out",
                                            use_trackers=False, peer_retries=0,
                                            concurrency=4, max_connections=4, timeout=0.1), 3)
        self.assertEqual(caught.exception.report["pex_peers"], 25)
        self.assertEqual(caught.exception.report["connections"], 26)

    async def test_cancellation_drains_pex_timer_and_discovery_waiter(self):
        first = await self.sharer([])
        task = asyncio.create_task(download(self.torrent, [first], self.root / "out", use_trackers=False))
        await asyncio.wait_for(self.advertised.wait(), 2)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        await asyncio.sleep(0.01)
        self.assertFalse(self.handlers)
        self.assertTrue((self.root / "out.part").exists())
