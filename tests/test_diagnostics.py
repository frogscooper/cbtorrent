"""Readable failure reports, DHT error samples, lookup convergence, quiet resets."""
import asyncio
import socket
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from cbtorrent.bencode import decode, encode
from cbtorrent.client import DownloadError, download
from cbtorrent.dht import MAX_CANDIDATES, MAX_PACKET, MAX_QUERIES, DhtNode, compact_peer
from cbtorrent.diagnostics import describe_error, install_reset_filter
from cbtorrent.metainfo import create
from cbtorrent.metrics import Metrics
from cbtorrent.wire import PROTOCOL, Peer, message

from test_dht import AsyncFixtures


class DescribeErrorTests(unittest.TestCase):
    def test_messages_are_never_empty(self):
        self.assertEqual(describe_error(TimeoutError()), "timed out")
        self.assertEqual(describe_error(asyncio.TimeoutError()), "timed out")
        self.assertEqual(describe_error(OSError()), "OSError")
        self.assertEqual(describe_error(ConnectionResetError(10054, "forcibly closed")),
                         "[Errno 10054] forcibly closed")
        self.assertEqual(describe_error(asyncio.IncompleteReadError(b"xy", 68)),
                         "connection closed by peer (2 of 68 bytes read)")
        self.assertEqual(describe_error(ValueError("piece hash mismatch")), "piece hash mismatch")


class LenientKeyOrderTests(unittest.TestCase):
    def test_only_explicit_lenient_decoding_accepts_unsorted_keys(self):
        unsorted = b"d1:y1:r1:t2:aae"
        with self.assertRaises(ValueError):
            decode(unsorted)  # metainfo and other hashed data stay canonical
        self.assertEqual(decode(unsorted, sorted_keys=False), {b"y": b"r", b"t": b"aa"})
        for duplicate in (b"d1:ai1e1:ai2ee", b"d1:bi1e1:ai1e1:bi2ee"):
            with self.subTest(duplicate=duplicate), self.assertRaises(ValueError):
                decode(duplicate, sorted_keys=False)


class ResetFilterTests(unittest.IsolatedAsyncioTestCase):
    async def test_teardown_resets_are_quiet_and_other_errors_still_reported(self):
        loop = asyncio.get_running_loop()
        seen = []
        loop.set_exception_handler(lambda loop, context: seen.append(context))
        install_reset_filter(loop)
        install_reset_filter(loop)  # idempotent: one filter, previous handler kept

        def _call_connection_lost():
            raise ConnectionResetError(10054, "An existing connection was forcibly closed")

        def unrelated():
            raise ConnectionResetError(10054, "reset somewhere unexpected")

        for callback in (_call_connection_lost, unrelated):
            loop.call_soon(callback)
        await asyncio.sleep(0)
        loop.call_exception_handler({"message": "Task exception was never retrieved",
                                     "exception": ConnectionResetError(), "future": object()})
        loop.call_exception_handler({"message": "Exception in callback "
                                     "_ProactorBasePipeTransport._call_connection_lost()",
                                     "exception": ValueError("real bug")})
        self.assertEqual([str(c["exception"]) for c in seen],
                         ["[Errno 10054] reset somewhere unexpected", "", "real bug"])

    async def test_download_installs_filter(self):
        loop = asyncio.get_running_loop()
        loop.set_exception_handler(None)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "source").write_bytes(b"x")
            torrent = create(root / "source", root / "a.torrent", piece_length=16384)
            with self.assertRaises(DownloadError):
                await download(torrent, [], root / "out", use_trackers=False)
        self.assertTrue(getattr(loop.get_exception_handler(), "cbtorrent_reset_filter", False))


class ClosedWriter:
    def __init__(self):
        self.written = []

    def is_closing(self):
        return True

    def write(self, data):
        self.written.append(data)


class DroppedConnectionWriteTests(unittest.IsolatedAsyncioTestCase):
    async def test_no_writes_reach_a_dropped_connection(self):
        """Repeated writes after a drop made asyncio log "socket.send() raised exception"."""
        with tempfile.TemporaryDirectory() as directory:
            (Path(directory) / "source").write_bytes(b"x")
            torrent = create(Path(directory) / "source", Path(directory) / "a.torrent")
        writer, metrics = ClosedWriter(), Metrics()
        peer = Peer(None, writer, torrent, metrics, 1.0)
        peer.pending[0, 0] = 1
        peer.cancel_block(0, 0)
        with self.assertRaises(ConnectionResetError):
            await peer.send(message(2))
        self.assertEqual(writer.written, [])
        self.assertEqual((metrics.wire_sent_bytes, metrics.cancel_requests), (0, 0))


class PeerFailureReportTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        (self.root / "source").write_bytes(b"data" * 10000)
        self.torrent = create(self.root / "source", self.root / "a.torrent", piece_length=16384)

    async def stalled_peer(self, *, handshake, keepalive=None):
        """A peer that stops after the handshake, or after its bitfield (never unchokes)."""
        async def serve(reader, writer):
            try:
                await reader.readexactly(68)
                if handshake:
                    count = len(self.torrent.hashes)
                    bits = bytearray(b"\xff" * ((count + 7) // 8))
                    if count % 8:
                        bits[-1] &= 0xff << (8 - count % 8) & 0xff
                    writer.write(PROTOCOL + bytes(8) + self.torrent.info_hash + b"-XX0000-" + b"p" * 12
                                 + message(5, bytes(bits)))
                while keepalive:
                    writer.write(bytes(4))
                    await asyncio.sleep(keepalive)
                await reader.read()
            except (OSError, asyncio.IncompleteReadError):
                pass
            finally:
                writer.close()
        server = await asyncio.start_server(serve, "127.0.0.1", 0)
        self.addAsyncCleanup(server.wait_closed)
        self.addCleanup(server.close)
        return "127.0.0.1", server.sockets[0].getsockname()[1]

    async def test_timeouts_name_the_phase_and_limit(self):
        silent = await self.stalled_peer(handshake=False)
        choking = await self.stalled_peer(handshake=True)
        chatty = await self.stalled_peer(handshake=True, keepalive=0.05)
        with self.assertRaises(DownloadError) as caught:
            await asyncio.wait_for(download(
                self.torrent, [silent, choking, chatty], self.root / "out", use_trackers=False,
                timeout=0.2, piece_timeout=0.5, peer_retries=0, concurrency=3), 5)
        report = caught.exception.report
        self.assertEqual(report["peer_failure_phases"], {"handshake": 1, "unchoke": 2})
        # A silent socket hits the read timeout; keep-alives without an
        # unchoke run into the whole-attempt deadline instead.
        self.assertCountEqual(report["peer_errors"], [
            f"127.0.0.1:{silent[1]}: handshake: timed out after 0.2s",
            f"127.0.0.1:{choking[1]}: unchoke: timed out after 0.2s",
            f"127.0.0.1:{chatty[1]}: unchoke: timed out after 0.5s"])
        self.assertTrue(all(error.rsplit(": ", 1)[1] for error in report["peer_errors"]))


class DhtErrorTests(AsyncFixtures):
    async def test_download_reports_query_failures_and_empty_lookup(self):
        silent = await self.fake()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "source").write_bytes(b"x" * 100)
            torrent = create(root / "source", root / "a.torrent", piece_length=16384)
            with self.assertRaises(DownloadError) as caught:
                await asyncio.wait_for(download(
                    torrent, [], root / "out", use_trackers=False, use_dht=True,
                    dht_bootstrap=[silent.address], timeout=0.2), 5)
        report = caught.exception.report
        self.assertGreater(report["dht_failures"], 0)
        # With timeout <= 2s the query and lookup deadlines coincide.
        self.assertEqual(report["dht_errors"],
                         ["lookup deadline of 0.2s expired: 1 queries, 0 replies, 0 peers"])

    async def test_bootstrap_dns_failure_is_reported(self):
        node = await self.node(query_timeout=0.2)
        node.bootstrap_hosts = (("test.invalid", 6881),)
        with patch.object(asyncio.get_running_loop(), "getaddrinfo",
                          side_effect=socket.gaierror(11001, "getaddrinfo failed")):
            self.assertEqual(await node.discover(b"h" * 20, timeout=1), ())
        self.assertEqual(node.errors[0], "bootstrap test.invalid:6881: [Errno 11001] getaddrinfo failed")

    async def test_query_error_sample_is_bounded(self):
        node = await self.node(query_timeout=0.01)
        silent = await self.fake()
        for _ in range(30):
            with self.assertRaises(TimeoutError):
                await node.query(silent.address, b"ping", {})
        self.assertEqual(len(node.errors), 8)
        self.assertEqual(node.errors[0], f"ping 127.0.0.1:{silent.address[1]}: no reply within 0.01s")
        node.note("summary")
        self.assertEqual(node.errors[-1], "summary")


class LookupConvergenceTests(AsyncFixtures):
    async def test_closer_referral_displaces_farthest_unqueried_candidate(self):
        """A full shortlist must still accept the referral that leads to peers."""
        target, peer = bytes(20), ("127.0.0.1", 7000)
        near, near_id = ("127.0.0.1", 2), (1).to_bytes(20, "big")
        far = [((0xff << 152) - j).to_bytes(20, "big") for j in range(200)]
        issued, queried = [0], []

        def referrals():  # each reply names K new, slightly closer, far nodes
            start, issued[0] = issued[0], issued[0] + 8
            return b"".join(far[j] + compact_peer(("127.0.0.1", 3000 + j)) for j in range(start, start + 8))

        async def reply(address, method, args):
            queried.append(address)
            if address == near:
                return {b"id": near_id, b"values": [compact_peer(peer)]}
            node_id = b"e" * 20 if address[1] == 1 else far[address[1] - 3000]
            # The near node is named only once the shortlist is already full.
            nodes = referrals() if issued[0] < MAX_CANDIDATES + 16 else near_id + compact_peer(near)
            return {b"id": node_id, b"nodes": nodes}

        node = await self.node()
        node.bootstrap_hosts = (("127.0.0.1", 1),)
        with patch.object(node, "query", side_effect=reply):
            self.assertEqual(await node.discover(target, timeout=5), (peer,))
        self.assertIn(near, queried)
        self.assertLessEqual(len(queried), MAX_QUERIES)

    async def test_dead_node_does_not_hold_back_the_next_hop(self):
        """Lockstep rounds would wait out the dead node's timeout before hop two."""
        peer = ("127.0.0.1", 7001)
        leaf = await self.fake(lambda msg, addr: {b"id": b"l" * 20, b"values": [compact_peer(peer)]})
        middle = await self.fake(lambda msg, addr: {b"id": b"m" * 20,
                                                   b"nodes": b"l" * 20 + compact_peer(leaf.address)})
        dead = await self.fake()
        root = await self.fake(lambda msg, addr: {b"id": b"r" * 20, b"nodes":
                                                 b"d" * 20 + compact_peer(dead.address)
                                                 + b"m" * 20 + compact_peer(middle.address)})
        node = await self.node(query_timeout=5)
        node.bootstrap_hosts = (root.address,)
        self.assertEqual(await node.discover(b"h" * 20, timeout=1), (peer,))
        self.assertFalse(node._pending)

    async def test_dead_bootstrap_nodes_release_their_slots(self):
        """Three silent routers queried first must not idle the lookup for a full timeout."""
        peer = ("127.0.0.1", 7002)
        dead = [await self.fake() for _ in range(3)]
        live = await self.fake(lambda msg, addr: {b"id": b"r" * 20, b"values": [compact_peer(peer)]})
        node = await self.node(query_timeout=5)
        node.bootstrap_hosts = tuple(d.address for d in dead) + (live.address,)
        with patch("cbtorrent.dht.SLOW_QUERY", 0.1):
            self.assertEqual(await node.discover(b"h" * 20, timeout=1), (peer,))
        self.assertFalse(node._pending)

    async def test_degenerate_router_reply_falls_back_to_find_node(self):
        """Seen live: a router named one silent address under eight node IDs."""
        peer = ("127.0.0.1", 7003)
        sybil = await self.fake()
        holder = await self.fake(lambda msg, addr: {b"id": b"p" * 20, b"values": [compact_peer(peer)]})
        def router(msg, addr):
            if msg[b"q"] == b"find_node":
                return {b"id": b"r" * 20, b"nodes": b"p" * 20 + compact_peer(holder.address)}
            return {b"id": b"r" * 20, b"nodes": b"".join(bytes([i]) * 20 + compact_peer(sybil.address)
                                                        for i in range(8))}
        root = await self.fake(router)
        node = await self.node(query_timeout=0.5)
        node.bootstrap_hosts = (root.address,)
        self.assertEqual(await node.discover(b"h" * 20, timeout=2), (peer,))
        methods = [m[b"q"] for m, _ in root.queries]
        self.assertEqual(methods, [b"get_peers", b"find_node"])
        self.assertEqual(root.queries[1][0][b"a"][b"target"], node.node_id)

    async def test_failed_find_node_fallback_keeps_get_peers_referrals(self):
        peer = ("127.0.0.1", 7005)
        holder = await self.fake(lambda msg, addr: {b"id": b"p" * 20, b"values": [compact_peer(peer)]})
        def router(msg, addr):  # one referral (triggers the fallback), then silence
            if msg[b"q"] == b"get_peers":
                return {b"id": b"r" * 20, b"nodes": b"p" * 20 + compact_peer(holder.address)}
        root = await self.fake(router)
        node = await self.node(query_timeout=0.2)
        node.bootstrap_hosts = (root.address,)
        self.assertEqual(await node.discover(b"h" * 20, timeout=2), (peer,))
        self.assertEqual([m[b"q"] for m, _ in root.queries], [b"get_peers", b"find_node"])

    async def test_unsorted_reply_keys_are_accepted(self):
        peer = ("127.0.0.1", 7004)
        root = await self.fake()
        node = await self.node(query_timeout=1)
        node.bootstrap_hosts = (root.address,)
        task = asyncio.create_task(node.discover(b"h" * 20, timeout=2))
        await asyncio.wait_for(root.arrived.wait(), 1)
        msg, addr = root.queries[0]
        values = b"l6:" + compact_peer(peer) + b"e"
        root.send(b"d1:rd6:values" + values + b"2:id20:" + b"r" * 20 + b"e1:t"
                  + str(len(msg[b"t"])).encode() + b":" + msg[b"t"] + b"1:y1:re", addr)
        self.assertEqual(await task, (peer,))

    async def test_reply_larger_than_send_limit_is_accepted(self):
        peers = [("127.0.0.1", 10000 + i) for i in range(150)]
        def reply(msg, addr):
            return {b"id": b"r" * 20, b"token": b"t" * 8, b"values": [compact_peer(p) for p in peers]}
        root = await self.fake(reply)
        node = await self.node(query_timeout=1)
        node.bootstrap_hosts = (root.address,)
        found = await node.discover(b"h" * 20, timeout=2)
        self.assertGreater(root.sent, MAX_PACKET)
        self.assertEqual(found, tuple(peers))


if __name__ == "__main__":
    unittest.main()
