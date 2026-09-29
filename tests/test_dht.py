"""Loopback-only protocol, adversarial input, lifecycle and discovery tests."""
import asyncio
import contextlib
import io
import socket
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from cbtorrent.bencode import decode, encode
from cbtorrent.client import DownloadError, download
from cbtorrent.dht import (DhtNode, DhtDiscovery, RoutingTable, compact_nodes,
                           compact_peer, parse_peer, xor_distance, MAX_PACKET,
                           MAX_HASHES, PEERS_PER_HASH, NODE_TTL, PEER_TTL, TOKEN_PERIOD)
from cbtorrent.metainfo import Torrent, create
from cbtorrent.policy import RecoveryPolicy
from cbtorrent.seeder import FileSource, SeedServer


class FakeNode(asyncio.DatagramProtocol):
    """Independent KRPC responder; can hold or forge individual replies."""
    def __init__(self, handler=None):
        self.handler = handler
        self.queries = []
        self.arrived = asyncio.Event()
        self.sent = self.received = 0

    def connection_made(self, transport):
        self.transport = transport
        self.address = transport.get_extra_info("sockname")[:2]

    def datagram_received(self, data, addr):
        self.received += len(data)
        msg = decode(data)
        self.queries.append((msg, addr))
        self.arrived.set()
        if self.handler:
            result = self.handler(msg, addr)
            if result is not None:
                self.respond(msg, addr, result)

    def respond(self, msg, addr, result):
        self.send(encode({b"t": msg[b"t"], b"y": b"r", b"r": result}), addr)

    def send(self, raw, addr):
        self.sent += len(raw)
        self.transport.sendto(raw, addr)


class AsyncFixtures(unittest.IsolatedAsyncioTestCase):
    async def node(self, **kwargs):
        node = DhtNode(bootstrap=(), bind_host="127.0.0.1", **kwargs)
        await node.start()
        self.addAsyncCleanup(node.close)
        return node

    async def fake(self, handler=None):
        server = FakeNode(handler)
        transport, _ = await asyncio.get_running_loop().create_datagram_endpoint(
            lambda: server, local_addr=("127.0.0.1", 0))
        self.addCleanup(transport.close)
        return server


class ContactTests(unittest.TestCase):
    def test_compact_contacts_and_invalid_lengths(self):
        peer = ("127.0.0.1", 6881)
        self.assertEqual(parse_peer(compact_peer(peer)), peer)
        self.assertEqual(compact_nodes(b"n" * 20 + compact_peer(peer)), [(b"n" * 20, *peer)])
        for raw in (b"", b"x" * 5, b"x" * 7, socket.inet_aton("0.0.0.0") + b"\x00\x01",
                    socket.inet_aton("224.0.0.1") + b"\x00\x01", b"\x7f\x00\x00\x01\x00\x00"):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                parse_peer(raw)
        with self.assertRaises(ValueError):
            compact_nodes(b"x" * 27)
        self.assertEqual(xor_distance(bytes(20), (1).to_bytes(20, "big")), 1)
        with self.assertRaises(ValueError):
            xor_distance(b"short", b"short")

    def test_routing_retains_good_contacts_and_is_bounded(self):
        time = [0]
        table = RoutingTable(bytes(20), lambda: time[0])
        for i in range(1, 2000):
            table.add((i << 148).to_bytes(20, "big"), ("127.0.0.1", i))
        self.assertLessEqual(len(table.buckets), 32)
        self.assertTrue(all(len(bucket) <= 8 for _, _, bucket in table.buckets))
        before = table.closest(bytes(20), 256)
        self.assertLessEqual(len(before), 256)
        contact = before[0]
        table.add(contact.node_id, ("127.0.0.2", 5555))
        self.assertEqual(table.closest(bytes(20), 256), before)
        table.add(b"x" * 20, contact.address)
        self.assertEqual(sum(c.address == contact.address for c in table.closest(bytes(20), 256)), 1)
        time[0] = NODE_TTL
        self.assertEqual(table.closest(bytes(20)), [])

    def test_metadata_private_flag_and_nodes_preserve_info_hash(self):
        root = {b"info": {b"name": b"x", b"length": 0, b"piece length": 1, b"pieces": b""}}
        original = Torrent.from_bytes(encode(root))
        root[b"nodes"] = [[b"localhost", 6881]]
        parsed = Torrent.from_bytes(encode(root))
        self.assertEqual(parsed.nodes, (("localhost", 6881),))
        self.assertEqual(parsed.info_hash, original.info_hash)
        root[b"info"][b"private"] = 1
        self.assertTrue(Torrent.from_bytes(encode(root)).private)
        for value in (b"1", 2, -1):
            root[b"info"][b"private"] = value
            with self.assertRaises(ValueError):
                Torrent.from_bytes(encode(root))
        root[b"info"][b"private"] = 0
        for nodes in (b"bad", [[b"localhost", 0]], [[b"host", 1]] * 9, [[3, 1]]):
            root[b"nodes"] = nodes
            with self.assertRaises(ValueError):
                Torrent.from_bytes(encode(root))

    def test_configuration_bounds(self):
        for args in ({"query_timeout": 0}, {"query_timeout": float("nan")},
                     {"max_inflight": 0}, {"max_inflight": 33}, {"node_id": b"short"},
                     {"bootstrap": [("host", 1)] * 9}, {"bind_port": -1}):
            with self.subTest(args=args), self.assertRaises(ValueError):
                DhtNode(**args)


class DatagramTests(AsyncFixtures):
    async def test_ping_find_node_errors_and_exact_udp_accounting(self):
        server, client = await self.node(), await self.node()
        address = ("127.0.0.1", server.port)
        response = await client.query(address, b"ping", {})
        self.assertEqual(response[b"id"], server.node_id)
        response = await client.query(address, b"find_node", {b"target": client.node_id})
        self.assertEqual(response[b"nodes"], b"")  # unsolicited query is not a confirmed contact
        with self.assertRaisesRegex(ValueError, "204"):
            await client.query(address, b"unknown", {})
        with self.assertRaisesRegex(ValueError, "203"):
            await client.query(address, b"get_peers", {b"info_hash": b"short"})
        self.assertEqual(client.metrics.dht_sent_bytes, server.metrics.dht_received_bytes)
        self.assertEqual(client.metrics.dht_received_bytes, server.metrics.dht_sent_bytes)
        self.assertEqual(client.metrics.dht_requests, 4)
        self.assertEqual(client.metrics.dht_failures, 2)
        self.assertEqual(client.table.closest(bytes(20))[0].address, address)

    async def test_wrong_source_tid_and_malformed_reply_cannot_resolve_query(self):
        client = await self.node(query_timeout=1)
        remote, attacker = await self.fake(), await self.fake()
        task = asyncio.create_task(client.query(remote.address, b"ping", {}))
        await asyncio.wait_for(remote.arrived.wait(), 1)
        msg, address = remote.queries[0]
        attacker.respond(msg, address, {b"id": b"a" * 20})
        wrong = dict(msg)
        wrong[b"t"] = b"wrong"
        remote.respond(wrong, address, {b"id": b"a" * 20})
        remote.respond(msg, address, {b"id": b"short"})
        remote.send(b"x" * (MAX_PACKET + 1), address)
        remote.send(b"l" * 70 + b"e" * 70, address)
        # A later real reply must be the one that resolves the pending transaction.
        remote.respond(msg, address, {b"id": b"r" * 20})
        self.assertEqual((await task)[b"id"], b"r" * 20)
        self.assertEqual([c.node_id for c in client.table.closest(bytes(20))], [b"r" * 20])
        remote.respond(msg, address, {b"id": b"a" * 20})  # late duplicate is harmless

    async def test_tokens_are_secret_ip_hash_bound_and_expire(self):
        time = [TOKEN_PERIOD * 10]
        server = await self.node(clock=lambda: time[0])
        client = await self.node()
        address, target = ("127.0.0.1", server.port), b"h" * 20
        result = await client.query(address, b"get_peers", {b"info_hash": target})
        token = result[b"token"]
        args = {b"info_hash": target, b"token": token, b"port": 6881}
        for bad in ({**args, b"token": b"forged"}, {**args, b"info_hash": b"z" * 20}):
            with self.assertRaisesRegex(ValueError, "203"):
                await client.query(address, b"announce_peer", bad)
        with self.assertRaises(ValueError):
            server._answer(b"announce_peer", {**args, b"id": client.node_id}, ("127.0.0.2", client.port))
        await client.query(address, b"announce_peer", args)
        result = await client.query(address, b"get_peers", {b"info_hash": target})
        self.assertEqual(result[b"values"], [compact_peer(("127.0.0.1", 6881))])
        time[0] += TOKEN_PERIOD
        await client.query(address, b"announce_peer", {**args, b"implied_port": 1, b"port": 0})
        self.assertIn(("127.0.0.1", client.port), server._peers[target])
        time[0] += TOKEN_PERIOD
        with self.assertRaisesRegex(ValueError, "203"):
            await client.query(address, b"announce_peer", args)
        time[0] += PEER_TTL
        result = await client.query(address, b"get_peers", {b"info_hash": target})
        self.assertNotIn(b"values", result)
        self.assertFalse(server._peers)

    async def test_peer_store_and_reply_rate_are_bounded(self):
        server = await self.node(clock=lambda: 0)
        address = ("127.0.0.1", 5555)
        for i in range(MAX_HASHES):
            target = i.to_bytes(20, "big")
            args = {b"id": b"c" * 20, b"info_hash": target,
                    b"token": server._token(address[0], target, 0), b"port": 6881}
            server._answer(b"announce_peer", args, address)
        target = b"\xff" * 20
        with self.assertRaisesRegex(ValueError, "full"):
            server._answer(b"announce_peer", {**args, b"info_hash": target,
                           b"token": server._token(address[0], target, 0)}, address)
        target = bytes(20)
        for port in range(1, PEERS_PER_HASH):
            server._answer(b"announce_peer", {**args, b"info_hash": target,
                           b"token": server._token(address[0], target, 0), b"port": port}, address)
        with self.assertRaisesRegex(ValueError, "full"):
            server._answer(b"announce_peer", {**args, b"info_hash": target,
                           b"token": server._token(address[0], target, 0), b"port": 6000}, address)
        self.assertEqual(len(server._peers[target]), PEERS_PER_HASH)
        packet = encode({b"t": b"a", b"y": b"q", b"q": b"get_peers",
                         b"a": {b"id": b"c" * 20, b"info_hash": target}})
        with patch.object(server, "_send") as send:
            for _ in range(1000):
                server.receive(packet, address)
            self.assertEqual(send.call_count, 100)
            self.assertTrue(all(len(call.args[0]) <= MAX_PACKET for call in send.call_args_list))

    async def test_query_timeout_cancel_close_and_inflight_cap(self):
        remote = await self.fake()
        client = await self.node(query_timeout=0.15, max_inflight=2)
        tasks = [asyncio.create_task(client.query(remote.address, b"ping", {})) for _ in range(10)]
        await remote.arrived.wait()
        self.assertLessEqual(len(client._pending), 2)
        tasks[0].cancel()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        self.assertIsInstance(results[0], asyncio.CancelledError)
        self.assertTrue(all(isinstance(r, TimeoutError) for r in results[1:]))
        self.assertFalse(client._pending)
        remote.arrived.clear()
        task = asyncio.create_task(client.query(remote.address, b"ping", {}))
        await remote.arrived.wait()
        await client.close()
        with self.assertRaises(OSError):
            await task
        self.assertFalse(client._pending)
        self.assertEqual(client.port, 0)


class LookupTests(AsyncFixtures):
    async def test_referral_flood_obeys_query_concurrency_and_peer_bounds(self):
        node = await self.node()
        node.bootstrap_hosts = (("127.0.0.1", 1000),)
        counts = {"active": 0, "peak": 0, "queries": 0}
        async def reply(address, method, args):
            counts["active"] += 1
            counts["peak"] = max(counts["peak"], counts["active"])
            counts["queries"] += 1
            index = counts["queries"]
            try:
                await asyncio.sleep(0)
                return {b"id": index.to_bytes(20, "big"),
                        b"nodes": b"".join((index * 20 + j).to_bytes(20, "big")
                                           + compact_peer(("127.0.0.1", 1000 + index * 20 + j))
                                           for j in range(20)),
                        b"values": [compact_peer(("127.0.0.1", 10000 + index * 20 + j))
                                     for j in range(20)]}
            finally:
                counts["active"] -= 1
        found = []
        with patch.object(node, "query", side_effect=reply):
            peers = await node.discover(b"h" * 20, on_peers=found.extend)
        self.assertEqual(counts["queries"], 32)
        self.assertLessEqual(counts["peak"], 3)
        self.assertEqual(len(peers), 200)
        self.assertEqual(found, list(peers))
        self.assertEqual(node.metrics.dht_peers, 200)

    async def test_multihop_partial_failure_and_announce_to_tcp_port(self):
        target, peer = b"h" * 20, ("127.0.0.1", 6881)
        leaf = await self.fake(lambda msg, addr: {b"id": b"l" * 20, b"token": b"leaf-token",
                                                 b"values": [b"invalid", compact_peer(peer), compact_peer(peer)]})
        dead = await self.fake()
        root = await self.fake(lambda msg, addr: {b"id": b"r" * 20, b"token": b"root-token",
                                                 b"nodes": b"l" * 20 + compact_peer(leaf.address)
                                                 + b"d" * 20 + compact_peer(dead.address)})
        node = await self.node(query_timeout=0.1)
        node.bootstrap_hosts = (root.address,)
        found = []
        peers = await node.discover(target, port=4567, timeout=2, on_peers=found.extend)
        self.assertEqual(peers, (peer,))
        self.assertEqual(found, [peer])
        self.assertGreater(node.metrics.dht_failures, 0)
        announces = [m for m, _ in leaf.queries if m[b"q"] == b"announce_peer"]
        self.assertEqual(len(announces), 1)
        self.assertEqual(announces[0][b"a"][b"port"], 4567)
        self.assertEqual(announces[0][b"a"][b"token"], b"leaf-token")
        self.assertNotIn(dead.address, [c.address for c in node.table.closest(target)])

    async def test_partial_results_survive_lookup_deadline(self):
        dead = await self.fake()
        peer = ("127.0.0.1", 6789)
        root = await self.fake(lambda msg, addr: {b"id": b"r" * 20, b"values": [compact_peer(peer)],
                                                 b"nodes": b"d" * 20 + compact_peer(dead.address)})
        node = await self.node(query_timeout=1)
        node.bootstrap_hosts = (root.address,)
        result = await node.discover(b"h" * 20, timeout=0.1)
        self.assertEqual(result, (peer,))
        self.assertFalse(node._pending)

    async def test_cancellation_drains_lookup_queries(self):
        remote = await self.fake()
        node = await self.node(query_timeout=10)
        node.bootstrap_hosts = (remote.address,)
        task = asyncio.create_task(node.discover(b"h" * 20, timeout=30))
        await remote.arrived.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertFalse(node._pending)

    async def test_dns_timeout_is_bounded_and_empty_bootstrap_stays_offline(self):
        node = await self.node(query_timeout=0.05)
        entered = asyncio.Event()
        async def stuck(*args, **kwargs):
            entered.set()
            await asyncio.Future()
        node.bootstrap_hosts = (("test.invalid", 6881),)
        with patch.object(asyncio.get_running_loop(), "getaddrinfo", side_effect=stuck):
            self.assertEqual(await asyncio.wait_for(node.discover(b"h" * 20, timeout=0.1), 1), ())
        self.assertTrue(entered.is_set())
        node.bootstrap_hosts = ()
        with patch.object(asyncio.get_running_loop(), "getaddrinfo", side_effect=AssertionError("DNS")):
            self.assertEqual(await node.discover(b"h" * 20), ())


class DownloadTests(AsyncFixtures):
    async def asyncSetUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.data = b"verified DHT download\x00\xff" * 3000
        self.source = self.root / "source"
        self.source.write_bytes(self.data)
        self.path = self.root / "file.torrent"
        self.torrent = create(self.source, self.path, piece_length=16384)
        self.output = self.root / "out"

    async def seed(self, corrupt=False):
        source = FileSource(self.torrent, self.source)
        self.addCleanup(source.close)
        if corrupt:
            read = source.read
            source.read = lambda *args: bytes(b ^ 255 for b in read(*args))
        server = SeedServer(self.torrent, source)
        self.addAsyncCleanup(server.close)
        return "127.0.0.1", await server.start()

    async def test_trackerless_download_rejects_corruption_and_trains_verified_data(self):
        bad, good = await self.seed(corrupt=True), await self.seed()
        remote = await self.fake(lambda msg, addr: {b"id": b"r" * 20,
                                                   b"values": [compact_peer(bad), compact_peer(good)]})
        policy = RecoveryPolicy()
        report = await asyncio.wait_for(download(
            replace(self.torrent, nodes=(remote.address,)), [], self.output,
            use_trackers=False, use_dht=True, timeout=1, concurrency=1, policy=policy), 5)
        self.assertEqual(self.output.read_bytes(), self.data)
        self.assertEqual(report["hash_failures"], 1)
        self.assertNotIn(bad, policy.models)
        self.assertEqual(policy.verified_bytes, len(self.data))
        self.assertEqual(report["dht_peers"], 2)
        self.assertGreater(report["dht_sent_bytes"], 0)
        self.assertEqual(report["dht_received_bytes"], remote.sent)
        self.assertEqual(report["tracker_requests"], 0)

    async def test_private_disabled_and_complete_resume_never_start_dht(self):
        peer = await self.seed()
        for name, torrent, use_dht in (("private", replace(self.torrent, private=True), True),
                                      ("disabled", self.torrent, False)):
            with patch("cbtorrent.client.DhtDiscovery", side_effect=AssertionError("DHT started")):
                report = await download(torrent, [peer], self.root / name,
                                        use_dht=use_dht, use_trackers=False)
                self.assertEqual(report["dht_requests"], 0)
        self.output.with_name("out.part").write_bytes(self.data)
        with patch("cbtorrent.client.DhtDiscovery", side_effect=AssertionError("DHT started")):
            report = await download(self.torrent, [], self.output, resume=True, use_dht=True)
        self.assertTrue(report["complete"])

    async def test_explicit_peer_completes_while_dht_is_stalled(self):
        peer = await self.seed()
        remote = await self.fake()
        report = await asyncio.wait_for(download(
            self.torrent, [peer], self.output, use_trackers=False, use_dht=True,
            dht_bootstrap=[remote.address], timeout=10), 2)
        self.assertTrue(report["complete"])
        self.assertEqual(self.output.read_bytes(), self.data)

    async def test_discovered_peer_wakes_scheduler_while_other_peer_stalls(self):
        good = await self.seed()
        connected = asyncio.Event()
        closed = asyncio.Event()
        async def stall(reader, writer):
            try:
                await reader.readexactly(68)
                connected.set()
                await reader.read()
            finally:
                writer.close()
                await writer.wait_closed()
                closed.set()
        server = await asyncio.start_server(stall, "127.0.0.1", 0)
        self.addCleanup(server.close)
        self.addAsyncCleanup(server.wait_closed)
        remote = await self.fake()
        task = asyncio.create_task(download(
            self.torrent, [("127.0.0.1", server.sockets[0].getsockname()[1])], self.output,
            use_trackers=False, use_dht=True, dht_bootstrap=[remote.address],
            timeout=10, piece_timeout=20, concurrency=2))
        try:
            await asyncio.wait_for(connected.wait(), 2)
            await asyncio.wait_for(remote.arrived.wait(), 2)
            msg, addr = remote.queries[0]
            remote.respond(msg, addr, {b"id": b"r" * 20, b"values": [compact_peer(good)]})
            report = await asyncio.wait_for(task, 2)
            self.assertTrue(report["complete"])
            self.assertEqual(report["peer_failures"], 0)
            self.assertEqual(self.output.read_bytes(), self.data)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            server.close()
            await server.wait_closed()
        await asyncio.wait_for(closed.wait(), 1)

    async def test_no_peers_failure_and_cancel_preserve_partial_and_close_socket(self):
        remote = await self.fake()
        with self.assertRaises(DownloadError) as caught:
            await asyncio.wait_for(download(self.torrent, [], self.output, use_trackers=False,
                                           use_dht=True, dht_bootstrap=[remote.address], timeout=0.1), 2)
        self.assertFalse(caught.exception.report["complete"])
        self.assertGreater(caught.exception.report["dht_failures"], 0)
        self.assertTrue(self.output.with_name("out.part").exists())
        self.assertFalse(self.output.exists())
        instances = []
        def factory(*args, **kwargs):
            discovery = DhtDiscovery(*args, **kwargs)
            instances.append(discovery)
            return discovery
        remote.arrived.clear()
        with patch("cbtorrent.client.DhtDiscovery", side_effect=factory):
            task = asyncio.create_task(download(self.torrent, [], self.root / "cancelled",
                                               use_trackers=False, use_dht=True,
                                               dht_bootstrap=[remote.address], timeout=10))
            await remote.arrived.wait()
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertTrue(instances[0].task.done())
        self.assertFalse(instances[0].node._pending)
        self.assertEqual(instances[0].node.port, 0)

    async def test_seed_command_announces_verified_file_and_cancels(self):
        from cbtorrent.__main__ import build_parser, seed_file
        announced = asyncio.Event()
        def response(msg, addr):
            if msg[b"q"] == b"announce_peer":
                announced.set()
            return {b"id": b"r" * 20, b"token": b"token", b"nodes": b""}
        remote = await self.fake(response)
        args = build_parser().parse_args(["seed", str(self.path), "--file", str(self.source),
                                          "--listen-host", "127.0.0.1", "--port", "0", "--no-trackers",
                                          "--dht-bootstrap", f"127.0.0.1:{remote.address[1]}"])
        with contextlib.redirect_stdout(io.StringIO()):
            task = asyncio.create_task(seed_file(args))
            try:
                await asyncio.wait_for(announced.wait(), 2)
                query = next(m for m, _ in remote.queries if m[b"q"] == b"announce_peer")
                port = query[b"a"][b"port"]
                self.assertEqual(query[b"a"][b"info_hash"], self.torrent.info_hash)
                report = await download(self.torrent, [("127.0.0.1", port)], self.output, use_trackers=False)
                self.assertTrue(report["complete"])
            finally:
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
        self.assertEqual(self.output.read_bytes(), self.data)

    async def test_create_embeds_only_explicit_bootstrap_nodes(self):
        created = create(self.source, self.root / "nodes.torrent", nodes=[("localhost", 1234)])
        self.assertEqual(Torrent.load(self.root / "nodes.torrent").nodes, (("localhost", 1234),))
        self.assertEqual(created.nodes, (("localhost", 1234),))
        self.assertNotIn(b"nodes", decode(self.path.read_bytes()))


class CliTests(unittest.TestCase):
    def test_cli_and_gui_dht_flags(self):
        from cbtorrent.__main__ import build_parser, main
        parser = build_parser()
        for argv in (["gui"], ["seed", "a", "--file", "b"], ["download", "a", "--output", "b"]):
            self.assertFalse(parser.parse_args(argv).no_dht)
            args = parser.parse_args([*argv, "--no-dht", "--dht-bootstrap", "127.0.0.1:1234"])
            self.assertTrue(args.no_dht)
            self.assertEqual(args.dht_bootstrap, [("127.0.0.1", 1234)])
        with patch("cbtorrent.gui.run") as gui:
            main(["gui", "--no-dht", "--dht-bootstrap", "127.0.0.1:1234"])
        self.assertFalse(gui.call_args.kwargs["use_dht"])
        self.assertEqual(gui.call_args.kwargs["dht_bootstrap"], [("127.0.0.1", 1234)])


if __name__ == "__main__":
    unittest.main()
