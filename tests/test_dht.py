"""Deterministic local UDP tests for BEP 5 DHT peer discovery."""
import asyncio
import os
import socket
import struct
import tempfile
import unittest
from pathlib import Path

from cbtorrent.bencode import decode, encode
from cbtorrent.client import download
from cbtorrent.dht import (
    DhtNode,
    compact_nodes,
    encode_compact_nodes,
    encode_compact_peers,
    sha1_token,
    xor_distance,
)
from cbtorrent.metainfo import create
from cbtorrent.seeder import FileSource, SeedServer
from cbtorrent.tracker import compact_peers


class EncodingTests(unittest.TestCase):
    def test_compact_nodes_round_trip(self):
        node_id = b"n" * 20
        raw = encode_compact_nodes([(node_id, "127.0.0.1", 6881)])
        self.assertEqual(len(raw), 26)
        self.assertEqual(compact_nodes(raw), [(node_id, "127.0.0.1", 6881)])

    def test_compact_peers_helper(self):
        raw = encode_compact_peers([("10.0.0.1", 51413)])
        self.assertEqual(compact_peers(raw), [("10.0.0.1", 51413)])

    def test_xor_distance_ordering(self):
        a, b, c = b"\x00" * 20, b"\x00" * 19 + b"\x01", b"\xff" * 20
        self.assertLess(xor_distance(a, b), xor_distance(a, c))

    def test_krpc_ping_encode_decode(self):
        packet = encode({
            b"t": b"aa", b"y": b"q", b"q": b"ping",
            b"a": {b"id": b"x" * 20},
        })
        message = decode(packet)
        self.assertEqual(message[b"q"], b"ping")
        self.assertEqual(message[b"a"][b"id"], b"x" * 20)

    def test_get_peers_response_values_and_nodes(self):
        peer_raw = encode_compact_peers([("127.0.0.1", 6881)])
        nodes_raw = encode_compact_nodes([(b"a" * 20, "127.0.0.2", 6882)])
        packet = encode({
            b"t": b"bb", b"y": b"r",
            b"r": {b"id": b"b" * 20, b"token": b"tok", b"values": [peer_raw],
                   b"nodes": nodes_raw},
        })
        response = decode(packet)[b"r"]
        self.assertEqual(compact_peers(response[b"values"][0]), [("127.0.0.1", 6881)])
        self.assertEqual(compact_nodes(response[b"nodes"])[0][1:], ("127.0.0.2", 6882))


class FakeDhtServer:
    """Minimal datagram peer that answers get_peers with a fixed peer list."""

    def __init__(self, node_id, info_hash, peers, *, closer_nodes=()):
        self.node_id = node_id
        self.info_hash = info_hash
        self.peers = peers
        self.closer_nodes = closer_nodes
        self.transport = None
        self.queries = []

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        try:
            message = decode(data, max_size=65536)
        except ValueError:
            return
        if not isinstance(message, dict) or message.get(b"y") != b"q":
            return
        method = message.get(b"q")
        tid = message.get(b"t")
        args = message.get(b"a") or {}
        self.queries.append(method)
        if method == b"ping":
            reply = {b"t": tid, b"y": b"r", b"r": {b"id": self.node_id}}
        elif method == b"find_node":
            reply = {
                b"t": tid, b"y": b"r",
                b"r": {
                    b"id": self.node_id,
                    b"nodes": encode_compact_nodes(
                        self.closer_nodes or [(self.node_id, addr[0], self.port)]),
                },
            }
        elif method == b"get_peers":
            token = sha1_token(self.node_id, addr[0])
            body = {b"id": self.node_id, b"token": token}
            if args.get(b"info_hash") == self.info_hash:
                body[b"values"] = [encode_compact_peers([peer]) for peer in self.peers]
            else:
                body[b"nodes"] = encode_compact_nodes(
                    self.closer_nodes or [(self.node_id, "127.0.0.1", self.port)])
            reply = {b"t": tid, b"y": b"r", b"r": body}
        elif method == b"announce_peer":
            reply = {b"t": tid, b"y": b"r", b"r": {b"id": self.node_id}}
        else:
            return
        self.transport.sendto(encode(reply), addr)

    @property
    def port(self):
        return self.transport.get_extra_info("sockname")[1]

    def error_received(self, exc):
        pass

    def connection_lost(self, exc):
        pass


class DhtNodeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.info_hash = os.urandom(20)
        self.peer = ("127.0.0.1", 51413)
        self.remote_id = b"r" * 20
        self.fake = FakeDhtServer(self.remote_id, self.info_hash, [self.peer])
        loop = asyncio.get_running_loop()
        self.transport, _ = await loop.create_datagram_endpoint(
            lambda: self.fake, local_addr=("127.0.0.1", 0))
        self.bootstrap = [("127.0.0.1", self.fake.port)]

    async def asyncTearDown(self):
        self.transport.close()

    async def test_get_peers_from_injected_bootstrap(self):
        node = DhtNode(bootstrap=self.bootstrap, bind_host="127.0.0.1", query_timeout=1.0)
        try:
            await node.start()
            await node.bootstrap(timeout=3.0)
            peers = await node.get_peers(self.info_hash, timeout=3.0)
            self.assertIn(self.peer, peers)
            self.assertGreaterEqual(node.requests, 1)
        finally:
            await node.close()

    async def test_serves_ping_and_stores_announce(self):
        server = DhtNode(bootstrap=(), bind_host="127.0.0.1", node_id=b"s" * 20)
        client = DhtNode(bootstrap=(), bind_host="127.0.0.1", node_id=b"c" * 20)
        try:
            sport = await server.start()
            await client.start()
            response = await client._query("127.0.0.1", sport, b"ping", {b"id": client.node_id})
            self.assertEqual(response[b"r"][b"id"], b"s" * 20)
            # Warm token via get_peers, then announce
            await client._query("127.0.0.1", sport, b"get_peers", {
                b"id": client.node_id, b"info_hash": self.info_hash,
            })
            await client._query("127.0.0.1", sport, b"announce_peer", {
                b"id": client.node_id, b"info_hash": self.info_hash,
                b"port": 9999, b"implied_port": 0,
                b"token": client._tokens[("127.0.0.1", sport)],
            })
            self.assertIn(("127.0.0.1", 9999), server._peer_store[self.info_hash])
        finally:
            await client.close()
            await server.close()


class ClientDhtTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.data = b"dht-client-integration\n" * 200
        source = self.root / "source.bin"
        source.write_bytes(self.data)
        self.torrent_path = self.root / "test.torrent"
        self.torrent = create(source, self.torrent_path, piece_length=16 * 1024)
        self.output = self.root / "out.bin"

    async def asyncTearDown(self):
        self.temp.cleanup()

    async def test_disabled_dht_does_not_start_node(self):
        # Seed + download with use_dht=False; metrics stay zero.
        seed_source = FileSource(self.torrent, self.root / "source.bin")
        server = SeedServer(self.torrent, seed_source, max_clients=4)
        try:
            port = await server.start("127.0.0.1", 0)
            report = await download(
                self.torrent, [("127.0.0.1", port)], self.output,
                use_trackers=False, use_dht=False, listen_host="127.0.0.1")
            self.assertTrue(report["complete"])
            self.assertEqual(report["dht_requests"], 0)
            self.assertEqual(report["dht_peers"], 0)
            self.assertEqual(report["dht_errors"], [])
        finally:
            await server.close()
            seed_source.close()

    async def test_dht_discovers_peer_via_fake_bootstrap(self):
        seed_source = FileSource(self.torrent, self.root / "source.bin")
        server = SeedServer(self.torrent, seed_source, max_clients=4)
        fake = None
        transport = None
        try:
            listen = await server.start("127.0.0.1", 0)
            peer = ("127.0.0.1", listen)
            fake = FakeDhtServer(b"f" * 20, self.torrent.info_hash, [peer])
            loop = asyncio.get_running_loop()
            transport, _ = await loop.create_datagram_endpoint(
                lambda: fake, local_addr=("127.0.0.1", 0))
            report = await download(
                self.torrent, [], self.output, use_trackers=False, use_dht=True,
                listen_host="127.0.0.1",
                dht_bootstrap=[("127.0.0.1", fake.port)],
                timeout=5.0, piece_timeout=20.0)
            self.assertTrue(report["complete"])
            self.assertGreaterEqual(report["dht_peers"], 1)
            self.assertEqual(self.output.read_bytes(), self.data)
        finally:
            if transport is not None:
                transport.close()
            await server.close()
            seed_source.close()


class CliDhtFlagTests(unittest.TestCase):
    def test_no_dht_flag_on_download_and_gui(self):
        from cbtorrent.__main__ import build_parser
        parser = build_parser()
        download_args = parser.parse_args([
            "download", "t.torrent", "--output", "o", "--no-dht", "--no-trackers",
        ])
        self.assertTrue(download_args.no_dht)
        gui_args = parser.parse_args(["gui", "--no-dht"])
        self.assertTrue(gui_args.no_dht)
        seed_args = parser.parse_args([
            "seed", "t.torrent", "--file", "f", "--no-dht",
        ])
        self.assertTrue(seed_args.no_dht)


if __name__ == "__main__":
    unittest.main()
