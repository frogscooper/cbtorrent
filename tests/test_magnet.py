"""Local-only metadata exchange, malformed peers, and complete magnet workflows."""
import asyncio
import base64
import json
import struct
import sys
import tempfile
import unittest
from dataclasses import replace
from hashlib import sha1
from pathlib import Path
from urllib.parse import quote
from unittest.mock import patch

from cbtorrent.bencode import decode, decode_prefix, encode
from cbtorrent.client import DownloadError
from cbtorrent.dht import DhtNode
from cbtorrent.extensions import (BLOCK, MAX_METADATA, MetadataServer, extended,
                                  metadata_message)
from cbtorrent.magnet import Magnet, download_magnet, fetch_metadata, resolve
from cbtorrent.metainfo import Torrent, create
from cbtorrent.metrics import Metrics
from cbtorrent.seeder import FileSource, SeedServer
from cbtorrent.session import Session


def uri(torrent):
    return "magnet:?xt=urn:btih:" + torrent.info_hash.hex()


class MagnetParsingTests(unittest.TestCase):
    def test_hex_base32_repeats_and_optional_fields(self):
        digest = bytes(range(20))
        tracker = "https://tracker.invalid/announce?token=a&b=c"
        for value in (digest.hex(), digest.hex().upper(), base64.b32encode(digest).decode().lower()):
            meta = Magnet.parse(f"magnet:?xt=urn:btih:{value}&dn=hello+world&tr={quote(tracker, safe='')}&x.pe=%5B::1%5D:42")
            self.assertEqual(meta.info_hash, digest)
            self.assertEqual(meta.name, "hello world")
            self.assertEqual(meta.trackers, (tracker,))
            self.assertEqual(meta.peers, (("::1", 42),))
        self.assertEqual(Magnet.parse(f"magnet:?xt=urn:btih:{digest.hex()}&xt=urn:btih:{digest.hex()}").info_hash, digest)

    def test_rejects_missing_conflicting_invalid_and_unbounded_input(self):
        good = "magnet:?xt=urn:btih:" + "11" * 20
        for value in ("magnet:", "https://example.org", "magnet:?xt=urn:btmh:abc", good + "#fragment",
                      good + "&xt=urn:btih:" + "22" * 20, "magnet:?xt=urn:btih:" + "!" * 32,
                      good + "&x.pe=host:0", good + "&tr=file:///tmp/a", good + "&tr=http://u:p@host/a",
                      good + "&x=1" * 128, good + "&dn=" + "x" * 16384,
                      good + "&tr=http://localhost/a" * 9):
            with self.subTest(value=value[:100]), self.assertRaises(ValueError):
                Magnet.parse(value)

    def test_decode_prefix_preserves_strict_full_decoder(self):
        raw = encode({b"piece": 0})
        self.assertEqual(decode_prefix(raw + b"raw"), ({b"piece": 0}, len(raw)))
        with self.assertRaises(ValueError):
            decode(raw + b"raw")

    def test_queue_roundtrip_safe_output_and_deduplicates_torrent(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source"
            source.write_bytes(b"hello")
            torrent = create(source, root / "a.torrent")
            session = Session(root / "session.json")
            link = uri(torrent) + "&dn=../../outside"
            item = session.add(link)
            self.assertEqual(item.output, Path("downloads") / torrent.info_hash.hex())
            with self.assertRaises(ValueError):
                session.add(root / "a.torrent")
            session.set_status(item.id, "paused")
            loaded = Session(session.path)
            self.assertEqual(loaded.load(), 1)
            self.assertEqual(loaded.get(item.id).magnet_uri, link)
            self.assertEqual(loaded.get(item.id).status, "paused")
            data = json.loads(session.path.read_text())
            data["items"][0]["id"] = "wrong"
            session.path.write_text(json.dumps(data))
            self.assertEqual(loaded.load(), 0)

    def test_bare_magnet_survives_cli_gui_dispatch(self):
        from cbtorrent.__main__ import main
        link = "magnet:?xt=urn:btih:" + "ab" * 20
        with patch("cbtorrent.gui.run", return_value=0) as run:
            self.assertEqual(main([link, "--no-dht"]), 0)
        self.assertEqual(run.call_args.args[0], link)


class MagnetTransferTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.source = self.root / "source"
        self.content = b"magnet testing\x00\xff" * 4000
        self.source.write_bytes(self.content)
        self.torrent = create(self.source, self.root / "a.torrent", piece_length=16384)
        self.output = self.root / "out"
        self.magnet = Magnet.parse(uri(self.torrent))
        self.servers, self.sources, self.tasks = [], [], set()
        self.script_errors = []

    async def asyncTearDown(self):
        for server in self.servers:
            if isinstance(server, SeedServer):
                await server.close()
            else:
                server.close()
                await server.wait_closed()
        tasks = tuple(self.tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        for source in self.sources:
            source.close()
        self.temp.cleanup()
        self.assertEqual(self.script_errors, [])

    async def seed(self, torrent=None, path=None, **kwargs):
        torrent = torrent or self.torrent
        source = FileSource(torrent, path or self.source)
        self.sources.append(source)
        server = SeedServer(torrent, source, **kwargs)
        self.servers.append(server)
        return "127.0.0.1", await server.start()

    async def scripted(self, *, fault=None, raw=None, size=None, fragment=False, gate=None):
        raw = self.torrent.info_bytes if raw is None else raw
        size = len(raw) if size is None else size
        connected, closed = asyncio.Event(), asyncio.Event()

        async def serve(reader, writer):
            self.tasks.add(asyncio.current_task())
            async def send(data):
                if fragment:
                    for start in range(0, len(data), 1021):
                        writer.write(data[start:start + 1021])
                        await writer.drain()
                        await asyncio.sleep(0)
                else:
                    writer.write(data)
                    await writer.drain()
            try:
                request = await reader.readexactly(68)
                self.assertEqual(request[25] & 0x10, 0x10)
                connected.set()
                if fault == "stall":
                    await reader.read()
                    return
                if gate:
                    await gate.wait()
                reserved = bytes(8) if fault == "unsupported" else bytes.fromhex("0000000000100000")
                await send(b"\x13BitTorrent protocol" + reserved + request[28:48] + b"S" * 20)
                if fault == "oversized_frame":
                    await send(struct.pack("!I", 0xFFFFFFFF))
                    await reader.read()
                    return
                # Deliberately different local ID: requests must use 7, replies use 1.
                await send(extended(0, encode({b"m": {b"ut_metadata": 7}, b"metadata_size": size})))
                await send(extended(1, encode({b"msg_type": 99})))  # Future metadata messages are ignored.
                await send(extended(77, b"unknown extension"))
                if fault == "changed_size":
                    await send(extended(0, encode({b"metadata_size": size + 1})))
                if fault == "disabled":
                    await send(extended(0, encode({b"m": {b"ut_metadata": 0}})))
                requests = []
                while True:
                    length = struct.unpack("!I", await reader.readexactly(4))[0]
                    body = await reader.readexactly(length)
                    self.assertEqual(body[0], 20)
                    if body[1] == 0:
                        self.assertEqual(decode(body[2:])[b"m"][b"ut_metadata"], 1)
                        continue
                    self.assertEqual(body[1], 7)
                    fields = decode(body[2:])
                    requests.append(fields[b"piece"])
                    # Hold the window and send in reverse order to test reassembly.
                    count = (size + BLOCK - 1) // BLOCK
                    if len(requests) < min(4, count - min(requests)):
                        continue
                    for piece in reversed(requests):
                        payload = raw[piece * BLOCK:(piece + 1) * BLOCK]
                        if fault == "corrupt":
                            payload = bytes([payload[0] ^ 1]) + payload[1:]
                        if fault == "short":
                            payload = payload[:-1]
                        header = {b"msg_type": 2 if fault == "reject" else 1,
                                  b"piece": count if fault == "unsolicited" else piece,
                                  b"total_size": size + 1 if fault == "wrong_size" else size}
                        packet = extended(1, encode(header) + (b"" if fault == "reject" else payload))
                        await send(packet)
                        if fault == "duplicate":
                            await send(packet)
                    requests.clear()
            except (OSError, asyncio.IncompleteReadError):
                pass
            except AssertionError as error:
                self.script_errors.append(str(error))
            finally:
                writer.close()
                try:
                    await writer.wait_closed()
                except OSError:
                    pass
                self.tasks.discard(asyncio.current_task())
                closed.set()

        server = await asyncio.start_server(serve, "127.0.0.1", 0)
        self.servers.append(server)
        return ("127.0.0.1", server.sockets[0].getsockname()[1]), connected, closed

    async def test_extension_ids_fragmented_frames_and_out_of_order_blocks(self):
        info = decode(self.torrent.info_bytes)
        info[b"comment"] = b"large metadata" * 6000
        raw = encode(info)
        magnet = Magnet.parse("magnet:?xt=urn:btih:" + sha1(raw).hexdigest())
        address, _, closed = await self.scripted(raw=raw, fragment=True)
        metrics = Metrics()
        meta = await fetch_metadata(magnet, address, metrics, timeout=5)
        self.assertEqual(meta.info_bytes, raw)
        self.assertEqual(meta.info_hash, magnet.info_hash)
        self.assertGreater(metrics.wire_received_bytes, len(raw))
        await asyncio.wait_for(closed.wait(), 1)

    async def test_rejects_bad_metadata_without_creating_payload_files(self):
        for fault in ("unsupported", "oversized_frame", "changed_size", "disabled", "corrupt",
                      "short", "reject", "unsolicited", "wrong_size"):
            with self.subTest(fault=fault):
                address, _, closed = await self.scripted(fault=fault)
                with self.assertRaises(DownloadError) as caught:
                    await download_magnet(self.magnet, [address], self.output,
                                          use_trackers=False, use_dht=False, timeout=0.5)
                self.assertFalse(self.output.exists())
                self.assertFalse(self.output.with_name("out.part").exists())
                self.assertEqual(caught.exception.report["metadata"]["peer_failures"], 1)
                await asyncio.wait_for(closed.wait(), 1)

    async def test_oversized_metadata_rejected_before_allocation(self):
        address, _, _ = await self.scripted(size=MAX_METADATA + 1)
        with self.assertRaisesRegex(ValueError, "size exceeds"):
            await fetch_metadata(self.magnet, address, Metrics(), timeout=1)

    async def test_duplicate_block_rejected(self):
        info = decode(self.torrent.info_bytes)
        info[b"comment"] = b"x" * (BLOCK * 2)
        raw = encode(info)
        address, _, _ = await self.scripted(raw=raw, fault="duplicate")
        magnet = Magnet.parse("magnet:?xt=urn:btih:" + sha1(raw).hexdigest())
        with self.assertRaisesRegex(ValueError, "duplicate"):
            await fetch_metadata(magnet, address, Metrics(), timeout=1)

    async def test_corrupt_peer_falls_back_without_learning_metadata(self):
        bad, _, failed = await self.scripted(fault="corrupt")
        good = await self.seed()
        # Ensure the corrupt response is checked before the good peer succeeds.
        raw = self.torrent.info_bytes
        other, _, _ = await self.scripted(raw=raw, gate=failed)
        meta, _, report = await resolve(self.magnet, [bad, other], use_trackers=False, use_dht=False)
        self.assertEqual(meta, self.torrent)
        self.assertEqual(report["hash_failures"], 1)
        from cbtorrent.policy import TimeBudgetPolicy
        policy = TimeBudgetPolicy()
        report = await download_magnet(self.magnet, [good], self.output, policy=policy, use_trackers=False)
        self.assertEqual(report["policy_diagnostics"]["training_bytes"], len(self.content))
        self.assertEqual(self.output.read_bytes(), self.content)
        self.assertGreater(report["completion_seconds"], report["payload_completion_seconds"])
        self.assertGreater(report["metadata"]["protocol_overhead_bytes"], 0)

    async def test_cancellation_closes_peer_and_creates_no_payload(self):
        address, connected, closed = await self.scripted(fault="stall")
        task = asyncio.create_task(download_magnet(self.magnet, [address], self.output,
                                                   use_trackers=False, use_dht=False))
        await asyncio.wait_for(connected.wait(), 1)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await asyncio.wait_for(task, 1)
        await asyncio.wait_for(closed.wait(), 1)
        self.assertFalse(self.output.with_name("out.part").exists())

    async def test_global_deadline_and_empty_discovery(self):
        address, _, closed = await self.scripted(fault="stall")
        with self.assertRaises(DownloadError) as caught:
            await asyncio.wait_for(resolve(self.magnet, [address], timeout=0.1,
                                          use_trackers=False, use_dht=False), 1)
        self.assertIn("deadline", str(caught.exception))
        await asyncio.wait_for(closed.wait(), 1)
        with self.assertRaisesRegex(DownloadError, "no peers"):
            await resolve(self.magnet, use_trackers=False, use_dht=False)

    async def test_working_peer_wins_while_another_stalls(self):
        slow, _, closed = await self.scripted(fault="stall")
        good = await self.seed()
        meta, _, report = await asyncio.wait_for(resolve(self.magnet, [slow, good],
                                                         use_trackers=False, use_dht=False), 2)
        self.assertEqual(meta.info_hash, self.torrent.info_hash)
        self.assertEqual(report["peer_failures"], 0)
        await asyncio.wait_for(closed.wait(), 1)

    async def test_private_metadata_rejected_before_payload_handoff(self):
        info = decode(self.torrent.info_bytes)
        info[b"private"] = 1
        raw = encode(info)
        address, _, _ = await self.scripted(raw=raw)
        magnet = Magnet.parse("magnet:?xt=urn:btih:" + sha1(raw).hexdigest())
        with patch("cbtorrent.magnet.download", side_effect=AssertionError("payload started")):
            with self.assertRaisesRegex(DownloadError, "private magnets"):
                await download_magnet(magnet, [address], self.output, use_trackers=False)

    async def test_multifile_magnet_download_and_verified_resume(self):
        directory = self.root / "tree"
        directory.mkdir()
        (directory / "one").write_bytes(self.content[:30001])
        (directory / "two").write_bytes(self.content[30001:])
        (directory / "empty").write_bytes(b"")
        torrent = create(directory, self.root / "tree.torrent", piece_length=16384)
        address = await self.seed(torrent, directory)
        self.output.with_name("out.part").write_bytes(self.content[:16384])
        report = await download_magnet(Magnet.parse(uri(torrent)), [address], self.output,
                                       resume=True, use_trackers=False)
        self.assertEqual(report["resumed_bytes"], 16384)
        self.assertEqual(report["payload_received_bytes"], len(self.content) - 16384)
        for name in ("one", "two", "empty"):
            self.assertEqual((self.output / name).read_bytes(), (directory / name).read_bytes())

    async def test_local_dht_discovers_metadata_without_pre_metadata_announce(self):
        address = await self.seed()
        router = DhtNode(bootstrap=(), bind_host="127.0.0.1")
        publisher = DhtNode(bootstrap=(), bind_host="127.0.0.1")
        await router.start()
        publisher.bootstrap_hosts = (("127.0.0.1", router.port),)
        await publisher.start()
        try:
            await publisher.discover(self.torrent.info_hash, port=address[1], timeout=1)
            meta, _, report = await resolve(self.magnet, use_trackers=False, use_dht=True,
                                            dht_bootstrap=[("127.0.0.1", router.port)], timeout=2)
            self.assertEqual(meta.info_hash, self.torrent.info_hash)
            self.assertGreater(report["dht_received_bytes"], 0)
        finally:
            await publisher.close()
            await router.close()

    async def test_cli_download_with_x_pe(self):
        address = await self.seed()
        link = uri(self.torrent) + f"&x.pe=127.0.0.1:{address[1]}"
        process = await asyncio.create_subprocess_exec(
            sys.executable, "-m", "cbtorrent", "download", link, "--output", str(self.output),
            "--metadata-cache-dir", str(self.root / "cache"),
            "--no-trackers", "--no-dht", stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        try:
            out, err = await asyncio.wait_for(process.communicate(), 10)
            self.assertEqual(process.returncode, 0, err.decode())
            self.assertTrue(json.loads(out)["metadata"]["complete"])
            self.assertEqual(self.output.read_bytes(), self.content)
        finally:
            if process.returncode is None:
                process.kill()
                await process.wait()

    async def test_tracker_discovers_metadata_and_receives_stop(self):
        address = await self.seed()
        targets = []
        async def tracker(reader, writer):
            self.tasks.add(asyncio.current_task())
            try:
                request = await reader.readuntil(b"\r\n\r\n")
                targets.append(request.split(b"\r\n")[0])
                body = encode({b"interval": 60, b"peers": b"\x7f\0\0\1" + struct.pack("!H", address[1])})
                writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: " + str(len(body)).encode() + b"\r\n\r\n" + body)
                await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()
                self.tasks.discard(asyncio.current_task())
        server = await asyncio.start_server(tracker, "127.0.0.1", 0)
        self.servers.append(server)
        url = f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}/announce"
        magnet = Magnet.parse(uri(self.torrent) + "&tr=" + quote(url, safe=""))
        meta, peers, report = await resolve(magnet, use_dht=False)
        self.assertIn(address, peers)
        self.assertEqual(meta.trackers, (url,))
        self.assertEqual(report["tracker_requests"], 2)
        self.assertGreater(report["tracker_response_bytes"], 0)
        self.assertIn(b"left=1", targets[0])
        self.assertIn(b"event=started", targets[0])
        self.assertIn(b"event=stopped", targets[-1])

    async def test_metadata_concurrency_cap_and_cancelled_dht_cleanup(self):
        peers = [await self.scripted(fault="stall") for _ in range(6)]
        nodes = []
        def factory(**kwargs):
            node = DhtNode(**kwargs)
            nodes.append(node)
            return node
        with patch("cbtorrent.magnet.DhtNode", side_effect=factory):
            task = asyncio.create_task(resolve(self.magnet, [p[0] for p in peers],
                                                use_trackers=False, dht_bootstrap=(), timeout=10))
            await asyncio.wait_for(asyncio.gather(*(p[1].wait() for p in peers[:3])), 1)
            self.assertFalse(any(p[1].is_set() for p in peers[3:]))
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await asyncio.wait_for(task, 1)
        self.assertEqual(nodes[0].port, 0)
        self.assertFalse(nodes[0]._pending)
        await asyncio.wait_for(asyncio.gather(*(p[2].wait() for p in peers[:3])), 1)

    async def test_gui_controller_resolves_and_downloads(self):
        from cbtorrent.gui.controller import DownloadController
        address = await self.seed()
        controller = DownloadController()
        controller.start(self.magnet, [address], self.output, use_trackers=False)
        try:
            await asyncio.wait_for(asyncio.to_thread(controller.join), 5)
            self.assertEqual(controller.snapshot.status, "complete", controller.snapshot.error)
            self.assertEqual(controller.resolved_torrent.info_hash, self.torrent.info_hash)
            self.assertEqual(controller.snapshot.length, len(self.content))
            self.assertEqual(self.output.read_bytes(), self.content)
        finally:
            controller.cancel()
            await asyncio.to_thread(controller.join, 2)

    async def test_lower_connection_cap_is_respected_in_metadata_stage(self):
        peers = [await self.scripted(fault="stall") for _ in range(3)]
        task = asyncio.create_task(download_magnet(self.magnet, [p[0] for p in peers], self.output,
                                                   max_connections=1, use_trackers=False))
        try:
            await asyncio.wait_for(peers[0][1].wait(), 1)
            self.assertFalse(any(p[1].is_set() for p in peers[1:]))
        finally:
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await asyncio.wait_for(task, 1)
        await asyncio.wait_for(peers[0][2].wait(), 1)

    async def test_hash_valid_but_unsafe_manifest_is_rejected(self):
        info = decode(self.torrent.info_bytes)
        info[b"name"] = b"../outside"
        raw = encode(info)
        address, _, _ = await self.scripted(raw=raw)
        magnet = Magnet.parse("magnet:?xt=urn:btih:" + sha1(raw).hexdigest())
        with self.assertRaises(DownloadError):
            await download_magnet(magnet, [address], self.output, use_trackers=False)
        self.assertFalse(self.output.with_name("out.part").exists())

    async def test_partial_metadata_read_is_counted_and_failure_is_bounded(self):
        async def broken(reader, writer):
            self.tasks.add(asyncio.current_task())
            try:
                request = await reader.readexactly(68)
                writer.write(b"\x13BitTorrent protocol" + bytes.fromhex("0000000000100000") + request[28:48] + b"S" * 20)
                writer.write(struct.pack("!I", 100) + b"\x14\0d")
                await writer.drain()
                length = struct.unpack("!I", await reader.readexactly(4))[0]
                await reader.readexactly(length)  # Drain the client's extension handshake before FIN.
            finally:
                writer.close()
                await writer.wait_closed()
                self.tasks.discard(asyncio.current_task())
        server = await asyncio.start_server(broken, "127.0.0.1", 0)
        self.servers.append(server)
        with self.assertRaises(DownloadError) as caught:
            await resolve(self.magnet, [("127.0.0.1", server.sockets[0].getsockname()[1])],
                          use_dht=False, use_trackers=False)
        self.assertEqual(caught.exception.report["metadata"]["wire_received_bytes"], 75)

    async def test_failed_setup_cancels_already_created_discovery_tasks(self):
        magnet = replace(self.magnet, trackers=("http://127.0.0.1:1/announce",))
        before = asyncio.all_tasks()
        with self.assertRaises(ValueError):
            await resolve(magnet, dht_bootstrap=[("localhost", 0)])
        # Windows proactor accept cleanup can finish one event-loop turn later.
        await asyncio.wait_for(asyncio.gather(*(asyncio.all_tasks() - before), return_exceptions=True), 1)
        leaked = {task for task in asyncio.all_tasks() - before if not task.done()}
        self.assertEqual(leaked, set())

    async def test_gui_controller_cancels_during_metadata(self):
        from cbtorrent.gui.controller import DownloadController
        address, connected, closed = await self.scripted(fault="stall")
        controller = DownloadController()
        controller.start(self.magnet, [address], self.output, use_trackers=False)
        try:
            await asyncio.wait_for(connected.wait(), 1)
            self.assertEqual(controller.snapshot.status, "metadata")
            controller.cancel()
            await asyncio.wait_for(asyncio.to_thread(controller.join), 2)
            self.assertEqual(controller.snapshot.status, "cancelled")
            await asyncio.wait_for(closed.wait(), 1)
        finally:
            controller.cancel()
            await asyncio.to_thread(controller.join, 2)


class MetadataServingTests(unittest.TestCase):
    def setUp(self):
        self.torrent = Torrent.from_bytes(encode({b"info": {b"name": b"empty", b"length": 0,
                                                            b"piece length": 16384, b"pieces": b""}}))

    def test_private_not_shared_disabled_id_and_request_budget(self):
        for torrent in (self.torrent, replace(self.torrent, private=True)):
            server = MetadataServer(torrent)
            server.receive(b"\0" + encode({b"m": {b"ut_metadata": 37}}))
            request = b"\1" + encode({b"msg_type": 0, b"piece": 0})
            response = server.receive(request)
            self.assertEqual(response[5], 37)
            fields, block = metadata_message(response[6:])
            self.assertEqual(fields[b"msg_type"], 2 if torrent.private else 1)
            self.assertEqual(block, b"" if torrent.private else torrent.info_bytes)
            server.receive(b"\0" + encode({b"m": {b"ut_metadata": 0}}))
            self.assertIsNone(server.receive(request))
            with self.assertRaisesRegex(ValueError, "budget"):
                for _ in range(20):
                    server.receive(request)

    def test_malformed_extension_handshakes(self):
        for value in ({b"m": []}, {b"m": {b"ut_metadata": -1}}, {b"m": {b"ut_metadata": 256}},
                      {b"metadata_size": MAX_METADATA + 1}, {b"metadata_size": 0}):
            with self.subTest(value=value), self.assertRaises(ValueError):
                MetadataServer(self.torrent).receive(b"\0" + encode(value))
