import asyncio
import hashlib
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from cbtorrent.client import DownloadError, download
from cbtorrent.metainfo import Torrent, create
from cbtorrent.metrics import Metrics
from cbtorrent.policy import AdaptivePolicy, BanditPolicy, Observation, RecoveryPolicy
from cbtorrent.seeder import FileSource, SeedServer
from cbtorrent.storage import Storage
from cbtorrent.wire import Peer, message


class SessionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.original = self.root / "original.bin"
        self.data = bytes(range(251)) * 1000
        self.original.write_bytes(self.data)
        self.torrent = create(self.original, self.root / "test.torrent", piece_length=32768)
        self.output = self.root / "result.bin"
        self.sources = []
        self.servers = []

    async def asyncTearDown(self):
        await asyncio.gather(*(s.close() for s in self.servers))
        for source in self.sources:
            source.close()
        self.temp.cleanup()

    async def seed(self, available=None, **kwargs):
        source = FileSource(self.torrent, self.original)
        self.sources.append(source)
        if available is not None:
            source.verified = set(available)
        server = SeedServer(self.torrent, source, **kwargs)
        self.servers.append(server)
        port = await server.start()
        return "127.0.0.1", port

    async def test_complementary_peers_supply_all_pieces(self):
        count = len(self.torrent.hashes)
        a = await self.seed(range(0, count, 2))
        b = await self.seed(range(1, count, 2))
        report = await asyncio.wait_for(download(self.torrent, [a, b], self.output, timeout=0.2), 5)
        self.assertEqual(self.output.read_bytes(), self.data)
        self.assertEqual(report["wasted_payload_bytes"], 0)
        self.assertEqual(report["connections"], 2)
        self.assertTrue(all(o["verified_bytes"] > 0 for o in report["peer_observations"].values()))

    async def test_concurrency_makes_progress_on_two_peers(self):
        a, b = await self.seed(latency=0.02), await self.seed(latency=0.02)
        report = await download(self.torrent, [a, b], self.output, concurrency=2)
        self.assertEqual(len(report["peer_observations"]), 2)
        self.assertTrue(all(o["verified_bytes"] > 0 for o in report["peer_observations"].values()))
        self.assertEqual(report["verified_bytes"], len(self.data))
        self.assertEqual(report["payload_received_bytes"], len(self.data))

    async def test_resume_rechecks_corrupt_and_missing_pieces(self):
        part = self.output.with_suffix(".bin.part")
        part.write_bytes(self.data[:32768] + b"X" * 32768)
        peer = await self.seed()
        report = await download(self.torrent, [peer], self.output, resume=True)
        self.assertEqual(report["resumed_bytes"], 32768)
        self.assertEqual(report["verified_bytes"], len(self.data) - 32768)
        self.assertEqual(report["wasted_payload_bytes"], 0)
        self.assertEqual(self.output.read_bytes(), self.data)

    async def test_complete_resume_needs_no_peers(self):
        self.output.with_suffix(".bin.part").write_bytes(self.data)
        report = await download(self.torrent, [], self.output, resume=True)
        self.assertEqual(report["resumed_bytes"], len(self.data))
        self.assertEqual(report["payload_received_bytes"], 0)
        self.assertEqual(self.output.read_bytes(), self.data)

    async def test_empty_file_download_needs_no_peers(self):
        empty = self.root / "empty"
        empty.write_bytes(b"")
        torrent = create(empty, self.root / "empty.torrent")
        report = await download(torrent, [], self.output)
        self.assertTrue(report["complete"])
        self.assertEqual(self.output.read_bytes(), b"")

    async def test_cancel_closes_all_sessions_and_retains_partial(self):
        address = await self.seed(latency=0.2)
        task = asyncio.create_task(download(self.torrent, [address], self.output))
        await asyncio.sleep(0.05)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertFalse(self.output.exists())
        self.assertTrue(self.output.with_suffix(".bin.part").exists())
        await asyncio.sleep(0.05)
        self.assertFalse(self.servers[0].tasks)

    async def test_disk_failure_does_not_penalize_peer(self):
        address = await self.seed()
        with patch.object(Storage, "write", side_effect=OSError("disk full")):
            with self.assertRaises(DownloadError) as caught:
                await download(self.torrent, [address], self.output)
        self.assertEqual(caught.exception.report["peer_failures"], 0)
        self.assertIn("disk full", str(caught.exception))

    async def test_bandit_completes_real_transfer(self):
        peers = [await self.seed(), await self.seed()]
        report = await download(self.torrent, peers, self.output, policy=BanditPolicy())
        self.assertEqual(self.output.read_bytes(), self.data)
        self.assertEqual(report["policy"], "BanditPolicy")
        self.assertGreater(sum(o["samples"] for o in report["peer_observations"].values()), 0)

    async def test_adaptive_transfer_trains_on_verified_data(self):
        peers = [await self.seed(latency=0.005), await self.seed()]
        policy = AdaptivePolicy()
        report = await download(self.torrent, peers, self.output, policy=policy)
        self.assertEqual(self.output.read_bytes(), self.data)
        self.assertEqual(report["policy"], "AdaptivePolicy")
        self.assertEqual(sum(m.samples for m in policy.models.values()), len(self.torrent.hashes))
        self.assertEqual(report["wasted_payload_bytes"], 0)

    async def test_adaptive_does_not_train_on_failed_disk_commit(self):
        peer = await self.seed()
        policy = AdaptivePolicy()
        with patch.object(Storage, "write", side_effect=OSError("disk full")):
            with self.assertRaises(DownloadError):
                await download(self.torrent, [peer], self.output, policy=policy)
        self.assertEqual(policy.models, {})

    async def test_recovery_handles_complementary_availability_and_resume(self):
        self.output.with_suffix(".bin.part").write_bytes(self.data[:32768])
        count = len(self.torrent.hashes)
        peers = [await self.seed(range(0, count, 2)), await self.seed(range(1, count, 2))]
        policy = RecoveryPolicy()
        report = await asyncio.wait_for(download(
            self.torrent, peers, self.output, policy=policy, resume=True, timeout=0.2), 5)
        self.assertEqual(self.output.read_bytes(), self.data)
        self.assertEqual(report["resumed_bytes"], 32768)
        self.assertEqual(policy.verified_bytes, len(self.data) - 32768)
        self.assertEqual(report["wasted_payload_bytes"], 0)
        self.assertEqual(report["connections"], 2)

    async def test_recovery_cancel_releases_sockets_without_training(self):
        address = await self.seed()
        policy = RecoveryPolicy()
        block_received = asyncio.Event()
        hold_block = asyncio.Event()
        receive = Peer.receive
        clients = []

        async def receive_then_pause(peer):
            packet = await receive(peer)
            if packet[0] == 7:
                clients.append(peer)
                block_received.set()
                # Receive real TCP payload, but prevent any piece from reaching
                # verification/commit before the test requests cancellation.
                await hold_block.wait()
            return packet

        with patch.object(Peer, "receive", receive_then_pause):
            task = asyncio.create_task(download(self.torrent, [address], self.output, policy=policy))
            try:
                await asyncio.wait_for(block_received.wait(), 5)
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await asyncio.wait_for(task, 5)
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        async with asyncio.timeout(5):
            while self.servers[0].tasks:
                await asyncio.sleep(0.01)
        self.assertEqual(policy.models, {})
        self.assertEqual(policy.verified_bytes, 0)
        self.assertTrue(all(peer.writer.is_closing() for peer in clients))
        self.assertGreater(clients[0].metrics.payload_received_bytes, 0)
        self.assertFalse(self.servers[0].tasks)
        self.assertFalse(self.output.exists())

    async def test_recovery_disk_failure_does_not_train(self):
        peer = await self.seed()
        policy = RecoveryPolicy()
        with patch.object(Storage, "write", side_effect=OSError("disk full")):
            with self.assertRaises(DownloadError):
                await download(self.torrent, [peer], self.output, policy=policy)
        self.assertEqual(policy.models, {})
        self.assertEqual(policy.verified_bytes, 0)

    async def test_recovery_respects_connection_cap_with_more_peers_than_slots(self):
        peers = [await self.seed() for _ in range(6)]
        report = await asyncio.wait_for(download(
            self.torrent, peers, self.output, policy=RecoveryPolicy(),
            concurrency=1, max_connections=1, timeout=0.2), 5)
        self.assertEqual(self.output.read_bytes(), self.data)
        self.assertEqual(report["wasted_payload_bytes"], 0)
        self.assertEqual(report["policy_diagnostics"]["probes"], 0)
        await asyncio.sleep(0.05)
        self.assertTrue(all(not s.tasks for s in self.servers))

    async def test_seed_rejects_file_with_wrong_content(self):
        self.original.write_bytes(b"X" * len(self.data))
        with self.assertRaisesRegex(ValueError, "hash"):
            FileSource(self.torrent, self.original)

    async def test_seed_refuses_out_of_bounds_requests(self):
        address = await self.seed()
        reader, writer = await asyncio.open_connection(*address)
        peer = Peer(reader, writer, self.torrent, Metrics(), 1)
        try:
            await peer.handshake(b"T" * 20)
            await peer.ready()
            await peer.send(message(6, bytes(8) + (16385).to_bytes(4, "big")))
            with self.assertRaises(asyncio.IncompleteReadError):
                await peer.receive()
        finally:
            await peer.close()

    async def test_partial_seed_exposes_only_verified_pieces(self):
        storage = Storage(self.torrent, self.output)
        try:
            storage.write(0, self.data[:32768])
            server = SeedServer(self.torrent, storage)
            self.servers.append(server)
            port = await server.start()
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            peer = Peer(reader, writer, self.torrent, Metrics(), 1)
            try:
                await peer.handshake(b"T" * 20)
                await peer.ready()
                self.assertEqual(peer.available, {0})
                received = await peer.download_piece(0, 4)
                self.assertEqual(received, self.data[:32768])
                storage.write(1, self.data[32768:65536])
                await server.have(1)
                await peer.receive()
                self.assertEqual(peer.available, {0, 1})
            finally:
                await peer.close()
                await server.close()
        finally:
            storage.close()


class BanditTests(unittest.TestCase):
    def test_exploration_and_reward(self):
        a, b = ("a", 1), ("b", 2)
        stats = {a: Observation()}
        policy = BanditPolicy(exploration=0)
        stats[a].record(100000, 0.1, 100500)
        self.assertEqual(policy.choose([a, b], stats), b)
        stats[b] = Observation()
        stats[b].record(100000, 2, 200000)
        self.assertEqual(policy.choose([a, b], stats), a)
        self.assertLessEqual(stats[a].reward_sum, 1)
        self.assertGreaterEqual(stats[b].reward_sum, 0)

    def test_failed_transfer_has_zero_reward(self):
        stats = Observation()
        stats.record(0, 1, 30000, True)
        self.assertEqual(stats.reward_sum, 0)
        self.assertEqual(stats.failures, 1)

    def test_invalid_exploration(self):
        for value in (-1, float("inf"), float("nan")):
            with self.assertRaises(ValueError):
                BanditPolicy(value)
