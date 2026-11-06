"""LearnedPolicy decisions and the client's streaming transfer path (loopback only)."""
import asyncio
import tempfile
import unittest
from pathlib import Path

from cbtorrent.benchmark import UnreliableSource
from cbtorrent.client import DownloadError, download
from cbtorrent.metainfo import create
from cbtorrent.policy import ActiveTransfer, LearnedPolicy, SchedulingContext
from cbtorrent.seeder import FileSource, SeedServer

PIECE = 32768


class Clock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


def context(*, unclaimed=100, active=None, concurrency=4, piece=PIECE):
    return SchedulingContext(0.0, unclaimed, {}, {}, active or {}, concurrency, piece)


class PolicyTests(unittest.TestCase):
    def test_window_follows_learned_rate_within_bounds(self):
        policy = LearnedPolicy(queue_seconds=1.0, initial_window=16, max_window=64)
        self.assertEqual(policy.pipeline_for("new"), 16)
        policy.observe("slow", 16384, 1.0)          # 16 KiB/s -> 1 block, floor 4
        policy.observe("mid", 32 * 16384, 1.0)      # 32 blocks/s -> 32
        policy.observe("fast", 1000 * 16384, 1.0)   # capped
        self.assertEqual([policy.pipeline_for(p) for p in ("slow", "mid", "fast")], [4, 32, 64])

    def test_more_slots_are_kept_only_when_goodput_rises(self):
        clock = Clock()
        policy = LearnedPolicy(start_slots=4, max_slots=16, epoch_seconds=1.0, min_gain=0.1, clock=clock)

        def epoch(rate):
            policy.record_bytes(0)
            clock.now += 1.0
            policy.record_bytes(int(rate))
            return policy.target_concurrency()

        self.assertEqual(epoch(100_000), 8)    # first epoch sets the reference, then tries more
        self.assertEqual(epoch(200_000), 16)   # gain confirmed: keep 8, try 16
        self.assertEqual(epoch(205_000), 8)    # +2.5% is noise: back to 8
        self.assertEqual(policy.accepted, 8)
        self.assertEqual(epoch(200_000), 8)    # cooling down
        self.assertEqual(epoch(200_000), 16)   # tries again later

    def test_unseen_peers_are_explored_only_while_work_remains(self):
        policy = LearnedPolicy()
        policy.observe("known", PIECE, 0.1)
        self.assertEqual(policy.choose_with_context(["known", "new"], {}, context(unclaimed=100)), "new")
        self.assertEqual(policy.choose_with_context(["known", "new"], {}, context(unclaimed=3)), "known")

    def test_slow_peer_stops_claiming_at_the_tail(self):
        policy = LearnedPolicy()
        policy.recent_goodput = 1_000_000
        policy.observe("slow", PIECE, 10.0)   # 10 s per piece; the rest needs < 1 s
        policy.observe("fast", PIECE, 0.01)
        tail = context(unclaimed=2, active={"fast": ActiveTransfer(1, PIECE, 0, 0.0)})
        self.assertFalse(policy.continue_stream("slow", tail, []))
        self.assertTrue(policy.continue_stream("fast", tail, []))
        self.assertTrue(policy.continue_stream("slow", context(unclaimed=1000), []))

    def test_full_slots_rotate_to_a_much_faster_idle_peer(self):
        policy = LearnedPolicy()
        policy.observe("slow", PIECE, 1.0)
        policy.observe("fast", PIECE, 0.1)
        busy = {f"p{i}": ActiveTransfer(i, PIECE, 0, 0.0) for i in range(4)}
        self.assertFalse(policy.continue_stream("slow", context(active=busy), ["fast"]))
        self.assertTrue(policy.continue_stream("slow", context(active=dict(list(busy.items())[:2])), ["fast"]))

    def test_abandon_only_when_far_slower_than_the_remaining_work(self):
        policy = LearnedPolicy()
        policy.recent_goodput = 1_000_000
        active = {"slow": ActiveTransfer(0, 256 * 1024, 0, 0.0)}
        # 256 KiB at 10 KiB/s is ~26 s; all remaining work needs well under a second.
        self.assertTrue(policy.abandon("slow", 10_000, context(unclaimed=1, active=active), []))
        # Nearly done pieces are always finished.
        active = {"slow": ActiveTransfer(0, 256 * 1024, 256 * 1024 - 1000, 0.0)}
        self.assertFalse(policy.abandon("slow", 10_000, context(unclaimed=1, active=active), []))
        policy.recent_goodput = None
        self.assertFalse(policy.abandon("slow", 1, context(unclaimed=1, active=active), []))

    def test_invalid_settings_are_rejected(self):
        for options in (dict(start_slots=0), dict(start_slots=9, max_slots=8), dict(epoch_seconds=0),
                        dict(min_gain=1), dict(initial_window=0), dict(max_window=129)):
            with self.assertRaises(ValueError):
                LearnedPolicy(**options)


class StreamingDownloadTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.data = bytes(range(256)) * (4 * 1024)  # 1 MiB, 32 pieces
        (self.root / "source").write_bytes(self.data)
        self.torrent = create(self.root / "source", self.root / "a.torrent", piece_length=PIECE)

    async def seed(self, *, corrupt=False, **options):
        source = FileSource(self.torrent, self.root / "source")
        self.addCleanup(source.close)
        server = SeedServer(self.torrent, UnreliableSource(source) if corrupt else source, **options)
        self.addAsyncCleanup(server.close)
        return "127.0.0.1", await server.start("127.0.0.1", 0)

    async def get(self, peers, name="out", **options):
        options.setdefault("policy", LearnedPolicy(start_slots=2, max_slots=4))
        return await asyncio.wait_for(download(self.torrent, peers, self.root / name,
                                               use_trackers=False, **options), 30)

    async def test_streams_pieces_back_to_back_and_verifies_everything(self):
        peers = [await self.seed(rate=2_000_000, delay=0.02) for _ in range(3)]
        report = await self.get(peers)
        self.assertEqual((self.root / "out").read_bytes(), self.data)
        self.assertEqual(report["verified_bytes"], len(self.data))
        self.assertEqual(report["hash_failures"], 0)
        # Few connections, many pieces: connections were reused across pieces.
        self.assertLessEqual(report["connections"], 3)
        self.assertIn("slots", report["policy_diagnostics"])

    async def test_corrupt_streaming_peer_is_banned_and_pieces_refetched(self):
        peers = [await self.seed(corrupt=True), await self.seed(rate=1_000_000, delay=0.01)]
        report = await self.get(peers)
        self.assertEqual((self.root / "out").read_bytes(), self.data)
        self.assertGreaterEqual(report["hash_failures"], 1)
        self.assertEqual(report["peers_banned"], 1)

    async def test_very_slow_peer_does_not_hold_the_tail(self):
        slow = await self.seed(rate=4_000, delay=0.01)  # 32 KiB piece takes 8 s
        fast = [await self.seed(rate=2_000_000, delay=0.01) for _ in range(2)]
        loop = asyncio.get_running_loop()
        started = loop.time()
        report = await self.get([slow] + fast, policy=LearnedPolicy(start_slots=3, max_slots=3,
                                                                     epoch_seconds=0.5))
        self.assertEqual((self.root / "out").read_bytes(), self.data)
        self.assertLess(loop.time() - started, 6)  # the slow piece was abandoned or never needed
        self.assertEqual(report["peer_failures"], 0)

    async def test_cancelling_a_streaming_download_leaves_no_tasks(self):
        peers = [await self.seed(rate=50_000, delay=0.01) for _ in range(2)]
        task = asyncio.create_task(download(self.torrent, peers, self.root / "c", use_trackers=False,
                                            policy=LearnedPolicy(start_slots=2, max_slots=2)))
        await asyncio.sleep(0.5)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        await asyncio.sleep(0)
        leftover = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()
                    and "download" in repr(t.get_coro())]
        self.assertEqual(leftover, [])

    async def test_policy_must_return_integer_controls(self):
        class Bad(LearnedPolicy):
            def target_concurrency(self):
                return 2.5
        peers = [await self.seed()]
        with self.assertRaisesRegex(DownloadError, "non-integer concurrency"):
            await self.get(peers, policy=Bad())

    async def test_slot_count_is_clamped_to_max_connections(self):
        peers = [await self.seed(rate=1_000_000) for _ in range(3)]
        policy = LearnedPolicy(start_slots=40, max_slots=64)
        report = await self.get(peers, policy=policy, max_connections=2, concurrency=1)
        self.assertEqual((self.root / "out").read_bytes(), self.data)
        self.assertLessEqual(report["connections"], 3)


if __name__ == "__main__":
    unittest.main()
