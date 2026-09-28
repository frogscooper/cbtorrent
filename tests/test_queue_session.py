"""Unit tests for cbtorrent.session.Session (queue + persistence)."""
import tempfile
import unittest
from pathlib import Path

from cbtorrent.metainfo import create
from cbtorrent.session import QueueItem, Session, default_session_path


class QueueSessionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.session_path = self.root / "session.json"
        self.source = self.root / "file.bin"
        self.source.write_bytes(b"abcdefghij" * 100)
        self.torrent_path = self.root / "file.torrent"
        self.meta = create(self.source, self.torrent_path, piece_length=64)
        self.session = Session(self.session_path, default_policy="heuristic")

    def tearDown(self):
        self.temp.cleanup()

    def test_default_session_path_uses_home_dot_cbtorrent(self):
        path = default_session_path()
        self.assertEqual(path, Path.home() / ".cbtorrent" / "session.json")

    def test_add_appends_queued_with_info_hash_id(self):
        item = self.session.add(self.torrent_path)
        self.assertEqual(item.id, self.meta.info_hash.hex())
        self.assertEqual(item.status, "queued")
        self.assertEqual(item.policy, "heuristic")
        self.assertEqual(item.output, Path("downloads") / self.meta.name)
        self.assertEqual([i.id for i in self.session.items()], [item.id])

    def test_add_uses_session_default_policy(self):
        self.session.set_default_policy("bandit")
        item = self.session.add(self.torrent_path)
        self.assertEqual(item.policy, "bandit")

    def test_remove_deletes_from_session(self):
        item = self.session.add(self.torrent_path)
        self.session.remove(item.id)
        self.assertEqual(self.session.items(), [])

    def test_pause_and_resume_status(self):
        item = self.session.add(self.torrent_path)
        self.session.resume(item.id)
        self.assertEqual(self.session.get(item.id).status, "downloading")
        self.session.pause(item.id)
        self.assertEqual(self.session.get(item.id).status, "paused")

    def test_resume_pauses_other_downloading(self):
        a = self.session.add(self.torrent_path)
        other = self.root / "other.bin"
        other.write_bytes(b"xyz" * 50)
        other_torrent = self.root / "other.torrent"
        create(other, other_torrent, piece_length=64)
        b = self.session.add(other_torrent)
        self.session.resume(a.id)
        self.session.resume(b.id)
        self.assertEqual(self.session.get(a.id).status, "paused")
        self.assertEqual(self.session.get(b.id).status, "downloading")
        self.assertEqual(self.session.downloading_id(), b.id)

    def test_only_one_downloading_intent_after_resume(self):
        a = self.session.add(self.torrent_path)
        other = self.root / "b.bin"
        other.write_bytes(b"1234" * 40)
        bt = self.root / "b.torrent"
        create(other, bt, piece_length=64)
        b = self.session.add(bt)
        self.session.resume(a.id)
        self.session.resume(b.id)
        downloading = [i for i in self.session.items() if i.status == "downloading"]
        self.assertEqual(len(downloading), 1)
        self.assertEqual(downloading[0].id, b.id)

    def test_reorder_splices_before_id(self):
        a = self.session.add(self.torrent_path)
        other = self.root / "c.bin"
        other.write_bytes(b"zzzz" * 40)
        ct = self.root / "c.torrent"
        create(other, ct, piece_length=64)
        b = self.session.add(ct)
        third = self.root / "d.bin"
        third.write_bytes(b"yyyy" * 40)
        dt = self.root / "d.torrent"
        create(third, dt, piece_length=64)
        c = self.session.add(dt)
        self.assertEqual([i.id for i in self.session.items()], [a.id, b.id, c.id])
        self.session.reorder(c.id, a.id)
        self.assertEqual([i.id for i in self.session.items()], [c.id, a.id, b.id])
        self.session.reorder(c.id, None)
        self.assertEqual([i.id for i in self.session.items()], [a.id, b.id, c.id])

    def test_set_policy(self):
        item = self.session.add(self.torrent_path)
        self.session.set_policy(item.id, "adaptive")
        self.assertEqual(self.session.get(item.id).policy, "adaptive")

    def test_persist_roundtrip(self):
        item = self.session.add(self.torrent_path, output=self.root / "out.bin", policy="timed")
        self.session.resume(item.id)
        self.session.set_status(item.id, "complete")
        again = Session(self.session_path)
        count = again.load()
        self.assertEqual(count, 1)
        loaded = again.items()[0]
        self.assertEqual(loaded.id, item.id)
        self.assertEqual(loaded.torrent_path, item.torrent_path)
        self.assertEqual(loaded.output, item.output)
        self.assertEqual(loaded.policy, "timed")
        self.assertEqual(loaded.status, "complete")
        self.assertEqual(loaded.queue_order, item.queue_order)

    def test_load_keeps_one_downloading_coerces_rest(self):
        a = self.session.add(self.torrent_path)
        other = self.root / "e.bin"
        other.write_bytes(b"eeee" * 40)
        et = self.root / "e.torrent"
        create(other, et, piece_length=64)
        b = self.session.add(et)
        # Bypass resume() so both can be saved as downloading.
        self.session.get(a.id).status = "downloading"
        self.session.get(b.id).status = "downloading"
        self.session.save()

        again = Session(self.session_path)
        again.load()
        statuses = {i.id: i.status for i in again.items()}
        self.assertEqual(statuses[a.id], "downloading")
        self.assertEqual(statuses[b.id], "paused")
        self.assertEqual(again.downloading_id(), a.id)

    def test_load_preserves_queued_paused_complete_error(self):
        item = self.session.add(self.torrent_path)
        self.session.set_status(item.id, "error")
        again = Session(self.session_path)
        again.load()
        self.assertEqual(again.get(item.id).status, "error")

    def test_queue_item_to_from_dict(self):
        item = QueueItem(
            id="abc",
            torrent_path=Path("/t.torrent"),
            output=Path("/out"),
            policy="heuristic",
            status="queued",
            queue_order=2.0,
        )
        restored = QueueItem.from_dict(item.to_dict())
        self.assertEqual(restored, item)


if __name__ == "__main__":
    unittest.main()
