import asyncio
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from cbtorrent.gui.controller import DownloadController
from cbtorrent.gui.scheduler import QueueScheduler
from cbtorrent.metainfo import create
from cbtorrent.seeder import FileSource, SeedServer
from cbtorrent.session import Session


class QueueSchedulerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.session = Session(self.root / "session.json")
        self.items = []
        for n in range(3):
            source = self.root / f"source{n}"
            source.write_bytes(bytes([n]) * 1000)
            torrent = self.root / f"{n}.torrent"
            create(source, torrent, piece_length=64)
            self.items.append(self.session.add(torrent, self.root / f"out{n}"))
        self.controller = SimpleNamespace(busy=False, snapshot=None, cancel=lambda: None)
        self.started = []

        def launch(item):
            self.assertFalse(self.controller.busy)
            self.started.append(item.id)
            self.controller.busy = True
            self.controller.snapshot = SimpleNamespace(status="starting")
            return True

        self.scheduler = QueueScheduler(self.session, self.controller, launch)

    def finish(self, status="complete"):
        self.controller.busy = False
        self.controller.snapshot = SimpleNamespace(status=status)
        self.scheduler.poll()

    def test_completion_advances_once_and_stops_when_drained(self):
        self.scheduler.start()
        # Complete snapshot alone is insufficient while cleanup is running.
        self.controller.snapshot.status = "complete"
        self.scheduler.poll()
        self.assertEqual(len(self.started), 1)
        for _ in self.items:
            self.finish()
        self.scheduler.poll()
        self.assertEqual(self.started, [item.id for item in self.items])
        self.assertTrue(all(item.status == "complete" for item in self.items))
        self.assertFalse(self.session.queue_running)

    def test_stop_during_transfer_does_not_advance_and_can_restart(self):
        self.scheduler.start()
        self.scheduler.stop()
        self.assertEqual(self.items[0].status, "queued")
        self.finish("cancelled")
        self.assertEqual(len(self.started), 1)
        self.scheduler.start()
        self.assertEqual(self.started, [self.items[0].id] * 2)

    def test_pause_and_error_are_skipped_by_explicit_start(self):
        self.items[0].status = "paused"
        self.items[1].status = "error"
        self.scheduler.start()
        self.assertEqual(self.started, [self.items[2].id])
        self.finish()
        self.assertEqual(self.items[0].status, "paused")
        self.assertEqual(self.items[1].status, "error")

    def test_error_or_unexpected_cancellation_stops_queue(self):
        for status in ("error", "cancelled"):
            with self.subTest(status=status):
                self.items[0].status = "queued"
                self.scheduler.start()
                before = len(self.started)
                self.finish(status)
                self.assertEqual(len(self.started), before)
                self.assertFalse(self.session.queue_running)

    def test_remove_active_after_stop_does_not_launch_next(self):
        self.scheduler.start()
        self.scheduler.stop(paused=True)
        self.session.remove(self.items[0].id)
        self.finish("cancelled")
        self.assertEqual(len(self.started), 1)

    def test_pause_does_not_start_another_item(self):
        self.scheduler.start()
        self.scheduler.stop(paused=True)
        self.finish("cancelled")
        self.assertEqual(self.items[0].status, "paused")
        self.assertEqual(len(self.started), 1)

    def test_restart_preserves_run_intent_and_order(self):
        self.session.reorder(self.items[2].id, self.items[0].id)
        self.scheduler.start()
        self.scheduler.close()
        again = Session(self.session.path)
        again.load()
        self.assertTrue(again.queue_running)
        self.assertEqual(again.downloading_id(), self.items[2].id)
        self.controller.busy = False
        self.controller.snapshot = None
        restored = QueueScheduler(again, self.controller, self.scheduler.launch)
        restored.poll()
        self.assertEqual(self.started, [self.items[2].id] * 2)
        self.assertEqual([i.id for i in again.items()],
                         [self.items[2].id, self.items[0].id, self.items[1].id])

    def test_stopped_queue_survives_restart(self):
        self.scheduler.start()
        self.scheduler.stop()
        again = Session(self.session.path)
        again.load()
        self.assertFalse(again.queue_running)
        self.assertEqual(again.get(self.items[0].id).status, "queued")

    def test_launch_failure_stops_queue(self):
        self.scheduler.launch = lambda item: False
        self.scheduler.start()
        self.assertEqual(self.items[0].status, "error")
        self.assertFalse(self.session.queue_running)
        self.scheduler.poll()
        self.assertEqual(self.started, [])

    def test_manual_resume_settles_previous_completion_before_next_poll(self):
        self.scheduler.resume(self.items[0])
        self.controller.busy = False
        self.controller.snapshot.status = "complete"
        self.scheduler.resume(self.items[1])
        self.assertEqual(self.items[0].status, "complete")
        self.assertEqual(self.items[1].status, "downloading")

    def test_close_after_completion_saves_it_without_starting_next(self):
        self.scheduler.start()
        self.controller.busy = False
        self.controller.snapshot.status = "complete"
        self.scheduler.close()
        again = Session(self.session.path)
        again.load()
        self.assertEqual(again.get(self.items[0].id).status, "complete")
        self.assertEqual(again.get(self.items[1].id).status, "queued")
        self.assertTrue(again.queue_running)
        self.assertEqual(len(self.started), 1)

    def test_legacy_session_defaults_to_stopped(self):
        import json
        data = json.loads(self.session.path.read_text())
        del data["queue_running"]
        data["version"] = 1
        self.session.path.write_text(json.dumps(data))
        again = Session(self.session.path)
        again.load()
        self.assertFalse(again.queue_running)
        self.assertEqual(len(again.items()), 3)

    def test_gui_buttons_drive_queue_and_reorder_without_a_display(self):
        import sys
        from types import ModuleType
        from cbtorrent.gui.queue_window import run
        from cbtorrent.observe import DownloadSnapshot

        tk, ttk = ModuleType("tkinter"), ModuleType("tkinter.ttk")
        root = MagicMock()
        tk.Tk = lambda: root
        tk.TclError = RuntimeError
        tk.StringVar = lambda *args, **kwargs: MagicMock()
        tk.Menu = lambda *args, **kwargs: MagicMock()
        tk.filedialog = SimpleNamespace()
        tk.ttk = ttk
        for name in ("Style", "Frame", "Label", "Menubutton", "Progressbar", "Scrollbar"):
            setattr(ttk, name, lambda *args, **kwargs: MagicMock())
        trees = []

        def tree(*args, **kwargs):
            widget = MagicMock()
            widget.get_children.return_value = ()
            widget.selection.return_value = ()
            trees.append(widget)
            return widget

        ttk.Treeview = tree
        commands = {}

        def button(*args, **kwargs):
            commands[kwargs["text"]] = kwargs["command"]
            return MagicMock()

        ttk.Button = button
        controller = MagicMock()
        controller.busy = False
        controller.active_id = None
        controller.snapshot = None

        def transfer(meta, peers, output, **kwargs):
            controller.active_id = kwargs["item_id"]
            self.started.append(controller.active_id)
            controller.snapshot = DownloadSnapshot(
                name=meta.name, length=meta.length, done_bytes=meta.length,
                verified_bytes=meta.length, resumed_bytes=0, uploaded_bytes=0,
                payload_received_bytes=meta.length, elapsed_seconds=1, peer_count=0,
                status="complete")

        controller.start.side_effect = transfer

        def mainloop():
            # Select the first row through the real GUI selection callback.
            trees[0].selection.return_value = (self.items[0].id,)
            trees[0].bind.call_args.args[1]()
            commands["Move Down"]()
            commands["Start Queue"]()
            for _ in range(3):
                root.after.call_args.args[1]()
            root.protocol.call_args.args[1]()

        root.mainloop.side_effect = mainloop
        with patch.dict(sys.modules, {"tkinter": tk, "tkinter.ttk": ttk}), \
                patch("cbtorrent.gui.controller.DownloadController", return_value=controller):
            self.assertEqual(run(session_path=self.session.path), 0)
        self.assertEqual(self.started, [self.items[1].id, self.items[0].id, self.items[2].id])
        again = Session(self.session.path)
        again.load()
        self.assertTrue(all(item.status == "complete" for item in again.items()))
        self.assertFalse(again.queue_running)


class ControllerCancellationTests(unittest.TestCase):
    def test_cancel_before_worker_creates_task_is_not_lost(self):
        worker_entered = threading.Event()
        release_worker = threading.Event()
        new_loop = asyncio.new_event_loop

        def delayed_loop():
            worker_entered.set()
            if not release_worker.wait(3):
                raise RuntimeError("test did not release worker")
            return new_loop()

        async def transfer(*args, **kwargs):
            await asyncio.Future()

        controller = DownloadController()
        with patch("cbtorrent.gui.controller.asyncio.new_event_loop", delayed_loop), \
                patch("cbtorrent.gui.controller.download", transfer):
            try:
                controller.start(SimpleNamespace(name="test", length=1), [], Path("unused"))
                self.assertTrue(worker_entered.wait(3))
                controller.cancel()
            finally:
                release_worker.set()
                controller.join(3)
        self.assertFalse(controller.busy)
        self.assertEqual(controller.snapshot.status, "cancelled")


class QueueTransferTests(unittest.IsolatedAsyncioTestCase):
    async def test_mixed_file_and_directory_downloads_advance_and_verify_in_order(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            session = Session(root / "session.json")
            controller = DownloadController()
            servers, sources, peers, metas, expected = [], [], {}, {}, {}
            started = []
            try:
                for n in range(3):
                    path = root / f"source{n}"
                    data = bytes([n]) * 70000
                    if n == 1:
                        (path / "nested").mkdir(parents=True)
                        (path / "first").write_bytes(data[:17000])
                        (path / "nested" / "rest").write_bytes(data[17000:])
                        (path / "nested" / "zero").write_bytes(b"")
                    else:
                        path.write_bytes(data)
                    torrent = root / f"{n}.torrent"
                    meta = create(path, torrent, piece_length=32768)
                    item = session.add(torrent, root / f"out{n}")
                    source = FileSource(meta, path)
                    sources.append(source)
                    server = SeedServer(meta, source)
                    servers.append(server)
                    port = await server.start()
                    peers[item.id] = [("127.0.0.1", port)]
                    metas[item.id] = meta
                    expected[item.id] = data

                def launch(item):
                    started.append(item.id)
                    controller.start(metas[item.id], peers[item.id], item.output,
                                     use_trackers=False, listen_host="127.0.0.1",
                                     item_id=item.id)
                    return True

                scheduler = QueueScheduler(session, controller, launch)
                scheduler.start()
                async with asyncio.timeout(10):
                    while session.queue_running or controller.busy:
                        scheduler.poll()
                        await asyncio.sleep(0.01)
                self.assertEqual(started, [item.id for item in session.items()])
                for item in session.items():
                    self.assertEqual(item.status, "complete")
                    if metas[item.id].multi_file:
                        self.assertEqual((item.output / "first").read_bytes(), expected[item.id][:17000])
                        self.assertEqual((item.output / "nested" / "rest").read_bytes(), expected[item.id][17000:])
                        self.assertEqual((item.output / "nested" / "zero").read_bytes(), b"")
                    else:
                        self.assertEqual(item.output.read_bytes(), expected[item.id])
                    self.assertFalse(item.output.with_name(item.output.name + ".part").exists())
            finally:
                controller.cancel()
                await asyncio.to_thread(controller.join, 3)
                for server in servers:
                    await server.close()
                for source in sources:
                    source.close()
