"""Real Tk button actions with local sources, isolated sessions, and no public peers."""
import asyncio
import json
import os
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from cbtorrent.gui.queue_window import create_window
from cbtorrent.metainfo import create
from cbtorrent.seeder import FileSource, SeedServer
from cbtorrent.session import Session


class GuiWorkflowTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.session_path = self.directory / "session.json"
        self.folder = self.directory / "downloads"
        self.payload = b"gui workflow\x00\xff" * 2000
        self.source = self.directory / "source.bin"
        self.source.write_bytes(self.payload)
        self.torrent_path = self.directory / "source.torrent"
        self.meta = create(self.source, self.torrent_path, piece_length=16384)
        self.windows = []
        self.addCleanup(self.close_windows)

    def close_windows(self):
        for window in reversed(self.windows):
            if not window.closed:
                window.close()
            window.controller.join(5)
            self.assertFalse(window.controller.busy, "GUI worker leaked after shutdown")

    def window(self, **options):
        try:
            import tkinter as tk
            root = tk.Tk()
        except (ImportError, RuntimeError) as error:
            if os.environ.get("CBTORRENT_REQUIRE_GUI_TESTS") == "1":
                raise
            self.skipTest(f"Tk/display unavailable: {error}")
        except tk.TclError as error:
            if os.environ.get("CBTORRENT_REQUIRE_GUI_TESTS") == "1":
                raise
            self.skipTest(f"Tk/display unavailable: {error}")
        root.withdraw()
        try:
            window = create_window(session_path=self.session_path, root=root,
                                   use_trackers=False, use_dht=False, **options)
        except BaseException:
            root.destroy()
            raise
        self.windows.append(window)
        root.update()
        return window

    def pump(self, window, predicate, timeout=5):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            window.root.update()
            if predicate():
                return
            time.sleep(0.005)
        self.fail(f"GUI did not reach expected state: {window.status.get()}")

    def add(self, window, source=None, folder=None):
        window.buttons["add"].invoke()
        dialog = window.add_dialog
        dialog.source.set(str(source or self.torrent_path))
        dialog.folder.set(str(folder or self.folder))
        dialog.add_button.invoke()
        window.root.update()
        return dialog

    def select(self, window, item):
        window.queue.selection_set(item.id)
        window.queue.event_generate("<<TreeviewSelect>>")
        window.root.update()

    def test_add_file_and_remember_folder_across_reopen(self):
        window = self.window()
        dialog = self.add(window)
        self.assertFalse(dialog.window.winfo_exists())
        item = window.session.items()[0]
        self.assertEqual(item.output, self.folder / self.meta.name)
        self.assertEqual(item.status, "queued")
        self.assertFalse(self.folder.exists(), "Adding must not create payload files")
        window.close()
        reopened = self.window()
        self.assertEqual(reopened.session.download_folder, self.folder)
        self.assertEqual(reopened.session.items()[0].output, item.output)
        reopened.buttons["add"].invoke()
        self.assertEqual(reopened.add_dialog.folder.get(), str(self.folder))

    def test_add_magnet_in_same_dialog_uses_hash_not_display_path(self):
        window = self.window()
        uri = "magnet:?xt=urn:btih:" + self.meta.info_hash.hex() + "&dn=../../outside"
        self.add(window, source=uri)
        item = window.session.items()[0]
        self.assertEqual(item.magnet_uri, uri)
        self.assertEqual(item.output, self.folder / self.meta.info_hash.hex())
        self.assertFalse(item.output.exists())

    def test_invalid_input_and_cancel_leave_queue_and_preferences_unchanged(self):
        window = self.window()
        original = window.session.download_folder
        window.buttons["add"].invoke()
        dialog = window.add_dialog
        dialog.folder.set(str(self.folder))
        for value in ("", "magnet:?xt=invalid", str(self.directory / "missing.torrent")):
            dialog.source.set(value)
            dialog.add_button.invoke()
            self.assertTrue(dialog.error.get())
            self.assertTrue(dialog.window.winfo_exists())
            self.assertEqual(window.session.items(), [])
            self.assertEqual(window.session.download_folder, original)
        dialog.cancel_button.invoke()
        self.assertFalse(dialog.window.winfo_exists())
        self.assertFalse(self.session_path.exists())

    def test_file_instead_of_folder_and_duplicate_are_actionable(self):
        window = self.window()
        window.buttons["add"].invoke()
        dialog = window.add_dialog
        dialog.source.set(str(self.torrent_path))
        dialog.folder.set(str(self.source))
        dialog.add_button.invoke()
        self.assertIn("Choose a directory", dialog.error.get())
        dialog.folder.set(str(self.folder))
        dialog.add_button.invoke()
        duplicate = self.add(window)
        self.assertIn("already in session", duplicate.error.get())
        self.assertEqual(len(window.session.items()), 1)

    def test_browse_buttons_and_single_dialog(self):
        window = self.window()
        window.buttons["add"].invoke()
        dialog = window.add_dialog
        window.buttons["add"].invoke()
        self.assertIs(window.add_dialog, dialog)
        with patch("tkinter.filedialog.askopenfilename", return_value=str(self.torrent_path)), \
             patch("tkinter.filedialog.askdirectory", return_value=str(self.folder)):
            dialog.browse_button.invoke()
            dialog.folder_button.invoke()
        self.assertEqual(dialog.source.get(), str(self.torrent_path))
        self.assertEqual(dialog.folder.get(), str(self.folder))
        dialog.add_button.invoke()
        self.assertEqual(len(window.session.items()), 1)

    def test_save_failure_keeps_dialog_open_and_rolls_back_session(self):
        window = self.window()
        original = window.session.download_folder
        with patch.object(window.session, "save", side_effect=OSError("disk full")):
            dialog = self.add(window)
        self.assertIn("disk full", dialog.error.get())
        self.assertTrue(dialog.window.winfo_exists())
        self.assertEqual(window.session.items(), [])
        self.assertEqual(window.session.download_folder, original)
        self.assertFalse(self.folder.exists())

    def test_failed_download_error_survives_restart_and_retry_reuses_partial(self):
        window = self.window()
        self.add(window)
        item = window.session.items()[0]
        window.buttons["resume"].invoke()  # No peers: genuine bounded local failure.
        self.pump(window, lambda: item.status == "error")
        self.assertTrue(item.error)
        self.assertIn("Retry", window.status.get())
        output = item.output
        self.assertTrue(output.with_name(output.name + ".part").exists())
        window.close()
        reopened = self.window()
        item = reopened.session.items()[0]
        self.select(reopened, item)
        self.assertTrue(item.error)
        partial = output.with_name(output.name + ".part")
        partial.write_bytes(self.payload)  # Complete payload still must be rehashed.
        reopened.buttons["retry"].invoke()
        self.pump(reopened, lambda: item.status == "complete")
        self.assertEqual(item.output, output)
        self.assertEqual(output.read_bytes(), self.payload)
        self.assertIsNone(item.error)

    def test_retry_never_overwrites_or_renames_existing_destination(self):
        window = self.window()
        self.add(window)
        item = window.session.items()[0]
        item.output.parent.mkdir()
        item.output.write_bytes(b"keep this file")
        window.session.set_status(item.id, "error", error="previous failure")
        self.select(window, item)
        window.buttons["retry"].invoke()
        self.assertEqual(item.status, "error")
        self.assertIn("not be overwritten", item.error)
        self.assertEqual(item.output.read_bytes(), b"keep this file")
        self.assertFalse(item.output.with_name("source-1.bin").exists())

    def test_add_reserves_distinct_destinations_and_preserves_existing_partial(self):
        window = self.window()
        self.folder.mkdir()
        existing = self.folder / self.meta.name
        existing.write_bytes(b"existing payload")
        partial = self.folder / "source-1.bin.part"
        partial.write_bytes(b"existing partial")
        self.add(window)
        first = window.session.items()[0]
        self.assertEqual(first.output, self.folder / "source-2.bin")
        self.source.write_bytes(b"different payload")
        other = self.directory / "different.torrent"
        create(self.source, other)
        self.add(window, source=other)
        self.assertEqual(window.session.items()[1].output, self.folder / "source-3.bin")
        self.assertEqual(existing.read_bytes(), b"existing payload")
        self.assertEqual(partial.read_bytes(), b"existing partial")

    def test_close_during_transfer_preserves_intent_without_late_gui_callbacks(self):
        window = self.window()
        self.add(window)
        item = window.session.items()[0]
        entered = threading.Event()
        async def transfer(*args, observe, **options):
            entered.set()
            await asyncio.Future()
        with patch("cbtorrent.gui.controller.download", side_effect=transfer):
            window.buttons["start_queue"].invoke()
            self.assertTrue(entered.wait(3))
            window.close()
        self.assertFalse(window.controller.busy)
        restored = Session(self.session_path)
        restored.load()
        self.assertTrue(restored.queue_running)
        self.assertEqual(restored.get(item.id).status, "downloading")
        window.close()  # Repeated close is harmless.

    def test_open_folder_routes_only_directories_and_reports_missing(self):
        window = self.window()
        self.add(window)
        item = window.session.items()[0]
        with patch("cbtorrent.gui.queue_window.open_folder") as opener:
            window.buttons["open_folder"].invoke()
            opener.assert_called_once_with(item.output.parent)
            item.output.mkdir(parents=True)
            window.buttons["open_folder"].invoke()
            self.assertEqual(opener.call_args.args[0], item.output)
        with patch("cbtorrent.gui.queue_window.open_folder", side_effect=OSError("folder unavailable")):
            window.buttons["open_folder"].invoke()
        self.assertEqual(window.status.get(), "folder unavailable")

    def test_pause_and_close_drain_real_local_transfer_and_reopen_resume(self):
        # Long enough to observe and pause a verified piece before completion.
        self.payload = b"gui pause workflow\x00\xff" * 20000
        self.source.write_bytes(self.payload)
        self.torrent_path = self.directory / "slow.torrent"
        self.meta = create(self.source, self.torrent_path, piece_length=16384)
        ready, stop = threading.Event(), threading.Event()
        endpoints, errors = [], []
        async def seed():
            source = FileSource(self.meta, self.source)
            server = SeedServer(self.meta, source, latency=0.1)
            try:
                port = await server.start("127.0.0.1", 0)
                endpoints.append(("127.0.0.1", port))
                ready.set()
                while not stop.is_set():
                    await asyncio.sleep(0.005)
            finally:
                await server.close()
                source.close()
        def worker():
            try:
                asyncio.run(seed())
            except BaseException as error:
                errors.append(error)
                ready.set()
        thread = threading.Thread(target=worker)
        thread.start()
        try:
            self.assertTrue(ready.wait(5))
            self.assertFalse(errors)
            window = self.window(peers=endpoints, pipeline=1, concurrency=1)
            self.add(window)
            item = window.session.items()[0]
            window.buttons["resume"].invoke()
            self.pump(window, lambda: window.controller.snapshot.verified_bytes >= 16384)
            window.buttons["pause"].invoke()
            self.pump(window, lambda: not window.controller.busy)
            self.assertEqual(item.status, "paused")
            self.assertFalse(item.output.exists())
            window.close()
            reopened = self.window(peers=endpoints, pipeline=1, concurrency=1)
            item = reopened.session.items()[0]
            self.select(reopened, item)
            reopened.buttons["resume"].invoke()
            self.pump(reopened, lambda: item.status == "complete")
            self.assertEqual(item.output.read_bytes(), self.payload)
            self.assertGreater(reopened.controller.snapshot.resumed_bytes, 0)
        finally:
            stop.set()
            thread.join(5)
            self.assertFalse(thread.is_alive())
            self.assertFalse(errors)


class FolderLaunchTests(unittest.TestCase):
    def test_os_launch_uses_argument_list_and_existing_directory(self):
        from cbtorrent.gui.folders import open_folder
        with tempfile.TemporaryDirectory(prefix="folder with spaces ") as directory:
            path = Path(directory).absolute()
            for platform, command in (("linux", "xdg-open"), ("darwin", "open")):
                with patch("cbtorrent.gui.folders.sys.platform", platform), \
                     patch("cbtorrent.gui.folders.subprocess.Popen") as launch:
                    open_folder(path)
                    self.assertEqual(launch.call_args.args[0], [command, str(path)])
            with patch("cbtorrent.gui.folders.sys.platform", "win32"), \
                 patch("cbtorrent.gui.folders.os.startfile", create=True) as launch:
                open_folder(path)
                launch.assert_called_once_with(str(path))
            with self.assertRaises(FileNotFoundError):
                open_folder(path / "missing")


class PreferenceTests(unittest.TestCase):
    def test_legacy_session_and_bad_folder_keep_valid_default(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "session.json"
            for folder in (None, [], "", "\x00", "x" * 4097):
                path.write_text(json.dumps({"version": 1, "items": [], "download_folder": folder}))
                session = Session(path, download_folder=Path(directory) / "preferred")
                session.load()
                self.assertEqual(session.download_folder, Path(directory) / "preferred")
