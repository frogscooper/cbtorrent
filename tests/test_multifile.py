import asyncio
import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from cbtorrent.bencode import decode, encode
from cbtorrent.client import DownloadError, download
from cbtorrent.metainfo import Torrent, create
from cbtorrent.seeder import FileSource, SeedServer
from cbtorrent.session import Session
from cbtorrent.storage import Storage
from cbtorrent.metrics import Metrics
from cbtorrent.wire import Peer


class Fixture:
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.source = self.root / "bundle"
        (self.source / "nested").mkdir(parents=True)
        self.contents = {"a": b"hello world", "nested/empty": b"",
                         "nested/long": bytes(range(256)) * 274 + b"tail", "z": b"end"}
        for name, data in self.contents.items():
            (self.source / name).write_bytes(data)
        self.meta_path = self.root / "bundle.torrent"
        self.torrent = create(self.source, self.meta_path, piece_length=32768)
        self.data = b"".join(self.contents[name] for name in sorted(self.contents))
        self.output = self.root / "result"

    def assert_directory(self, directory):
        self.assertEqual({str(p.relative_to(directory)).replace(os.sep, "/"): p.read_bytes()
                          for p in directory.rglob("*") if p.is_file()}, self.contents)

    def fill(self, storage):
        for index in range(len(self.torrent.hashes)):
            start = index * self.torrent.piece_length
            storage.write(index, self.data[start:start + self.torrent.piece_size(index)])


class MetadataTests(Fixture, unittest.TestCase):
    def test_manifest_and_hashes_cross_file_boundaries(self):
        self.assertTrue(self.torrent.multi_file)
        self.assertEqual(self.torrent.length, len(self.data))
        self.assertEqual([f.offset for f in self.torrent.files], [0, 11, 11, 70159])
        self.assertEqual(["/".join(f.path) for f in self.torrent.files], sorted(self.contents))
        expected = tuple(hashlib.sha1(self.data[i:i + 32768]).digest()
                         for i in range(0, len(self.data), 32768))
        self.assertEqual(self.torrent.hashes, expected)
        info = decode(self.meta_path.read_bytes())[b"info"]
        self.assertEqual(self.torrent.info_hash, hashlib.sha1(encode(info)).digest())
        self.assertEqual(create(self.source, self.root / "again.torrent", piece_length=32768),
                         self.torrent)

    def test_rejects_unsafe_and_conflicting_paths(self):
        base = decode(self.meta_path.read_bytes())
        paths = [[b".."], [b""], [], [b"/absolute"], [b"a/b"], [b"a\\b"],
                 [b"C:drive"], [b"x\x00y"], [b"CON.txt"], [b"file."], [b"file "],
                 [b"\xff"], [b"."]]
        for path in paths:
            with self.subTest(path=path), self.assertRaises(ValueError):
                root = {b"info": dict(base[b"info"])}
                root[b"info"][b"files"] = [{b"path": path, b"length": 0}]
                Torrent.from_bytes(encode(root))
        for paths in (([b"a"], [b"A"]), ([b"a"], [b"a", b"b"]),
                      ([b"a", b"b"], [b"a"]), ([b"\xc3\xa9"], [b"e\xcc\x81"])):
            with self.subTest(paths=paths), self.assertRaises(ValueError):
                info = dict(base[b"info"])
                info[b"files"] = [{b"path": p, b"length": 0} for p in paths]
                Torrent.from_bytes(encode({b"info": info}))

    def test_rejects_invalid_manifest(self):
        base = decode(self.meta_path.read_bytes())[b"info"]
        for files in ([], {}, [1], [{b"path": [b"a"], b"length": -1}],
                      [{b"path": [b"a"], b"length": 1 << 63}],
                      [{b"path": [b"a"], b"length": 0, b"attr": b"l"}],
                      [{b"path": [b"a"] * 65, b"length": 0}]):
            with self.subTest(files=files), self.assertRaises(ValueError):
                Torrent.from_bytes(encode({b"info": base | {b"files": files}}))
        for changes in ({b"length": 0}, {b"meta version": 2}, {b"pieces": b""},
                        {b"name": b"../bundle"}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                Torrent.from_bytes(encode({b"info": base | changes}))

    def test_source_links_and_empty_directory_are_rejected(self):
        empty = self.root / "empty"
        empty.mkdir()
        with self.assertRaises(ValueError):
            create(empty, self.root / "empty.torrent")
        try:
            (self.source / "linked").symlink_to(self.source / "a")
        except OSError:
            self.skipTest("symlinks unavailable")
        with self.assertRaises(ValueError):
            create(self.source, self.root / "link.torrent")
        self.assertFalse((self.root / "link.torrent").exists())

    def test_gui_session_uses_safe_directory_destination(self):
        session = Session(self.root / "session.json")
        item = session.add(self.meta_path)
        self.assertEqual(item.output, Path("downloads/bundle"))
        restored = Session(session.path)
        restored.load()
        self.assertEqual(restored.get(item.id).output, item.output)


class StorageTests(Fixture, unittest.IsolatedAsyncioTestCase):
    async def test_many_files_and_empty_files_inside_one_piece(self):
        small = self.root / "small"
        small.mkdir()
        for index in range(40):
            (small / f"{index:02}").write_bytes(b"" if index % 3 == 0 else bytes([index]))
        torrent = create(small, self.root / "small.torrent", piece_length=9)
        source = FileSource(torrent, small)
        storage = Storage(torrent, self.output)
        try:
            for index in range(len(torrent.hashes)):
                storage.write(index, source.read(index, 0, torrent.piece_size(index)))
            await storage.publish_async()
            self.assertEqual([p.read_bytes() for p in sorted(self.output.iterdir())],
                             [p.read_bytes() for p in sorted(small.iterdir())])
        finally:
            storage.close()
            source.close()

    async def test_hardlinked_partial_payload_is_rejected_without_modifying_original(self):
        original = self.root / "original"
        original.write_bytes(b"keep")
        try:
            os.link(original, self.output.with_name("result.part"))
        except OSError:
            self.skipTest("hard links unavailable")
        with self.assertRaises(ValueError):
            Storage(self.torrent, self.output, resume=True)
        self.assertEqual(original.read_bytes(), b"keep")

    async def test_publish_and_seed_blocks_span_files(self):
        storage = Storage(self.torrent, self.output)
        try:
            with self.assertRaises(ValueError):
                storage.write(0, b"X" * 32768)
            self.assertEqual(storage.verified, set())
            self.fill(storage)
            self.assertEqual(storage.read(0, 0, 100), self.data[:100])
            await storage.publish_async()
        finally:
            storage.close()
        self.assert_directory(self.output)
        self.assertFalse(self.output.with_name("result.part").exists())
        source = FileSource(self.torrent, self.output)
        try:
            self.assertEqual(source.read(0, 0, 100), self.data[:100])
            self.assertEqual(source.read(2, 0, self.torrent.piece_size(2)), self.data[65536:])
        finally:
            source.close()

    async def test_partial_resume_rehashes_corrupt_pieces(self):
        part = self.output.with_name("result.part")
        part.write_bytes(self.data[:32768] + b"X" * 32768)
        storage = Storage(self.torrent, self.output, resume=True)
        try:
            self.assertEqual(storage.verified, {0})
            self.fill(storage)
            await storage.publish_async()
        finally:
            storage.close()
        self.assert_directory(self.output)

    async def test_publication_failure_rolls_back_and_retains_verified_spool(self):
        storage = Storage(self.torrent, self.output)
        self.fill(storage)
        link = os.link
        count = 0

        def fail_second(*args, **kwargs):
            nonlocal count
            count += 1
            if count == 2:
                raise OSError("publication failed")
            return link(*args, **kwargs)

        try:
            with patch("cbtorrent.storage.os.link", side_effect=fail_second):
                with self.assertRaisesRegex(OSError, "publication failed"):
                    await storage.publish_async()
        finally:
            storage.close()
        self.assertFalse(self.output.exists())
        self.assertEqual(self.output.with_name("result.part").read_bytes(), self.data)
        self.assertFalse(list(self.root.glob(".cbtorrent-publish-*")))
        resumed = Storage(self.torrent, self.output, resume=True)
        try:
            self.assertEqual(len(resumed.verified), len(self.torrent.hashes))
            await resumed.publish_async()
        finally:
            resumed.close()
        self.assert_directory(self.output)

    async def test_existing_destination_is_never_replaced(self):
        storage = Storage(self.torrent, self.output)
        self.fill(storage)
        self.output.mkdir()
        (self.output / "mine").write_bytes(b"keep")
        try:
            with self.assertRaises(FileExistsError):
                await storage.publish_async()
        finally:
            storage.close()
        self.assertEqual((self.output / "mine").read_bytes(), b"keep")
        self.assertTrue(self.output.with_name("result.part").exists())

    async def test_cancel_during_publication_retains_spool(self):
        storage = Storage(self.torrent, self.output)
        self.fill(storage)
        linked = asyncio.Event()
        link = os.link

        def notify_link(*args, **kwargs):
            result = link(*args, **kwargs)
            linked.set()
            return result

        try:
            with patch("cbtorrent.storage.os.link", side_effect=notify_link):
                task = asyncio.create_task(storage.publish_async())
                await asyncio.wait_for(linked.wait(), 5)
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
        finally:
            storage.close()
        self.assertFalse(self.output.exists())
        self.assertTrue(self.output.with_name("result.part").exists())

    async def test_seed_rejects_corrupt_missing_and_symlink_files(self):
        (self.source / "a").write_bytes(b"X" * 11)
        with self.assertRaisesRegex(ValueError, "hash"):
            FileSource(self.torrent, self.source)
        (self.source / "a").unlink()
        with self.assertRaises(FileNotFoundError):
            FileSource(self.torrent, self.source)
        outside = self.root / "outside"
        outside.write_bytes(b"hello world")
        try:
            (self.source / "a").symlink_to(outside)
        except OSError:
            self.skipTest("symlinks unavailable")
        with self.assertRaises(ValueError):
            FileSource(self.torrent, self.source)
        self.output.with_name("result.part").symlink_to(outside)
        with self.assertRaises(ValueError):
            Storage(self.torrent, self.output, resume=True)
        self.assertEqual(outside.read_bytes(), b"hello world")


class TransferTests(Fixture, unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.servers = []
        self.sources = []

    async def asyncTearDown(self):
        for server in self.servers:
            await server.close()
        for source in self.sources:
            source.close()

    async def seed(self, path, latency=0):
        source = FileSource(self.torrent, path)
        self.sources.append(source)
        server = SeedServer(self.torrent, source, latency=latency)
        self.servers.append(server)
        return "127.0.0.1", await server.start()

    async def test_tcp_download_and_reseed_directory(self):
        peer = await self.seed(self.source)
        report = await download(self.torrent, [peer], self.output, use_trackers=False)
        self.assertTrue(report["complete"])
        self.assertEqual(report["verified_bytes"], len(self.data))
        self.assert_directory(self.output)
        peer = await self.seed(self.output)
        second = self.root / "second"
        report = await download(self.torrent, [peer], second, use_trackers=False)
        self.assertTrue(report["complete"])
        self.assert_directory(second)

    async def test_incoming_upload_only_exposes_verified_cross_file_piece(self):
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
                self.assertEqual(await peer.download_piece(0, 4), self.data[:32768])
            finally:
                await peer.close()
                await server.close()
        finally:
            storage.close()

    async def test_tcp_resume_only_downloads_missing_verified_bytes(self):
        self.output.with_name("result.part").write_bytes(self.data[:32768])
        peer = await self.seed(self.source)
        report = await download(self.torrent, [peer], self.output, resume=True,
                                use_trackers=False)
        self.assertEqual(report["resumed_bytes"], 32768)
        self.assertEqual(report["payload_received_bytes"], len(self.data) - 32768)
        self.assert_directory(self.output)

    async def test_cancel_download_then_resume(self):
        peer = await self.seed(self.source, latency=0.05)
        started = asyncio.Event()
        def progress(done, total):
            if done:
                started.set()
        task = asyncio.create_task(download(self.torrent, [peer], self.output,
                                             concurrency=1, use_trackers=False,
                                             progress=progress))
        try:
            await asyncio.wait_for(started.wait(), 5)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self.assertFalse(self.output.exists())
        report = await download(self.torrent, [peer], self.output, resume=True,
                                use_trackers=False)
        self.assertGreater(report["resumed_bytes"], 0)
        self.assert_directory(self.output)

    async def test_zero_length_files_publish_without_peers(self):
        for path in self.source.rglob("*"):
            if path.is_file():
                path.write_bytes(b"")
        torrent = create(self.source, self.root / "zeros.torrent")
        report = await download(torrent, [], self.output, use_trackers=False)
        self.assertTrue(report["complete"])
        self.assertEqual(len(list(self.output.rglob("*"))), 5)
        source = FileSource(torrent, self.output)
        self.assertEqual(source.verified, set())
        source.close()

    async def test_cli_create_inspect_seed_download(self):
        async def command(*args):
            process = await asyncio.create_subprocess_exec(
                os.sys.executable, "-m", "cbtorrent", *map(str, args),
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            stdout, stderr = await asyncio.wait_for(process.communicate(), 10)
            self.assertEqual(process.returncode, 0, stderr.decode())
            return stdout
        cli_meta = self.root / "cli.torrent"
        await command("create", self.source, "--output", cli_meta, "--piece-length", 32768)
        inspected = json.loads(await command("inspect", cli_meta))
        self.assertEqual(len(inspected["files"]), 4)
        process = await asyncio.create_subprocess_exec(
            os.sys.executable, "-m", "cbtorrent", "seed", str(cli_meta), "--file",
            str(self.source), "--port", "0", "--listen-host", "127.0.0.1", "--no-trackers",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        try:
            line = await asyncio.wait_for(process.stdout.readline(), 5)
            port = int(line.decode().strip().rsplit(":", 1)[1])
            await command("download", cli_meta, "--output", self.output,
                          "--peer", f"127.0.0.1:{port}", "--no-trackers")
            self.assert_directory(self.output)
        finally:
            if process.returncode is None:
                process.terminate()
            await asyncio.wait_for(process.communicate(), 5)
