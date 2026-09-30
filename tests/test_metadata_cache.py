"""Local cache integrity, resource limits, failure, and restart workflows."""
import asyncio
import os
import subprocess
import sys
import tempfile
import threading
import unittest
from dataclasses import replace
from hashlib import sha1
from pathlib import Path
from unittest.mock import patch

from cbtorrent.bencode import encode
from cbtorrent.client import DownloadError
from cbtorrent.extensions import MAX_METADATA
from cbtorrent.magnet import Magnet, download_magnet, resolve
from cbtorrent.metadata_cache import MetadataCache, verified_metadata
from cbtorrent.metainfo import Torrent
from cbtorrent.seeder import FileSource, SeedServer
from cbtorrent.storage import Storage


def torrent(content=b"hello", name=b"source", private=0):
    return Torrent.from_bytes(encode({b"info": {b"name": name, b"length": len(content),
        b"piece length": 16384, b"pieces": b"".join(sha1(content[i:i+16384]).digest()
                                                  for i in range(0, len(content), 16384)),
        b"private": private}}))


def magnet(meta):
    return Magnet.parse("magnet:?xt=urn:btih:" + meta.info_hash.hex())


class CacheTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.cache = MetadataCache(self.root / "cache")
        self.meta = torrent()

    def target(self, meta=None):
        return self.cache.path / ((meta or self.meta).info_hash.hex() + ".info")

    def test_roundtrip_new_instance_only_info_not_discovery(self):
        self.assertIsNone(self.cache.get(self.meta.info_hash))
        meta = replace(self.meta, trackers=("https://tracker.invalid/secret",), nodes=(("localhost", 44),))
        self.cache.put(meta)
        loaded = MetadataCache(self.cache.path).get(meta.info_hash)
        self.assertEqual(loaded, self.meta)
        self.assertEqual(self.target().read_bytes(), meta.info_bytes)

    def test_hash_rechecked_on_every_read(self):
        self.cache.put(self.meta)
        self.cache.get(self.meta.info_hash)
        self.target().write_bytes(torrent(name=b"other").info_bytes)
        with self.assertRaisesRegex(ValueError, "info-hash"):
            self.cache.get(self.meta.info_hash)

    def test_manifest_and_canonical_encoding_rechecked(self):
        self.cache.path.mkdir()
        for raw in (encode({b"name": b"../escape", b"length": 0, b"piece length": 1, b"pieces": b""}),
                    b"d1:bi0e1:ai0ee", b"d4:name1:xe", b"d4:name1:xeextra"):
            digest = sha1(raw).digest()
            (self.cache.path / (digest.hex() + ".info")).write_bytes(raw)
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                self.cache.get(digest)

    def test_oversized_file_rejected_without_reading(self):
        self.cache.path.mkdir()
        with self.target().open("wb") as stream:
            stream.truncate(MAX_METADATA + 1)
        with self.assertRaisesRegex(ValueError, "file"):
            self.cache.get(self.meta.info_hash)

    def test_private_or_forged_torrent_not_published(self):
        for meta in (torrent(private=1), replace(self.meta, info_hash=b"x" * 20),
                     replace(self.meta, info_bytes=b"invalid")):
            with self.subTest(meta=meta), self.assertRaises(ValueError):
                self.cache.put(meta)
        self.assertFalse(self.cache.path.exists())

    def test_bounded_entries_evict_oldest_and_preserve_unknown_files(self):
        self.cache = MetadataCache(self.root / "cache", max_entries=2)
        metas = [torrent(name=str(i).encode()) for i in range(3)]
        for i, meta in enumerate(metas[:2]):
            self.cache.put(meta)
            os.utime(self.target(meta), ns=((i + 1) * 10**9, (i + 1) * 10**9))
        unknown = self.cache.path / "my-document"
        unknown.write_bytes(b"keep")
        self.cache.put(metas[2])
        self.assertIsNone(self.cache.get(metas[0].info_hash))
        self.assertIsNotNone(self.cache.get(metas[1].info_hash))
        self.assertEqual(len(list(self.cache.path.glob("*.info"))), 2)
        self.assertEqual(unknown.read_bytes(), b"keep")

    def test_byte_budget_and_replacement_do_not_count_target_twice(self):
        self.cache = MetadataCache(self.root / "cache", max_bytes=len(self.meta.info_bytes))
        self.cache.put(self.meta)
        self.cache.put(self.meta)
        other = torrent(name=b"longer-name")
        with self.assertRaisesRegex(ValueError, "capacity"):
            self.cache.put(other)
        other = torrent(name=b"short")
        self.cache.put(other)
        self.assertIsNone(self.cache.get(self.meta.info_hash))
        self.assertLessEqual(sum(p.stat().st_size for p in self.cache.path.glob("*.info")), self.cache.max_bytes)

    def test_failed_flush_and_replace_leave_previous_file_and_no_temp(self):
        self.cache.put(self.meta)
        for operation in ("fsync", "replace"):
            with self.subTest(operation=operation), patch("cbtorrent.metadata_cache.os." + operation,
                                                         side_effect=OSError("disk failure")):
                with self.assertRaises(OSError):
                    self.cache.put(self.meta)
            self.assertEqual(self.cache.get(self.meta.info_hash), self.meta)
            self.assertEqual(list(self.cache.path.glob("*.tmp")), [])
        self.cache.put(self.meta)  # A failure released the process lock.

    def test_read_during_publication_sees_complete_previous_file(self):
        self.cache.put(self.meta)
        original = os.replace
        def replacing(source, target):
            self.assertEqual(self.cache.get(self.meta.info_hash), self.meta)
            original(source, target)
        with patch("cbtorrent.metadata_cache.os.replace", side_effect=replacing):
            self.cache.put(self.meta)

    def test_cross_process_publication_lock_is_nonblocking(self):
        self.cache.put(self.meta)
        code = ("import sys; from cbtorrent.metadata_cache import MetadataCache; "
                "c=MetadataCache(sys.argv[1]); "
                "\nwith c._lock():\n print('locked',flush=True)\n sys.stdin.readline()\n")
        process = subprocess.Popen([sys.executable, "-c", code, str(self.cache.path)],
                                   stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            self.assertEqual(process.stdout.readline().strip(), "locked")
            with self.assertRaises(OSError):
                self.cache.put(self.meta)
            self.assertEqual(self.cache.get(self.meta.info_hash), self.meta)
        finally:
            process.communicate("release\n", timeout=5)
        self.assertEqual(process.returncode, 0)
        self.cache.put(self.meta)

    def test_scan_limit_preserves_unowned_entries(self):
        self.cache.path.mkdir()
        for i in range(4096):
            (self.cache.path / str(i)).touch()
        with self.assertRaisesRegex(ValueError, "scan limit"):
            self.cache.put(self.meta)
        self.assertFalse(self.target().exists())
        self.assertEqual(len(list(self.cache.path.glob("[0-9]*"))), 4096)

    def test_hard_links_and_special_targets_refused(self):
        self.cache.path.mkdir()
        source = self.root / "outside"
        source.write_bytes(self.meta.info_bytes)
        os.link(source, self.target())
        with self.assertRaises(ValueError):
            self.cache.get(self.meta.info_hash)
        with self.assertRaises(ValueError):
            self.cache.put(self.meta)
        self.assertEqual(source.read_bytes(), self.meta.info_bytes)
        self.target().unlink()
        self.target().mkdir()
        with self.assertRaises(ValueError):
            self.cache.put(self.meta)

    def test_symlinks_refused_without_touching_destination(self):
        source = self.root / "outside"
        source.write_bytes(self.meta.info_bytes)
        self.cache.path.mkdir()
        try:
            self.target().symlink_to(source)
        except OSError:
            self.skipTest("symlinks unavailable on this platform")
        with self.assertRaises(ValueError):
            self.cache.get(self.meta.info_hash)
        with self.assertRaises(ValueError):
            self.cache.put(self.meta)
        self.assertEqual(source.read_bytes(), self.meta.info_bytes)

    def test_limits_and_hash_keys(self):
        for options in ({"max_entries": 0}, {"max_entries": True}, {"max_entries": 129},
                        {"max_bytes": 0}, {"max_bytes": 64 * 1024 * 1024 + 1}):
            with self.subTest(options=options), self.assertRaises(ValueError):
                MetadataCache(self.root, **options)
        for key in ("../x", b"x", b"x" * 21):
            with self.assertRaises(ValueError):
                self.cache.get(key)
        with self.assertRaises(ValueError):
            verified_metadata(self.meta.info_bytes, b"x" * 20)

    def test_linked_lock_does_not_modify_external_file(self):
        self.cache.path.mkdir()
        outside = self.root / "outside"
        outside.write_bytes(b"untouched")
        os.link(outside, self.cache.path / ".lock")
        with self.assertRaisesRegex(ValueError, "lock"):
            self.cache.put(self.meta)
        self.assertEqual(outside.read_bytes(), b"untouched")
        self.assertFalse(self.target().exists())

    @unittest.skipUnless(hasattr(os, "mkfifo"), "named pipes unavailable on this platform")
    def test_named_pipe_does_not_block_read_or_write(self):
        self.cache.path.mkdir()
        os.mkfifo(self.target())
        with self.assertRaises(ValueError):
            self.cache.get(self.meta.info_hash)
        with self.assertRaises(ValueError):
            self.cache.put(self.meta)
        self.target().unlink()
        os.mkfifo(self.cache.path / ".lock")
        with self.assertRaises(ValueError):
            self.cache.put(self.meta)


class CacheWorkflowTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.content = b"verified resume" * 2500
        self.meta = torrent(self.content)
        self.link = magnet(self.meta)
        self.cache = MetadataCache(self.root / "cache")
        self.output = self.root / "download"
        self.source_path = self.root / "source"
        self.source_path.write_bytes(self.content)
        self.source = FileSource(self.meta, self.source_path)
        self.server = SeedServer(self.meta, self.source)
        port = await self.server.start("127.0.0.1", 0)
        self.address = ("127.0.0.1", port)

    async def asyncTearDown(self):
        await self.server.close()
        self.source.close()
        self.temp.cleanup()

    async def lookup(self, **options):
        return await resolve(self.link, [self.address], use_trackers=False, use_dht=False,
                             metadata_cache=self.cache, **options)

    async def test_restart_uses_zero_metadata_connections_and_current_hints(self):
        _, _, first = await self.lookup()
        self.assertFalse(first["cache_hit"])
        self.assertGreater(first["connections"], 0)
        self.cache = MetadataCache(self.cache.path)
        new = replace(self.link, trackers=("https://new.invalid/announce",), peers=(("localhost", 42),))
        with patch("cbtorrent.magnet.fetch_metadata", side_effect=AssertionError("network lookup")):
            meta, addresses, second = await resolve(new, [], metadata_cache=self.cache)
        self.assertEqual(meta.trackers, new.trackers)
        self.assertEqual(addresses, new.peers)
        self.assertTrue(second["cache_hit"])
        for counter in ("connections", "wire_sent_bytes", "wire_received_bytes", "tracker_requests", "dht_requests"):
            self.assertEqual(second[counter], 0)

    async def test_cli_cache_survives_process_restart(self):
        uri = self.link.uri + f"&x.pe=127.0.0.1:{self.address[1]}"
        for i in range(2):
            output = self.root / f"cli-{i}"
            process = await asyncio.create_subprocess_exec(
                sys.executable, "-m", "cbtorrent", "download", uri, "--output", str(output),
                "--no-trackers", "--no-dht", "--metadata-cache-dir", str(self.cache.path),
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            try:
                out, err = await asyncio.wait_for(process.communicate(), 10)
            finally:
                if process.returncode is None:
                    process.kill()
                    await process.wait()
            self.assertEqual(process.returncode, 0, err.decode())
            import json
            report = json.loads(out)
            self.assertEqual(report["metadata"]["cache_hit"], bool(i))
            self.assertEqual(output.read_bytes(), self.content)

    async def test_offline_complete_resume_still_rehashes_payload(self):
        self.cache.put(self.meta)
        storage = Storage(self.meta, self.output)
        try:
            for i in range(len(self.meta.hashes)):
                storage.write(i, self.content[i * self.meta.piece_length:(i+1) * self.meta.piece_length])
        finally:
            storage.close()
        with patch("cbtorrent.magnet.fetch_metadata", side_effect=AssertionError("network lookup")):
            report = await download_magnet(self.link, [], self.output, resume=True,
                         metadata_cache=self.cache, use_trackers=False, use_dht=False)
        self.assertTrue(report["complete"])
        self.assertTrue(report["metadata"]["cache_hit"])
        self.assertEqual(report["resumed_bytes"], len(self.content))
        self.assertEqual(report["payload_received_bytes"], 0)
        self.assertEqual(self.output.read_bytes(), self.content)

    async def test_corrupt_partial_payload_is_not_trusted_on_cache_hit(self):
        self.cache.put(self.meta)
        storage = Storage(self.meta, self.output)
        storage.write(0, self.content[:self.meta.piece_length])
        storage.close()
        partial = Path(str(self.output) + ".part")
        with partial.open("r+b") as stream:
            stream.write(b"BAD")
        report = await download_magnet(self.link, [self.address], self.output, resume=True,
                     metadata_cache=self.cache, use_trackers=False, use_dht=False)
        self.assertEqual(report["resumed_bytes"], 0)
        self.assertEqual(report["verified_bytes"], len(self.content))
        self.assertEqual(self.output.read_bytes(), self.content)

    async def test_corrupt_cache_falls_back_and_repairs(self):
        self.cache.put(self.meta)
        target = self.cache.path / (self.meta.info_hash.hex() + ".info")
        target.write_bytes(b"corrupt")
        _, _, report = await self.lookup()
        self.assertFalse(report["cache_hit"])
        self.assertEqual(len(report["cache_errors"]), 1)
        self.assertEqual(self.cache.get(self.meta.info_hash), self.meta)

    async def test_unwritable_cache_is_optional(self):
        with patch.object(self.cache, "get", side_effect=PermissionError("read denied")), \
             patch.object(self.cache, "put", side_effect=PermissionError("write denied")):
            meta, _, report = await self.lookup()
        self.assertEqual(meta, self.meta)
        self.assertEqual(len(report["cache_errors"]), 2)

    async def test_valid_private_cache_refuses_discovery_and_payload(self):
        private = torrent(private=1)
        self.cache.path.mkdir()
        (self.cache.path / (private.info_hash.hex() + ".info")).write_bytes(private.info_bytes)
        with patch("cbtorrent.magnet.fetch_metadata", side_effect=AssertionError("private discovery")):
            with self.assertRaises(DownloadError) as caught:
                await download_magnet(magnet(private), [], self.output, metadata_cache=self.cache)
        self.assertFalse(caught.exception.report["complete"])
        self.assertEqual(caught.exception.report["metadata"]["connections"], 0)
        self.assertFalse(self.output.exists())

    async def test_invalid_options_rejected_on_cache_hit(self):
        self.cache.put(self.meta)
        for options in ({"timeout": 0}, {"peer_timeout": float("nan")}, {"concurrency": 0}):
            with self.subTest(options=options), self.assertRaises(ValueError):
                await self.lookup(**options)
        with self.assertRaises(ValueError):
            await resolve(self.link, [self.address] * 201, metadata_cache=self.cache)

    async def test_cancelled_cache_worker_is_drained(self):
        entered, release, finished = threading.Event(), threading.Event(), threading.Event()
        def slow_get(digest):
            entered.set()
            if not release.wait(5):
                raise RuntimeError("test worker release deadline")
            finished.set()
            return None
        with patch.object(self.cache, "get", side_effect=slow_get):
            task = asyncio.create_task(self.lookup())
            while not entered.is_set():
                await asyncio.sleep(0.001)
            task.cancel()
            await asyncio.sleep(0.01)
            self.assertFalse(task.done())
            release.set()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertTrue(finished.is_set())
        self.assertFalse(self.cache.path.exists())

    async def test_cancelled_publication_finishes_before_return(self):
        entered, release, finished = threading.Event(), threading.Event(), threading.Event()
        original = self.cache.put
        def slow_put(meta):
            entered.set()
            if not release.wait(5):
                raise RuntimeError("test worker release deadline")
            original(meta)
            finished.set()
        with patch.object(self.cache, "put", side_effect=slow_put):
            task = asyncio.create_task(self.lookup())
            while not entered.is_set():
                await asyncio.sleep(0.001)
            task.cancel()
            await asyncio.sleep(0.01)
            self.assertFalse(task.done())
            task.cancel()
            await asyncio.sleep(0.01)
            self.assertFalse(task.done())
            release.set()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertTrue(finished.is_set())
        self.assertEqual(self.cache.get(self.meta.info_hash), self.meta)
        self.assertFalse(self.output.exists())
