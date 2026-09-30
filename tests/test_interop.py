"""Harness readiness regressions, using only bounded loopback fixtures."""
import asyncio
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from integration.qbittorrent import Qbittorrent


class ReadinessTests(unittest.IsolatedAsyncioTestCase):
    async def fixture(self, handler):
        root = tempfile.TemporaryDirectory()
        self.addCleanup(root.cleanup)
        qbit = Qbittorrent("unused-test-binary", Path(root.name))
        server = await asyncio.start_server(handler, "127.0.0.1", 0)
        self.addAsyncCleanup(server.wait_closed)
        self.addCleanup(server.close)
        qbit.peer_port = server.sockets[0].getsockname()[1]
        return qbit, SimpleNamespace(info_hash=b"i" * 20)

    async def test_api_ready_but_peer_handshake_not_ready_is_retried(self):
        requests = []
        async def handler(reader, writer):
            try:
                request = await reader.readexactly(68)
                requests.append(request)
                if len(requests) > 1:
                    writer.write(request[:48] + b"r" * 20)
                    await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()
        qbit, torrent = await self.fixture(handler)
        await asyncio.wait_for(qbit.seed_ready(torrent, timeout=2), 3)
        self.assertEqual(len(requests), 2)
        self.assertNotEqual(requests[0][48:], requests[1][48:])

    async def test_wrong_info_hash_never_counts_as_ready_and_has_deadline(self):
        requests = []
        async def handler(reader, writer):
            try:
                request = await reader.readexactly(68)
                requests.append(request)
                writer.write(request[:28] + b"x" * 20 + b"r" * 20)
                await writer.drain()
            except asyncio.IncompleteReadError:
                pass  # The final probe can hit its deadline before writing.
            finally:
                writer.close()
                await writer.wait_closed()
        qbit, torrent = await self.fixture(handler)
        with self.assertRaises(TimeoutError):
            await asyncio.wait_for(qbit.seed_ready(torrent, timeout=0.3), 2)
        self.assertGreater(len(requests), 0)

    async def test_cancellation_closes_pending_readiness_connection(self):
        connected, disconnected = asyncio.Event(), asyncio.Event()
        async def handler(reader, writer):
            try:
                await reader.readexactly(68)
                connected.set()
                await reader.read()  # Silent remote; cancellation must close our socket.
            finally:
                writer.close()
                await writer.wait_closed()
                disconnected.set()
        qbit, torrent = await self.fixture(handler)
        task = asyncio.create_task(qbit.seed_ready(torrent))
        try:
            await asyncio.wait_for(connected.wait(), 2)
        finally:
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        await asyncio.wait_for(disconnected.wait(), 2)
