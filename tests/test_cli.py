import asyncio
import json
import sys
import tempfile
import unittest
from pathlib import Path


class CommandTests(unittest.IsolatedAsyncioTestCase):
    async def command(self, *args):
        process = await asyncio.create_subprocess_exec(
            sys.executable, "-m", "cbtorrent", *map(str, args),
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        try:
            stdout, stderr = await asyncio.wait_for(process.communicate(), 15)
            self.assertEqual(process.returncode, 0, stderr.decode())
            return stdout.decode()
        finally:
            if process.returncode is None:
                process.kill()
                await process.wait()

    async def test_create_inspect_seed_and_download_in_separate_processes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, torrent, output = root / "source", root / "test.torrent", root / "downloads" / "result"
            content = b"independent processes\x00\xff" * 10000
            source.write_bytes(content)
            await self.command("create", source, "--output", torrent, "--piece-length", 32768)
            info = json.loads(await self.command("inspect", torrent))
            self.assertEqual(info["length"], len(content))
            seed = await asyncio.create_subprocess_exec(
                sys.executable, "-m", "cbtorrent", "seed", str(torrent), "--file", str(source),
                "--listen-host", "127.0.0.1", "--port", "0", "--no-trackers",
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            try:
                line = await asyncio.wait_for(seed.stdout.readline(), 10)
                self.assertIn(b"Seeding", line)
                port = int(line.strip().rsplit(b":", 1)[1])
                for policy in ("bandit", "recovery", "timed"):
                    with self.subTest(policy=policy):
                        report_path = root / "reports" / f"{policy}.json"
                        target = output.with_name(policy)
                        report = json.loads(await self.command(
                            "download", torrent, "--peer", f"127.0.0.1:{port}", "--no-trackers",
                            "--output", target, "--listen-host", "127.0.0.1", "--policy", policy,
                            "--report", report_path))
                        self.assertEqual(target.read_bytes(), content)
                        self.assertTrue(report["complete"])
                        self.assertEqual(json.loads(report_path.read_text()), report)
                        if policy in ("recovery", "timed"):
                            self.assertEqual(report["policy_diagnostics"]["training_bytes"], len(content))
                        if policy == "timed":
                            self.assertEqual(report["policy_diagnostics"]["probe_pending_seconds"], 0)
            finally:
                if seed.returncode is None:
                    seed.terminate()
                await asyncio.wait_for(seed.communicate(), 5)
