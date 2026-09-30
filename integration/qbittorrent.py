"""Local qBittorrent 4.6+/5.x interoperability; no third-party Python packages.

Run: python -m integration.qbittorrent --binary PATH --report NEW_REPORT.json
The executable is an optional test dependency, never a cbtorrent dependency.
"""
import argparse
import asyncio
import base64
import hashlib
import json
import os
import platform
import random
import socket
import subprocess
import tempfile
from pathlib import Path
from time import perf_counter
from urllib.parse import urlencode
from urllib.request import Request, build_opener, ProxyHandler

from cbtorrent.client import download
from cbtorrent.magnet import Magnet, download_magnet
from cbtorrent.metainfo import create
from cbtorrent.metrics import Metrics
from cbtorrent.seeder import FileSource, SeedServer
from cbtorrent.wire import PROTOCOL

MAX_API_RESPONSE = 1024 * 1024


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class Qbittorrent:
    """Own one process and disposable profile; never attach to a personal session."""
    def __init__(self, binary, root, *, pex=False):
        self.binary, self.root = str(Path(binary).resolve()), root.resolve()
        self.web_port, self.peer_port = free_port(), free_port()
        while self.peer_port == self.web_port:
            self.peer_port = free_port()
        self.base = f"http://127.0.0.1:{self.web_port}"
        self.opener = build_opener(ProxyHandler({}))
        self.process = self.log = None
        self.version = self.build = None
        self.pex = pex

    def _request(self, endpoint, fields=None, *, upload=None):
        headers = {"Referer": self.base + "/", "Origin": self.base}
        if upload is not None:
            boundary = "cbtorrent-interop-boundary"
            chunks = []
            for key, value in fields.items():
                chunks.append(f'--{boundary}\r\nContent-Disposition: form-data; name="{key}"\r\n\r\n{value}\r\n'.encode())
            chunks.append(f'--{boundary}\r\nContent-Disposition: form-data; name="torrents"; filename="test.torrent"\r\nContent-Type: application/x-bittorrent\r\n\r\n'.encode())
            chunks.extend((upload, f"\r\n--{boundary}--\r\n".encode()))
            data = b"".join(chunks)
            headers["Content-Type"] = "multipart/form-data; boundary=" + boundary
        else:
            data = urlencode(fields).encode() if fields is not None else None
        request = Request(self.base + "/api/v2/" + endpoint, data=data, headers=headers)
        with self.opener.open(request, timeout=3) as response:
            result = response.read(MAX_API_RESPONSE + 1)
        if len(result) > MAX_API_RESPONSE:
            raise ValueError("qBittorrent API response exceeds limit")
        return result.decode("utf-8")

    async def api(self, endpoint, fields=None, **kwargs):
        return await asyncio.to_thread(self._request, endpoint, fields, **kwargs)

    async def start(self):
        profile = self.root / "profile"
        config = profile / "qBittorrent" / "config"
        config.mkdir(parents=True)
        suffix = ".ini" if os.name == "nt" or platform.system() == "Darwin" else ".conf"
        settings = r"""[BitTorrent]
Session\DHTEnabled=false
Session\PeXEnabled=false
Session\LSDEnabled=false
Session\UPnPEnabled=false
Session\InterfaceAddress=127.0.0.1
Session\Port=PEER_PORT
Session\BTProtocol=1
Session\Encryption=2
[Preferences]
WebUI\Enabled=true
WebUI\Address=127.0.0.1
WebUI\LocalHostAuth=false
WebUI\Username=cbtorrent-interop
WebUI\Password_PBKDF2=@ByteArray(CREDENTIAL)
WebUI\UseUPnP=false
Connection\UPnP=false
Connection\ResolvePeerCountries=false
Advanced\updateCheck=false
[LegalNotice]
Accepted=true
[GUI]
StartUpWindowState=1
"""
        salt = os.urandom(16)
        key = hashlib.pbkdf2_hmac("sha512", os.urandom(32), salt, 100000)
        credential = base64.b64encode(salt) + b":" + base64.b64encode(key)
        settings = settings.replace("CREDENTIAL", credential.decode("ascii"))
        settings = settings.replace("PEER_PORT", str(self.peer_port))
        if self.pex:
            settings = settings.replace("Session\\PeXEnabled=false", "Session\\PeXEnabled=true")
        (config / ("qBittorrent" + suffix)).write_text(settings, encoding="utf-8")
        self.log = (self.root / "qbit-process.log").open("wb")
        options = {}
        if os.name == "nt":
            startup = subprocess.STARTUPINFO()
            startup.dwFlags |= subprocess.STARTF_USESHOWWINDOW
            startup.wShowWindow = subprocess.SW_HIDE
            options.update(startupinfo=startup, creationflags=subprocess.CREATE_NO_WINDOW)
        env = {k: v for k, v in os.environ.items() if not k.startswith("QBT_")}
        self.process = subprocess.Popen(
            [self.binary, "--profile=" + str(profile),
             "--webui-port=" + str(self.web_port)],
            stdin=subprocess.DEVNULL, stdout=self.log, stderr=self.log, env=env, **options)
        deadline = perf_counter() + 20
        last_error = None
        while perf_counter() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError("qBittorrent exited during startup")
            try:
                self.version = (await self.api("app/version")).strip()
                break
            except OSError as error:
                last_error = str(error)
                await asyncio.sleep(0.1)
        else:
            raise TimeoutError("qBittorrent WebUI did not start: " + str(last_error))
        version = tuple(int(part) for part in self.version.lstrip("v").split(".")[:2])
        if version < (4, 6):
            raise ValueError("qBittorrent 4.6+ or 5.x required")
        self.build = json.loads(await self.api("app/buildInfo"))
        prefs = dict(dht=False, pex=self.pex, lsd=False, upnp=False, random_port=False,
                     listen_port=self.peer_port, current_interface_address="127.0.0.1",
                     bittorrent_protocol=1, encryption=2, queueing_enabled=False,
                     max_connec=16, max_connec_per_torrent=8,
                     enable_multi_connections_from_same_ip=True, resolve_peer_countries=False)
        await self.api("app/setPreferences", {"json": json.dumps(prefs)})
        actual = json.loads(await self.api("app/preferences"))
        print("qBittorrent TCP port: " + str(self.peer_port), flush=True)
        for key in ("dht", "pex", "lsd", "upnp", "listen_port", "bittorrent_protocol",
                    "current_interface_address", "encryption", "resolve_peer_countries"):
            if actual.get(key) != prefs[key]:
                raise RuntimeError("qBittorrent did not apply local test setting: " + key)
        if json.loads(await self.api("torrents/info")):
            raise RuntimeError("test profile unexpectedly contains torrents")

    async def add(self, torrent_path, torrent, save_path, *, magnet=None):
        fields = dict(savepath=str(save_path.resolve()), autoTMM="false", paused="false", stopped="false")
        if magnet:
            fields["urls"] = magnet
            result = await self.api("torrents/add", fields)
        else:
            result = await self.api("torrents/add", fields, upload=torrent_path.read_bytes())
        if result.strip() != "Ok.":
            raise RuntimeError("qBittorrent rejected torrent: " + result)
        await self.wait(torrent, lambda row: True)

    async def row(self, torrent):
        rows = json.loads(await self.api("torrents/info?" + urlencode({"hashes": torrent.info_hash.hex()})))
        return rows[0] if rows else None

    async def wait(self, torrent, predicate, *, peer=None, timeout=25):
        deadline, next_peer = perf_counter() + timeout, 0
        row = None
        while perf_counter() < deadline:
            row = await self.row(torrent)
            if row and predicate(row):
                return row
            if peer and row and perf_counter() >= next_peer:
                await self.api("torrents/addPeers", {"hashes": torrent.info_hash.hex(), "peers": f"{peer[0]}:{peer[1]}"})
                next_peer = perf_counter() + 1
            await asyncio.sleep(0.1)
        raise TimeoutError("qBittorrent condition timed out: " + json.dumps(row))

    async def remove(self, torrent):
        await self.api("torrents/delete", {"hashes": torrent.info_hash.hex(), "deleteFiles": "false"})
        deadline = perf_counter() + 5
        while await self.row(torrent) is not None:
            if perf_counter() > deadline:
                raise TimeoutError("qBittorrent did not remove test torrent")
            await asyncio.sleep(0.1)

    async def seed_ready(self, torrent, *, timeout=10):
        """The API can report stalledUP before libtorrent accepts handshakes."""
        deadline = perf_counter() + timeout
        last_error = None
        while perf_counter() < deadline:
            writer = None
            try:
                async with asyncio.timeout(min(1, deadline - perf_counter())):
                    reader, writer = await asyncio.open_connection("127.0.0.1", self.peer_port)
                    writer.write(PROTOCOL + bytes(8) + torrent.info_hash + b"-CI0001-" + os.urandom(12))
                    await writer.drain()
                    reply = await reader.readexactly(68)
                    if reply[:20] != PROTOCOL or reply[28:48] != torrent.info_hash:
                        raise ValueError("seed readiness handshake does not match fixture")
                    return
            except (OSError, ValueError, asyncio.IncompleteReadError, TimeoutError) as error:
                last_error = f"{type(error).__name__}: {error}"
            finally:
                if writer is not None:
                    writer.close()
                    try:
                        await asyncio.wait_for(writer.wait_closed(), 1)
                    except (OSError, TimeoutError):
                        pass
            await asyncio.sleep(0.1)
        raise TimeoutError("qBittorrent seed did not accept fixture handshake: " + str(last_error))

    async def close(self):
        if self.process is not None:
            if self.process.poll() is None:
                try:
                    await self.api("app/shutdown", {})
                except OSError:
                    pass
                try:
                    await asyncio.to_thread(self.process.wait, 5)
                except subprocess.TimeoutExpired:
                    self.process.kill()  # Only the process this harness created.
                    await asyncio.to_thread(self.process.wait, 5)
        if self.log is not None:
            self.log.close()


def verify(torrent, path, expected):
    source = FileSource(torrent, path)  # Independent disk rehash, not a progress flag.
    source.close()
    if torrent.multi_file:
        actual = {tuple(p.relative_to(path).parts): p.read_bytes() for p in path.rglob("*") if p.is_file()}
    else:
        actual = {(): path.read_bytes()}
    if actual != expected:
        raise AssertionError("published files differ from fixture, including paths/empty files")


def log_tail(path, size):
    with path.open("rb") as stream:
        stream.seek(0, 2)
        stream.seek(max(0, stream.tell() - size))
        return stream.read(size).decode("utf-8", errors="replace")


async def run(binary, root, *, pex=False):
    qbit = Qbittorrent(binary, root, pex=pex)
    report = dict(schema_version=1, python=platform.python_version(), platform=platform.platform(),
                  scope="Loopback TCP only; no trackers/DHT/PEX/LSD/NAT mapping. Correctness, not a speed comparison.",
                  cases=[], expected_cases=9, complete=False)
    report["pex_enabled"] = pex
    if pex:
        report["scope"] = "Loopback TCP with PEX enabled; no trackers/DHT/LSD/NAT mapping. Correctness, not a speed comparison."
    try:
        await qbit.start()
        report.update(qbittorrent=qbit.version, build=qbit.build)
        rng = random.Random(5292026)
        seed_root = root / "seed"
        seed_root.mkdir()
        single = seed_root / "single.bin"
        single.write_bytes(rng.randbytes(512 * 1024 + 17))
        tree = seed_root / "tree"
        (tree / "nested").mkdir(parents=True)
        (tree / "first.bin").write_bytes(rng.randbytes(45001))
        (tree / "nested" / "second.bin").write_bytes(rng.randbytes(512 * 1024 + 7))
        (tree / "empty").write_bytes(b"")
        fixtures = []
        for path in (single, tree):
            meta_path = root / (path.name + ".torrent")
            meta = create(path, meta_path, piece_length=32768)
            expected = {f.path: path.joinpath(*f.path).read_bytes() for f in meta.files} if meta.multi_file else {(): path.read_bytes()}
            fixtures.append((meta, meta_path, path, expected))

        async def case(name, action):
            print("Testing " + name, flush=True)
            started = perf_counter()
            try:
                result = await asyncio.wait_for(action(), 45)
                report["cases"].append(dict(name=name, complete=True, seconds=perf_counter() - started, **(result or {})))
            except Exception as error:
                report["cases"].append(dict(name=name, complete=False, seconds=perf_counter() - started,
                                           error=str(error), error_type=type(error).__name__))
                if hasattr(error, "report"):
                    report["cases"][-1]["metrics"] = error.report
            print("  " + ("passed" if report["cases"][-1]["complete"] else report["cases"][-1]["error"]), flush=True)

        for meta, meta_path, path, expected in fixtures:
            kind = "directory" if meta.multi_file else "single"
            await qbit.add(meta_path, meta, seed_root)
            await qbit.wait(meta, lambda row: row["progress"] == 1 and row["state"] in ("uploading", "stalledUP", "queuedUP", "forcedUP"))
            await qbit.seed_ready(meta)
            for use_magnet in (False, True):
                async def receive():
                    output = root / f"cb-{kind}-{use_magnet}"
                    runner = download_magnet if use_magnet else download
                    source = Magnet.parse("magnet:?xt=urn:btih:" + meta.info_hash.hex()) if use_magnet else meta
                    metrics = await runner(source, [("127.0.0.1", qbit.peer_port)], output,
                                           use_trackers=False, use_dht=False, use_pex=pex, listen_host="127.0.0.1",
                                           timeout=5, piece_timeout=15)
                    verify(meta, output, expected)
                    return dict(metrics=metrics)
                await case(f"qbit-to-cb-{kind}-{'magnet' if use_magnet else 'torrent'}", receive)
            if meta.multi_file:
                async def resume():
                    output = root / "cb-resume"
                    reached = asyncio.Event()
                    task = None
                    def progress(done, total):
                        if done and not reached.is_set():
                            reached.set()
                            task.cancel()
                    source = Magnet.parse("magnet:?xt=urn:btih:" + meta.info_hash.hex())
                    task = asyncio.create_task(download_magnet(source, [("127.0.0.1", qbit.peer_port)], output,
                                                               progress=progress, concurrency=1, use_trackers=False,
                                                               use_dht=False, use_pex=pex, listen_host="127.0.0.1"))
                    try:
                        await task
                        raise AssertionError("download was not interrupted")
                    except asyncio.CancelledError:
                        if not reached.is_set():
                            raise
                    if output.exists() or not output.with_name(output.name + ".part").exists():
                        raise AssertionError("cancelled download did not preserve partial data")
                    metrics = await download_magnet(source, [("127.0.0.1", qbit.peer_port)], output,
                                                    resume=True, use_trackers=False, use_dht=False, use_pex=pex,
                                                    listen_host="127.0.0.1")
                    if metrics["resumed_bytes"] <= 0 or metrics["payload_received_bytes"] != meta.length - metrics["resumed_bytes"]:
                        raise AssertionError("resume did not rehash/reuse verified pieces")
                    verify(meta, output, expected)
                    return dict(metrics=metrics)
                await case("qbit-to-cb-directory-cancel-resume-magnet", resume)
            await qbit.remove(meta)

            for use_magnet in (False, True):
                async def send():
                    source = FileSource(meta, path)
                    metrics = Metrics()
                    metrics.start()
                    server = SeedServer(meta, source, metrics=metrics)
                    save_path = root / f"qbit-{kind}-{use_magnet}"
                    save_path.mkdir()
                    try:
                        port = await server.start()
                        magnet = f"magnet:?xt=urn:btih:{meta.info_hash.hex()}&x.pe=127.0.0.1:{port}" if use_magnet else None
                        await qbit.add(meta_path, meta, save_path, magnet=magnet)
                        row = await qbit.wait(meta, lambda row: row["progress"] == 1 and row["amount_left"] == 0,
                                              peer=("127.0.0.1", port))
                        verify(meta, save_path / meta.name, expected)
                        return dict(metrics=metrics.report(complete=True), remote_downloaded=row["downloaded"])
                    finally:
                        try:
                            await qbit.remove(meta)
                        finally:
                            try:
                                await server.close()
                            finally:
                                source.close()
                await case(f"cb-to-qbit-{kind}-{'magnet' if use_magnet else 'torrent'}", send)
        report["complete"] = len(report["cases"]) == 9 and all(c["complete"] for c in report["cases"])
    except Exception as error:
        report["setup_error"] = str(error)
    finally:
        await qbit.close()
        log_path = root / "qbit-process.log"
        if not report["complete"] and log_path.exists():
            report["process_log_tail"] = log_tail(log_path, 4096)
        qbit_log = root / "profile" / "qBittorrent" / "data" / "logs" / "qbittorrent.log"
        if not report["complete"] and qbit_log.exists():
            report["qbit_log_tail"] = log_tail(qbit_log, 8192)
    report["successes"] = sum(c["complete"] for c in report["cases"])
    report["failures"] = sum(not c["complete"] for c in report["cases"])
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--report", required=True, type=Path)
    parser.add_argument("--enable-pex", action="store_true", help="enable PEX among the local test peers")
    args = parser.parse_args()
    if args.report.exists():
        parser.error("report already exists; use a new name")
    runtime = Path(".interop-runtime").resolve()
    runtime.mkdir(exist_ok=True)
    temp = tempfile.TemporaryDirectory(prefix="qbit-", dir=runtime)
    target = Path(temp.name).resolve()
    if not target.is_relative_to(runtime):
        raise ValueError("test directory escaped runtime root")
    try:
        report = asyncio.run(run(args.binary, target, pex=args.enable_pex))
        args.report.parent.mkdir(parents=True, exist_ok=True)
        with args.report.open("x", encoding="utf-8") as stream:
            json.dump(report, stream, indent=2)
            stream.write("\n")
        print(json.dumps({k: report.get(k) for k in ("qbittorrent", "complete", "successes", "failures", "setup_error")}))
        return 0 if report["complete"] else 1
    finally:
        if target.is_relative_to(runtime):
            temp.cleanup()


if __name__ == "__main__":
    raise SystemExit(main())
