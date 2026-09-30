"""v1 magnet parsing, bounded metadata discovery, and the normal download handoff."""
import asyncio
import base64
import math
import os
import struct
from dataclasses import dataclass, replace
from hashlib import sha1
from time import perf_counter
from urllib.parse import parse_qsl, urlsplit

from .bencode import encode
from .client import DownloadError, download
from .dht import DEFAULT_BOOTSTRAP, DhtNode
from .extensions import (BLOCK, MAX_EXTENDED, METADATA_ID, RESERVED, extended,
                         handshake, metadata_message, negotiation)
from .metainfo import Torrent
from .metadata_cache import verified_metadata
from .metrics import Metrics
from .tracker import announce
from .wire import PROTOCOL, Peer


def endpoint(value):
    try:
        host, port = value.rsplit(":", 1)
        host = host.removeprefix("[").removesuffix("]")
        port = int(port)
        if not 1 <= len(host) <= 253 or not 1 <= port <= 65535 or any(c.isspace() for c in host):
            raise ValueError
        return host, port
    except (ValueError, TypeError):
        raise ValueError("magnet peer must be HOST:PORT or [IPv6]:PORT") from None


@dataclass(frozen=True)
class Magnet:
    uri: str
    info_hash: bytes
    name: str
    trackers: tuple[str, ...] = ()
    peers: tuple[tuple[str, int], ...] = ()
    # Pending metadata has no trusted layout. These are display placeholders.
    length: int = 0
    files: tuple = ()
    multi_file: bool = False

    @classmethod
    def parse(cls, uri):
        if not isinstance(uri, str) or len(uri) > 16384 or len(uri.encode("utf-8")) > 16384:
            raise ValueError("magnet URI exceeds 16 KiB")
        parsed = urlsplit(uri)
        if parsed.scheme.lower() != "magnet" or parsed.netloc or parsed.path or parsed.fragment:
            raise ValueError("invalid magnet URI")
        pairs = parse_qsl(parsed.query, max_num_fields=128, errors="strict")
        hashes, trackers, peers, name = set(), [], [], None
        for key, value in pairs:
            if key == "xt" and value.lower().startswith("urn:btih:"):
                raw = value[9:]
                try:
                    if len(raw) == 40:
                        digest = bytes.fromhex(raw)
                    elif len(raw) == 32:
                        digest = base64.b32decode(raw.upper())
                    else:
                        digest = b""
                except ValueError:
                    raise ValueError("invalid v1 magnet info hash") from None
                if len(digest) != 20:
                    raise ValueError("invalid v1 magnet info hash")
                hashes.add(digest)
            elif key == "dn" and name is None:
                name = "".join(c for c in value[:256] if c.isprintable())
            elif key == "tr":
                url = urlsplit(value)
                if (url.scheme not in ("http", "https", "udp") or not url.hostname
                        or url.username or url.fragment or len(value) > 2048
                        or any(ord(c) < 32 for c in value)):
                    raise ValueError("invalid magnet tracker URL")
                trackers.append(value)
            elif key == "x.pe":
                peers.append(endpoint(value))
        if len(hashes) != 1:
            raise ValueError("magnet requires one unambiguous v1 (btih) info hash")
        if len(trackers) > 8 or len(peers) > 50:
            raise ValueError("magnet allows at most 8 trackers and 50 explicit peers")
        digest = hashes.pop()
        return cls(uri, digest, name or digest.hex(), tuple(dict.fromkeys(trackers)),
                   tuple(dict.fromkeys(peers)))


def load_source(source):
    from pathlib import Path
    if isinstance(source, str) and source.lower().startswith("magnet:"):
        return Magnet.parse(source)
    return Torrent.load(Path(source))


async def fetch_metadata(magnet, address, metrics, *, timeout=10.0):
    """Assemble from one peer; trust nothing until the complete SHA-1 matches."""
    async with asyncio.timeout(timeout):
        reader, writer = await asyncio.open_connection(*address, limit=MAX_EXTENDED)
        metrics.connections += 1
        peer = Peer(reader, writer, magnet, metrics, timeout)
        try:
            peer_id = b"-CB0002-" + os.urandom(12)
            await peer.send(PROTOCOL + RESERVED + magnet.info_hash + peer_id)
            reply = await peer.read(68)
            if (reply[:20] != PROTOCOL or reply[28:48] != magnet.info_hash
                    or reply[48:] == peer_id or not reply[25] & 0x10):
                raise ValueError("peer does not support metadata for this magnet")
            await peer.send(handshake())
            remote_id, size, result = None, None, None
            pending, received, cursor = set(), set(), 0
            # Bounds chatter as well as bytes. The total deadline also covers trickles.
            for _ in range(4096):
                if remote_id and size:
                    count = (size + BLOCK - 1) // BLOCK
                    while cursor < count and len(pending) < 4:
                        await peer.send(extended(remote_id, encode({b"msg_type": 0, b"piece": cursor})))
                        pending.add(cursor)
                        cursor += 1
                length = struct.unpack("!I", await peer.read(4))[0]
                # A bitfield can precede metadata; enough room for the maximum v1 layout.
                if length > 128 * 1024:
                    raise ValueError("oversized metadata peer frame")
                if not length:
                    continue
                kind = (await peer.read(1))[0]
                if kind == 20 and length > MAX_EXTENDED + 1:
                    raise ValueError("oversized extended message")
                payload = await peer.read(length - 1)
                if kind != 20:
                    if ((kind in (0, 1, 2, 3) and not payload)
                            or (kind == 4 and len(payload) == 4) or kind == 5):
                        continue
                    raise ValueError("unexpected message during metadata exchange")
                if not payload:
                    raise ValueError("empty extended message")
                identifier, body = payload[0], payload[1:]
                if identifier == 0:
                    new_id, new_size = negotiation(body)
                    if new_id == 0:
                        raise ValueError("peer disabled metadata exchange")
                    if new_id is not None:
                        remote_id = new_id
                    if new_size is not None:
                        if size is not None and size != new_size:
                            raise ValueError("peer changed metadata size")
                        size = new_size
                        if result is None:
                            result = bytearray(size)
                    continue
                if identifier != METADATA_ID:
                    continue
                fields, block = metadata_message(body)
                kind = fields[b"msg_type"]
                if kind not in (0, 1, 2):
                    continue
                piece = fields[b"piece"]
                if kind == 0:
                    if remote_id:
                        await peer.send(extended(remote_id, encode({b"msg_type": 2, b"piece": piece})))
                    continue  # Never serve unverified metadata.
                if piece not in pending:
                    raise ValueError("unsolicited or duplicate metadata piece")
                if kind == 2:
                    raise ValueError("peer rejected metadata request")
                if fields.get(b"total_size") != size or len(block) != min(BLOCK, size - piece * BLOCK):
                    raise ValueError("incorrect metadata block size")
                result[piece * BLOCK:piece * BLOCK + len(block)] = block
                pending.remove(piece)
                received.add(piece)
                if len(received) == (size + BLOCK - 1) // BLOCK:
                    raw = bytes(result)
                    if sha1(raw).digest() != magnet.info_hash:
                        metrics.hash_failures += 1
                        raise ValueError("metadata failed info-hash verification")
                    torrent = verified_metadata(raw, magnet.info_hash)
                    if torrent.private:
                        raise ValueError("private magnets are unsupported; use the original .torrent file")
                    return replace(torrent, trackers=magnet.trackers)
            raise ValueError("metadata message budget exceeded")
        finally:
            await peer.close()


async def resolve(magnet, peers=(), *, timeout=60.0, peer_timeout=10.0,
                  use_trackers=True, use_dht=True, dht_bootstrap=None,
                  concurrency=3, listen_host="0.0.0.0", metadata_cache=None):
    if not all(math.isfinite(t) and t > 0 for t in (timeout, peer_timeout)):
        raise ValueError("metadata timeouts must be positive and finite")
    if len(peers) > 200:
        raise ValueError("too many explicit peers")
    if type(concurrency) is not int or not 1 <= concurrency <= 3:
        raise ValueError("metadata concurrency must be 1..3")
    metrics = Metrics()
    metrics.start()
    cache_errors = []

    async def cache_call(method, value):
        # Drain the worker on cancellation: no cache mutation continues after
        # this coroutine has returned and the GUI closes its private loop.
        task = asyncio.create_task(asyncio.to_thread(method, value))
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            while not task.done():
                try:
                    await asyncio.shield(task)
                except asyncio.CancelledError:
                    continue  # Repeated UI cancel actions must not detach it.
                except Exception:
                    break
            if not task.cancelled():
                task.exception()  # Retrieve any worker error; cancellation wins.
            raise
        except (OSError, ValueError) as error:
            cache_errors.append((str(error) or type(error).__name__)[:256])
            return None

    if metadata_cache is not None:
        cached = await cache_call(metadata_cache.get, magnet.info_hash)
        if cached is not None:
            report = metrics.report(complete=not cached.private)
            report.update(cache_hit=True, cache_errors=cache_errors,
                          metadata_size=len(cached.info_bytes), errors=[])
            if cached.private:
                report["errors"] = ["private magnets are unsupported; use the original .torrent file"]
                raise DownloadError(report["errors"][0], dict(complete=False, completion_seconds=None,
                                    elapsed_seconds=report["elapsed_seconds"], metadata=report))
            addresses = tuple(dict.fromkeys(tuple(peers) + magnet.peers))[:200]
            return replace(cached, trackers=magnet.trackers), addresses, report
    queue, seen, errors, tasks = asyncio.Queue(maxsize=200), {}, [], []
    loop = asyncio.get_running_loop()
    found = loop.create_future()
    node, listener = None, None
    announced = set()
    peer_id = b"-CB0002-" + os.urandom(12)
    port = 0

    def add(addresses):
        for address in addresses:
            if len(seen) >= 200:
                break
            if address not in seen:
                seen[address] = None
                queue.put_nowait(address)

    def error_note(error):
        if len(errors) < 20:
            errors.append(str(error) or type(error).__name__)

    async def worker():
        while True:
            address = await queue.get()
            try:
                torrent = await fetch_metadata(magnet, address, metrics, timeout=peer_timeout)
                if not found.done():
                    found.set_result(torrent)
            except (OSError, ValueError, asyncio.TimeoutError, asyncio.IncompleteReadError) as error:
                metrics.peer_failures += 1
                error_note(error)
            finally:
                queue.task_done()

    async def tracker(url, event="started"):
        metrics.tracker_requests += 1
        try:
            if event != "stopped":
                announced.add(url)
            response = await announce(url, magnet.info_hash, peer_id, port=port, left=1,
                                      event=event, timeout=min(peer_timeout, 1.0) if event == "stopped" else peer_timeout)
            metrics.tracker_response_bytes += response.response_bytes
            if event != "stopped":
                add(response.peers)
        except (OSError, ValueError, asyncio.TimeoutError) as error:
            metrics.tracker_failures += 1
            error_note(error)

    async def dht():
        try:
            await node.start()
            await node.discover(magnet.info_hash, timeout=timeout, on_peers=add)
        except (OSError, ValueError, asyncio.TimeoutError) as error:
            error_note(error)

    torrent = None
    try:
        async with asyncio.timeout(timeout):
            add(tuple(peers) + magnet.peers)
            tasks.extend(asyncio.create_task(worker()) for _ in range(concurrency))
            discovery = []
            if use_trackers and magnet.trackers:
                # A real, short-lived port for tracker lookup; no unverified data served.
                listener = await asyncio.start_server(lambda r, w: w.close(), listen_host, 0)
                port = listener.sockets[0].getsockname()[1]
                discovery.extend(asyncio.create_task(tracker(url)) for url in magnet.trackers)
                tasks.extend(discovery)
            if use_dht:
                node = DhtNode(bootstrap=DEFAULT_BOOTSTRAP if dht_bootstrap is None else dht_bootstrap,
                               metrics=metrics, bind_host=listen_host)
                task = asyncio.create_task(dht())
                discovery.append(task)
                tasks.append(task)

            async def exhausted():
                await asyncio.gather(*discovery)
                await queue.join()

            exhaustion = asyncio.create_task(exhausted())
            tasks.append(exhaustion)
            await asyncio.wait((found, exhaustion), return_when=asyncio.FIRST_COMPLETED)
            if found.done():
                torrent = found.result()
            else:
                exhaustion.result()
    except asyncio.TimeoutError:
        error_note("metadata resolution deadline expired")
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if node is not None:
            await node.close()
        if listener is not None:
            listener.close()
            await listener.wait_closed()
        if announced:
            await asyncio.gather(*(tracker(url, "stopped") for url in announced))
        found.cancel()
    if torrent is not None and metadata_cache is not None:
        await cache_call(metadata_cache.put, torrent)
    report = metrics.report(complete=torrent is not None)
    report.update(cache_hit=False, cache_errors=cache_errors)
    report["errors"] = errors
    report["metadata_size"] = len(torrent.info_bytes) if torrent is not None else 0
    if torrent is None:
        raise DownloadError("could not resolve magnet metadata" + (": " + errors[-1] if errors else "; no peers found"),
                            dict(complete=False, completion_seconds=None,
                                 elapsed_seconds=report["elapsed_seconds"], metadata=report))
    return torrent, tuple(seen), report


async def download_magnet(magnet, peers, output, *, metadata_timeout=60.0,
                          metadata_cache=None, **options):
    started = perf_counter()
    torrent, addresses, metadata = await resolve(
        magnet, peers, timeout=metadata_timeout, peer_timeout=min(options.get("timeout", 15.0), 10.0),
        use_trackers=options.get("use_trackers", True), use_dht=options.get("use_dht", False),
        dht_bootstrap=options.get("dht_bootstrap"),
        concurrency=min(3, options.get("max_connections", 16)),
        listen_host=options.get("listen_host", "0.0.0.0"), metadata_cache=metadata_cache)
    on_metadata = options.pop("on_metadata", None)
    if on_metadata:
        on_metadata(torrent)
    observe = options.get("observe")
    if observe:
        def with_metadata(snapshot):
            observe(replace(snapshot, elapsed_seconds=snapshot.elapsed_seconds + metadata["elapsed_seconds"]))
        options["observe"] = with_metadata

    def combined(report):
        report["metadata"] = metadata
        report["payload_completion_seconds"] = report["completion_seconds"]
        report["elapsed_seconds"] = perf_counter() - started
        report["completion_seconds"] = report["elapsed_seconds"] if report["complete"] else None
        return report

    try:
        return combined(await download(torrent, addresses, output, **options))
    except DownloadError as error:
        combined(error.report)
        raise
