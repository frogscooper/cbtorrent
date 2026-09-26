"""Bounded HTTP(S) and IPv4 UDP tracker discovery (BEP 3/23/15)."""
import asyncio
import ipaddress
import math
import os
import socket
import struct
from dataclasses import dataclass
from urllib.parse import urlencode, urlsplit, urljoin, quote
import ssl

from .bencode import decode

MAX_RESPONSE = 1024 * 1024


@dataclass(frozen=True)
class Announce:
    peers: tuple[tuple[str, int], ...]
    interval: int
    response_bytes: int


def compact_peers(raw, *, ipv6=False):
    width = 18 if ipv6 else 6
    if not isinstance(raw, bytes) or len(raw) % width:
        raise ValueError("invalid compact peer list")
    peers = []
    for offset in range(0, len(raw), width):
        host = str(ipaddress.ip_address(raw[offset:offset + width - 2]))
        port = int.from_bytes(raw[offset + width - 2:offset + width], "big")
        if port:
            peers.append((host, port))
    return peers


def parse_response(raw):
    response = decode(raw, max_size=MAX_RESPONSE)
    if not isinstance(response, dict):
        raise ValueError("tracker response must be a dictionary")
    if b"failure reason" in response:
        raise ValueError(f"tracker rejected announce: {response[b'failure reason']!r}")
    interval = response.get(b"interval")
    if type(interval) is not int or interval <= 0:
        raise ValueError("invalid tracker interval")
    value = response.get(b"peers", b"")
    if isinstance(value, bytes):
        peers = compact_peers(value)
    elif isinstance(value, list):
        peers = []
        for item in value:
            if not isinstance(item, dict) or not isinstance(item.get(b"ip"), bytes):
                raise ValueError("invalid dictionary peer")
            port = item.get(b"port")
            if type(port) is not int or not 1 <= port <= 65535:
                raise ValueError("invalid peer port")
            host = item[b"ip"].decode("ascii")
            if not host or len(host) > 253:
                raise ValueError("invalid peer host")
            peers.append((host, port))
    else:
        raise ValueError("invalid tracker peers")
    peers.extend(compact_peers(response.get(b"peers6", b""), ipv6=True))
    return Announce(tuple(dict.fromkeys(peers))[:200], max(1, interval), len(raw))


async def _http(url, info_hash, peer_id, port, downloaded, uploaded, left, event, timeout):
    params = dict(info_hash=info_hash, peer_id=peer_id, port=port, uploaded=uploaded,
                  downloaded=downloaded, left=left, compact=1, numwant=80)
    if event:
        params["event"] = event
    parsed = urlsplit(url)
    target = url + ("&" if parsed.query else "?") + urlencode(params, quote_via=quote)
    async with asyncio.timeout(timeout):
        for redirect in range(4):
            parsed = urlsplit(target)
            if (parsed.scheme not in ("http", "https") or not parsed.hostname
                    or parsed.fragment or parsed.username or any(ord(c) < 32 for c in target)):
                raise ValueError("invalid HTTP tracker URL")
            secure = parsed.scheme == "https"
            reader, writer = await asyncio.open_connection(
                parsed.hostname, parsed.port or (443 if secure else 80),
                ssl=ssl.create_default_context() if secure else None, limit=65536)
            try:
                path = (parsed.path or "/") + ("?" + parsed.query if parsed.query else "")
                request = (f"GET {path} HTTP/1.1\r\nHost: {parsed.netloc}\r\n"
                           "User-Agent: cbtorrent/0.2\r\nAccept-Encoding: identity\r\n"
                           "Connection: close\r\n\r\n")
                writer.write(request.encode("ascii"))
                await writer.drain()
                try:
                    raw_headers = await reader.readuntil(b"\r\n\r\n")
                except (asyncio.LimitOverrunError, asyncio.IncompleteReadError) as error:
                    raise ValueError("invalid or oversized HTTP headers") from error
                lines = raw_headers[:-4].split(b"\r\n")
                status = lines[0].split()
                if len(status) < 2 or not status[0].startswith(b"HTTP/") or not status[1].isdigit():
                    raise ValueError("invalid tracker HTTP status")
                status = int(status[1])
                headers = {}
                for line in lines[1:]:
                    key, separator, value = line.partition(b":")
                    if not separator:
                        raise ValueError("malformed tracker HTTP header")
                    key = key.lower()
                    if key in headers and key in (b"content-length", b"transfer-encoding"):
                        raise ValueError("ambiguous tracker framing")
                    headers[key] = value.strip()
                if status in (301, 302, 303, 307, 308):
                    if redirect == 3 or b"location" not in headers:
                        raise ValueError("too many or invalid tracker redirects")
                    target = urljoin(target, headers[b"location"].decode("ascii"))
                    continue
                if status != 200:
                    raise ValueError(f"tracker HTTP status {status}")
                if headers.get(b"content-encoding", b"identity").lower() != b"identity":
                    raise ValueError("unsupported tracker content encoding")
                if b"transfer-encoding" in headers:
                    if headers[b"transfer-encoding"].lower() != b"chunked" or b"content-length" in headers:
                        raise ValueError("unsupported or ambiguous tracker transfer encoding")
                    body = bytearray()
                    while True:
                        line = await reader.readline()
                        try:
                            size = int(line.strip().split(b";", 1)[0], 16)
                        except ValueError as error:
                            raise ValueError("invalid HTTP chunk") from error
                        if size < 0 or size + len(body) > MAX_RESPONSE:
                            raise ValueError("tracker response exceeds size limit")
                        if size == 0:
                            break
                        body.extend(await reader.readexactly(size))
                        if await reader.readexactly(2) != b"\r\n":
                            raise ValueError("invalid HTTP chunk terminator")
                    raw = bytes(body)
                elif b"content-length" in headers:
                    size = int(headers[b"content-length"])
                    if not 0 <= size <= MAX_RESPONSE:
                        raise ValueError("tracker response exceeds size limit")
                    raw = await reader.readexactly(size)
                else:
                    body = bytearray()
                    while chunk := await reader.read(min(65536, MAX_RESPONSE + 1 - len(body))):
                        body.extend(chunk)
                        if len(body) > MAX_RESPONSE:
                            raise ValueError("tracker response exceeds size limit")
                    raw = bytes(body)
                return parse_response(raw)
            finally:
                writer.close()
                try:
                    await asyncio.wait_for(writer.wait_closed(), min(timeout, 1))
                except (OSError, asyncio.TimeoutError):
                    pass


async def _udp(url, info_hash, peer_id, port, downloaded, uploaded, left, event, timeout):
    parsed = urlsplit(url)
    if not parsed.hostname or not parsed.port:
        raise ValueError("UDP tracker requires host and port")
    loop = asyncio.get_running_loop()
    addresses = await loop.getaddrinfo(parsed.hostname, parsed.port, family=socket.AF_INET,
                                       type=socket.SOCK_DGRAM)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setblocking(False)
    received_bytes = 0
    try:
        await loop.sock_connect(sock, addresses[0][4])

        async def exchange(action, body):
            nonlocal received_bytes
            transaction = os.urandom(4)
            packet = body[:8] + struct.pack("!I", action) + transaction + body[8:]
            await loop.sock_sendall(sock, packet)
            async with asyncio.timeout(timeout):
                while True:
                    response = await loop.sock_recv(sock, 65536)
                    received_bytes += len(response)
                    if len(response) < 8 or response[4:8] != transaction:
                        continue
                    result_action = int.from_bytes(response[:4], "big")
                    if result_action == 3:
                        raise ValueError(f"UDP tracker error: {response[8:]!r}")
                    if result_action != action:
                        raise ValueError("unexpected UDP tracker action")
                    return response[8:]

        connection = await exchange(0, struct.pack("!Q", 0x41727101980))
        if len(connection) != 8:
            raise ValueError("invalid UDP connection response")
        event_id = {"": 0, "completed": 1, "started": 2, "stopped": 3}[event]
        body = connection + info_hash + peer_id + struct.pack("!QQQII", downloaded, left, uploaded, event_id, 0)
        body += os.urandom(4) + struct.pack("!iH", 80, port)
        response = await exchange(1, body)
        if len(response) < 12:
            raise ValueError("truncated UDP announce response")
        interval = int.from_bytes(response[:4], "big")
        if not interval:
            raise ValueError("invalid UDP tracker interval")
        return Announce(tuple(compact_peers(response[12:]))[:200], interval, received_bytes)
    finally:
        sock.close()


async def announce(url, info_hash, peer_id, *, port, downloaded=0, uploaded=0, left=0,
                   event="started", timeout=10.0):
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("tracker timeout must be positive and finite")
    if len(info_hash) != 20 or len(peer_id) != 20 or not 1 <= port <= 65535:
        raise ValueError("invalid announce identity or port")
    if event not in ("started", "completed", "stopped", ""):
        raise ValueError("invalid tracker event")
    if min(downloaded, uploaded, left) < 0:
        raise ValueError("negative tracker counters")
    args = (url, info_hash, peer_id, port, downloaded, uploaded, left, event, timeout)
    if urlsplit(url).scheme == "udp":
        async with asyncio.timeout(timeout * 2):
            return await _udp(*args)
    try:
        return await _http(*args)
    except asyncio.IncompleteReadError as error:
        raise ValueError("truncated tracker HTTP response") from error
