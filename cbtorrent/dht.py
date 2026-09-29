"""Bounded, session-scoped IPv4 BEP 5 discovery and announcement.

Compact contact helpers build on the earlier pair/dht-bep5 prototype. Only
matched replies enter the routing table; referrals stay in a lookup shortlist.
No public network is used unless a caller explicitly starts discovery.
"""
import asyncio
import hmac
import ipaddress
import math
import os
import socket
from dataclasses import dataclass
from time import monotonic

from .bencode import decode, encode
from .metrics import Metrics

K, ALPHA = 8, 3
MAX_PACKET = 1200
MAX_CANDIDATES, MAX_QUERIES, MAX_PEERS = 64, 32, 200
MAX_HASHES, PEERS_PER_HASH = 128, 50
NODE_TTL, PEER_TTL, TOKEN_PERIOD = 900, 1800, 300
DEFAULT_BOOTSTRAP = (("dht.transmissionbt.com", 6881), ("router.utorrent.com", 6881))


def identity(value):
    if not isinstance(value, bytes) or len(value) != 20:
        raise ValueError("DHT identity must be 20 bytes")
    return value


def endpoint(host, port):
    address = ipaddress.IPv4Address(host)
    if (address.is_unspecified or address.is_multicast or int(address) == 0xffffffff
            or type(port) is not int or not 1 <= port <= 65535):
        raise ValueError("invalid IPv4 DHT endpoint")
    return str(address), port


def xor_distance(a, b):
    return int.from_bytes(identity(a), "big") ^ int.from_bytes(identity(b), "big")


def compact_peer(address):
    host, port = endpoint(*address)
    return socket.inet_aton(host) + port.to_bytes(2, "big")


def parse_peer(raw):
    if not isinstance(raw, bytes) or len(raw) != 6:
        raise ValueError("invalid compact IPv4 peer")
    return endpoint(socket.inet_ntoa(raw[:4]), int.from_bytes(raw[4:], "big"))


def compact_nodes(raw):
    if not isinstance(raw, bytes) or len(raw) % 26 or len(raw) > MAX_PACKET:
        raise ValueError("invalid compact node list")
    nodes = []
    for offset in range(0, len(raw), 26):
        try:
            host, port = parse_peer(raw[offset + 20:offset + 26])
            nodes.append((raw[offset:offset + 20], host, port))
        except ValueError:
            continue
    return nodes


@dataclass(frozen=True)
class Contact:
    node_id: bytes
    address: tuple[str, int]
    seen: float


class RoutingTable:
    """BEP 5 buckets: split only the bucket containing our own ID.

    Admit confirmed responses, retain good contacts, expire stale contacts.
    At most 32 buckets / 256 contacts; there is no unbounded replacement cache.
    """
    def __init__(self, node_id, clock=monotonic):
        self.node_id = identity(node_id)
        self.clock = clock
        self.buckets = [(0, 1 << 160, {})]

    def closest(self, target, limit=K):
        identity(target)
        now = self.clock()
        contacts = []
        for _, _, bucket in self.buckets:
            for key, contact in list(bucket.items()):
                if now - contact.seen >= NODE_TTL:
                    del bucket[key]
            contacts.extend(bucket.values())
        return sorted(contacts, key=lambda n: xor_distance(n.node_id, target))[:limit]

    def add(self, node_id, address):
        identity(node_id)
        address = endpoint(*address)
        if node_id == self.node_id:
            return
        self.closest(self.node_id)  # expire stale entries before considering splits
        number = int.from_bytes(node_id, "big")
        local = int.from_bytes(self.node_id, "big")
        for _, _, bucket in self.buckets:
            # One address cannot consume all buckets with rotating identities.
            if any(c.address == address and c.node_id != node_id for c in bucket.values()):
                return
        while True:
            index = next(i for i, (lo, hi, _) in enumerate(self.buckets) if lo <= number < hi)
            lo, hi, bucket = self.buckets[index]
            if node_id in bucket and bucket[node_id].address != address:
                return
            if node_id in bucket or len(bucket) < K:
                bucket[node_id] = Contact(node_id, address, self.clock())
                return
            if not lo <= local < hi or len(self.buckets) >= 32:
                return
            mid = (lo + hi) // 2
            left = {key: c for key, c in bucket.items() if int.from_bytes(key, "big") < mid}
            right = {key: c for key, c in bucket.items() if key not in left}
            self.buckets[index:index + 1] = [(lo, mid, left), (mid, hi, right)]


class _Protocol(asyncio.DatagramProtocol):
    def __init__(self, node):
        self.node = node

    def datagram_received(self, data, addr):
        self.node.receive(data, addr)

    def error_received(self, exc):
        # UDP errors cannot reliably be attributed to one query. Its deadline
        # settles it without failing unrelated peers sharing the socket.
        pass

    def connection_lost(self, exc):
        self.node._lost.set()


class DhtNode:
    def __init__(self, *, node_id=None, bootstrap=DEFAULT_BOOTSTRAP,
                 bind_host="0.0.0.0", bind_port=0, query_timeout=2.0,
                 max_inflight=8, metrics=None, clock=monotonic):
        self.node_id = identity(os.urandom(20) if node_id is None else node_id)
        if not math.isfinite(query_timeout) or query_timeout <= 0:
            raise ValueError("DHT query timeout must be positive and finite")
        if type(max_inflight) is not int or not 1 <= max_inflight <= 32:
            raise ValueError("DHT inflight limit must be 1..32")
        self.bootstrap_hosts = tuple(bootstrap)
        if len(self.bootstrap_hosts) > 8:
            raise ValueError("at most 8 DHT bootstrap endpoints")
        for host, port in self.bootstrap_hosts:
            if not isinstance(host, str) or not host or len(host) > 253 or type(port) is not int or not 1 <= port <= 65535:
                raise ValueError("invalid DHT bootstrap endpoint")
        if type(bind_port) is not int or not 0 <= bind_port <= 65535:
            raise ValueError("invalid DHT bind port")
        self.bind_host, self.bind_port = bind_host, bind_port
        self.query_timeout, self.clock = query_timeout, clock
        self.metrics = metrics if metrics is not None else Metrics()
        self.table = RoutingTable(self.node_id, clock)
        self._slots = asyncio.Semaphore(max_inflight)
        self._lookup_lock = asyncio.Lock()
        self._pending = {}
        self._peers = {}
        self._secret = os.urandom(32)
        self._transport = None
        self._closed = False
        self._lost = asyncio.Event()
        self._credit, self._credit_time = 100.0, clock()

    @property
    def port(self):
        return self._transport.get_extra_info("sockname")[1] if self._transport else 0

    async def start(self):
        if self._closed:
            raise OSError("DHT node is closed")
        if self._transport is None:
            async with asyncio.timeout(self.query_timeout):
                self._transport, _ = await asyncio.get_running_loop().create_datagram_endpoint(
                    lambda: _Protocol(self), local_addr=(self.bind_host, self.bind_port),
                    family=socket.AF_INET)
        return self.port

    async def close(self):
        self._closed = True
        for _, future in tuple(self._pending.values()):
            if not future.done():
                future.set_exception(OSError("DHT node closed"))
        if self._transport is not None:
            self._transport.close()
            self._transport = None
            await self._lost.wait()

    def _send(self, packet, address):
        if self._closed or self._transport is None:
            raise OSError("DHT node is not running")
        if len(packet) > MAX_PACKET:
            raise ValueError("DHT packet exceeds 1200 bytes")
        self._transport.sendto(packet, address)
        self.metrics.dht_sent_bytes += len(packet)

    async def query(self, address, method, args):
        address = endpoint(*address)  # resolve names separately, before matching replies
        try:
            async with asyncio.timeout(self.query_timeout):
                async with self._slots:
                    tid = os.urandom(4)
                    while tid in self._pending:
                        tid = os.urandom(4)
                    future = asyncio.get_running_loop().create_future()
                    self._pending[tid] = (address, future)
                    try:
                        self._send(encode({b"t": tid, b"y": b"q", b"q": method,
                                           b"a": {**args, b"id": self.node_id}}), address)
                        self.metrics.dht_requests += 1
                        result = await future
                        self.table.add(result[b"id"], address)
                        return result
                    finally:
                        self._pending.pop(tid, None)
                        if not future.done():
                            future.cancel()
        except (OSError, ValueError, TimeoutError):
            self.metrics.dht_failures += 1
            raise

    def _token(self, host, info_hash, epoch):
        return hmac.digest(self._secret, socket.inet_aton(host) + info_hash
                           + str(epoch).encode("ascii"), "sha256")[:16]

    def _prune_peers(self):
        now = self.clock()
        for info_hash, peers in list(self._peers.items()):
            for address, expires in list(peers.items()):
                if expires <= now:
                    del peers[address]
            if not peers:
                del self._peers[info_hash]

    def _answer(self, method, args, address):
        identity(args.get(b"id"))
        result = {b"id": self.node_id}
        if method == b"ping":
            return result
        if method not in (b"find_node", b"get_peers", b"announce_peer"):
            raise LookupError("unknown DHT method")
        target = identity(args.get(b"target" if method == b"find_node" else b"info_hash"))
        self._prune_peers()
        epoch = int(self.clock() // TOKEN_PERIOD)
        if method == b"announce_peer":
            token = args.get(b"token")
            if not isinstance(token, bytes) or not any(
                    hmac.compare_digest(token, self._token(address[0], target, e))
                    for e in (epoch, epoch - 1)):
                raise ValueError("invalid announce token")
            implied = args.get(b"implied_port", 0)
            if type(implied) is not int:
                raise ValueError("invalid implied_port")
            peer = endpoint(address[0], address[1] if implied else args.get(b"port"))
            if target not in self._peers and len(self._peers) >= MAX_HASHES:
                raise ValueError("peer store full")
            peers = self._peers.setdefault(target, {})
            if peer not in peers and len(peers) >= PEERS_PER_HASH:
                raise ValueError("peer store full")
            peers[peer] = self.clock() + PEER_TTL
            return result
        if method == b"get_peers":
            result[b"token"] = self._token(address[0], target, epoch)
            if self._peers.get(target):
                result[b"values"] = [compact_peer(p) for p in self._peers[target]]
                return result
        result[b"nodes"] = b"".join(c.node_id + compact_peer(c.address)
                                    for c in self.table.closest(target))
        return result

    def receive(self, data, address):
        if self._closed:
            return
        self.metrics.dht_received_bytes += len(data)
        if len(data) > MAX_PACKET:
            return
        try:
            address = endpoint(*address)
            msg = decode(data, max_size=MAX_PACKET)
            if not isinstance(msg, dict):
                return
            tid, kind = msg.get(b"t"), msg.get(b"y")
            if not isinstance(tid, bytes) or not 1 <= len(tid) <= 16:
                return
            if kind in (b"r", b"e"):
                pending = self._pending.get(tid)
                if pending is None or pending[0] != address or pending[1].done():
                    return
                if kind == b"e":
                    error = msg.get(b"e")
                    if isinstance(error, list) and len(error) == 2 and type(error[0]) is int and isinstance(error[1], bytes):
                        pending[1].set_exception(ValueError(f"DHT error {error[0]}"))
                else:
                    result = msg.get(b"r")
                    if isinstance(result, dict):
                        identity(result.get(b"id"))
                        pending[1].set_result(result)
                return
            if kind != b"q" or not isinstance(msg.get(b"a"), dict):
                return
            now = self.clock()
            self._credit = min(100.0, self._credit + max(0, now - self._credit_time) * 50)
            self._credit_time = now
            if self._credit < 1:
                return
            self._credit -= 1
            try:
                result = self._answer(msg.get(b"q"), msg[b"a"], address)
                reply = {b"t": tid, b"y": b"r", b"r": result}
            except LookupError:
                reply = {b"t": tid, b"y": b"e", b"e": [204, b"Unknown method"]}
            except (ValueError, TypeError):
                reply = {b"t": tid, b"y": b"e", b"e": [203, b"Invalid arguments"]}
            self._send(encode(reply), address)
        except (ValueError, TypeError, OSError):
            return

    async def _resolve(self):
        seeds = []
        for host, port in self.bootstrap_hosts:
            try:
                async with asyncio.timeout(self.query_timeout):
                    infos = await asyncio.get_running_loop().getaddrinfo(
                        host, port, family=socket.AF_INET, type=socket.SOCK_DGRAM)
                for info in infos[:4]:
                    address = endpoint(*info[4][:2])
                    if address not in seeds:
                        seeds.append(address)
            except (OSError, ValueError, TimeoutError):
                self.metrics.dht_failures += 1
        return seeds

    async def discover(self, info_hash, *, port=None, timeout=15.0, on_peers=None):
        """Bounded XOR lookup; publish peers as replies arrive, then announce.

        Tokens belong to this info-hash lookup only. Deadline expiry returns
        partial results; cancellation propagates and drains all child queries.
        """
        identity(info_hash)
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("DHT lookup timeout must be positive and finite")
        if port is not None and (type(port) is not int or not 1 <= port <= 65535):
            raise ValueError("invalid DHT announce port")
        peers, tokens, queried = {}, {}, set()
        candidates = {c.address: c.node_id for c in self.table.closest(info_hash, MAX_CANDIDATES)}

        async def probe(address):
            try:
                result = await self.query(address, b"get_peers", {b"info_hash": info_hash})
                candidates[address] = result[b"id"]
                token = result.get(b"token")
                if isinstance(token, bytes) and 0 < len(token) <= 64:
                    tokens[address] = (result[b"id"], token)
                values = result.get(b"values", [])
                new = []
                if isinstance(values, list):
                    for value in values[:MAX_PEERS]:
                        try:
                            peer = parse_peer(value)
                            if peer not in peers and len(peers) < MAX_PEERS:
                                peers[peer] = None
                                new.append(peer)
                        except ValueError:
                            continue
                if new:
                    self.metrics.dht_peers += len(new)
                    if on_peers is not None:
                        on_peers(tuple(new))
                for node_id, host, node_port in compact_nodes(result.get(b"nodes", b"")):
                    referral = (host, node_port)
                    if node_id != self.node_id and referral not in candidates and len(candidates) < MAX_CANDIDATES:
                        candidates[referral] = node_id
            except (OSError, ValueError, TimeoutError):
                return

        try:
            async with asyncio.timeout(timeout):
                async with self._lookup_lock:
                    if not candidates:
                        for address in await self._resolve():
                            candidates.setdefault(address, None)
                    while len(queried) < MAX_QUERIES:
                        available = [a for a in candidates if a not in queried]
                        available.sort(key=lambda a: xor_distance(candidates[a], info_hash)
                                       if candidates[a] is not None else -1)
                        batch = available[:min(ALPHA, MAX_QUERIES - len(queried))]
                        if not batch:
                            break
                        queried.update(batch)
                        # TaskGroup drains siblings on cancellation or callback failure.
                        async with asyncio.TaskGroup() as group:
                            for address in batch:
                                group.create_task(probe(address))
                    if port is not None:
                        targets = sorted(tokens, key=lambda a: xor_distance(tokens[a][0], info_hash))[:K]
                        async def announce(address):
                            try:
                                await self.query(address, b"announce_peer", {
                                    b"info_hash": info_hash, b"port": port,
                                    b"implied_port": 0, b"token": tokens[address][1]})
                            except (OSError, ValueError, TimeoutError):
                                pass
                        async with asyncio.TaskGroup() as group:
                            for address in targets:
                                group.create_task(announce(address))
        except TimeoutError:
            self.metrics.dht_failures += 1
        return tuple(peers)


class DhtDiscovery:
    """Own the node, periodic refresh, first-lookup signal, and cleanup."""
    def __init__(self, torrent, port, metrics, *, bootstrap=None, timeout=15.0,
                 on_peers=None, bind_host="0.0.0.0"):
        self.node = DhtNode(bootstrap=(torrent.nodes or DEFAULT_BOOTSTRAP) if bootstrap is None else bootstrap,
                            bind_host=bind_host, query_timeout=min(2.0, timeout), metrics=metrics)
        self.torrent, self.port, self.timeout = torrent, port, timeout
        self.on_peers = on_peers
        self.first_done = asyncio.Event()
        self.changed = asyncio.Event()
        self.errors = []
        self.task = None

    def start(self):
        self.task = asyncio.create_task(self._run())

    async def _run(self):
        def found(peers):
            if self.on_peers is not None:
                self.on_peers(peers)
            self.changed.set()
        try:
            await self.node.start()
            while True:
                await self.node.discover(self.torrent.info_hash, port=self.port,
                                         timeout=self.timeout, on_peers=found)
                self.first_done.set()
                self.changed.set()
                await asyncio.sleep(300)
        except (OSError, ValueError) as error:
            self.node.metrics.dht_failures += 1
            self.errors.append(str(error))
        finally:
            self.first_done.set()
            self.changed.set()
            await self.node.close()

    async def close(self):
        if self.task is not None:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
