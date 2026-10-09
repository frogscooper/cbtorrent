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
from .diagnostics import describe_error
from .metrics import Metrics

K, ALPHA = 8, 3
# A query unanswered after SLOW_QUERY seconds stops holding one of the ALPHA
# lookup slots (its late reply still counts); at most MAX_IN_FLIGHT overlap.
SLOW_QUERY, MAX_IN_FLIGHT = 1.0, 8
# Sent packets stay under 1200 bytes. Replies from nodes holding many peers can
# be larger, so accept one unfragmented Ethernet-sized datagram (plus slack).
MAX_PACKET, MAX_DATAGRAM = 1200, 2048
MAX_CANDIDATES, MAX_QUERIES, MAX_PEERS = 64, 32, 200
MAX_HASHES, PEERS_PER_HASH = 128, 50
MAX_ERRORS, MAX_QUERY_ERRORS = 20, 8
NODE_TTL, PEER_TTL, TOKEN_PERIOD = 900, 1800, 300
REFRESH_SECONDS, RETRY_SECONDS = 300, 60
DEFAULT_BOOTSTRAP = (("dht.transmissionbt.com", 6881), ("router.bittorrent.com", 6881),
                     ("router.utorrent.com", 6881), ("dht.libtorrent.org", 25401))


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


def shared_prefix_bits(a, b):
    return 160 - xor_distance(a, b).bit_length()


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
        self.errors = []
        self._query_errors = 0

    def note(self, text, *, query=False):
        """Keep a bounded sample: a few query failures, then lookup summaries."""
        if query:
            if self._query_errors >= MAX_QUERY_ERRORS:
                return
            self._query_errors += 1
        if len(self.errors) < MAX_ERRORS:
            self.errors.append(text)

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
        except (OSError, ValueError, TimeoutError) as error:
            self.metrics.dht_failures += 1
            if not self._closed:
                reason = (f"no reply within {self.query_timeout:g}s" if isinstance(error, TimeoutError)
                          else describe_error(error))
                self.note(f"{method.decode('ascii', 'replace')} {address[0]}:{address[1]}: {reason}",
                          query=True)
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
        if len(data) > MAX_DATAGRAM:
            return
        try:
            address = endpoint(*address)
            # Some deployed clients emit unsorted keys; KRPC replies are never hashed.
            msg = decode(data, max_size=MAX_DATAGRAM, sorted_keys=False)
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
        async def resolve(host, port):
            try:
                async with asyncio.timeout(self.query_timeout):
                    infos = await asyncio.get_running_loop().getaddrinfo(
                        host, port, family=socket.AF_INET, type=socket.SOCK_DGRAM)
                return [endpoint(*info[4][:2]) for info in infos[:4]]
            except (OSError, ValueError, TimeoutError) as error:
                self.metrics.dht_failures += 1
                reason = (f"DNS timed out after {self.query_timeout:g}s"
                          if isinstance(error, TimeoutError) else describe_error(error))
                self.note(f"bootstrap {host}:{port}: {reason}")
                return []
        # One slow resolver must not delay the others; keep the configured order.
        results = await asyncio.gather(*(resolve(host, port) for host, port in self.bootstrap_hosts))
        return list(dict.fromkeys(address for addresses in results for address in addresses))

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
        peers, tokens, queried, in_flight, launched = {}, {}, set(), set(), {}
        seeds = set()
        loop = asyncio.get_running_loop()
        replies, closest = 0, None
        candidates = {c.address: c.node_id for c in self.table.closest(info_hash, MAX_CANDIDATES)}

        def distance(address):
            node_id = candidates[address]
            return -1 if node_id is None else xor_distance(node_id, info_hash)  # bootstrap first

        def admit(address, node_id):
            # A full shortlist trades its farthest unqueried entry for a closer
            # referral. Rejecting all late referrals would stall convergence a
            # few hops from the bootstrap nodes, before reaching peer holders.
            if node_id == self.node_id or address in candidates:
                return
            waiting = [a for a in candidates if a not in queried]
            if len(waiting) >= MAX_CANDIDATES:
                farthest = max(waiting, key=distance)
                if distance(farthest) <= xor_distance(node_id, info_hash):
                    return
                del candidates[farthest]
            candidates[address] = node_id

        async def probe(address):
            nonlocal replies, closest
            try:
                result = await self.query(address, b"get_peers", {b"info_hash": info_hash})
                candidates[address] = result[b"id"]
                replies += 1
                if closest is None or xor_distance(result[b"id"], info_hash) < xor_distance(closest, info_hash):
                    closest = result[b"id"]
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
                referrals = compact_nodes(result.get(b"nodes", b""))
                if address in seeds and len({(h, p) for _, h, p in referrals}) < K // 2:
                    # Routers sometimes answer with one address under many IDs.
                    # BEP 5's bootstrap query for our own ID gives other contacts.
                    try:
                        reply = await self.query(address, b"find_node", {b"target": self.node_id})
                        referrals += compact_nodes(reply.get(b"nodes", b""))
                    except (OSError, ValueError, TimeoutError):
                        pass  # keep the get_peers referrals we already have
                for node_id, host, node_port in referrals:
                    admit((host, node_port), node_id)
            except (OSError, ValueError, TimeoutError):
                return

        try:
            async with asyncio.timeout(timeout):
                async with self._lookup_lock:
                    if not candidates:
                        seeds.update(await self._resolve())
                        for address in seeds:
                            candidates.setdefault(address, None)
                    # Keep ALPHA responsive queries in flight. A dead node holds
                    # its slot for SLOW_QUERY, not for a whole lockstep round.
                    while True:
                        now = loop.time()
                        fresh = [launched[t] + SLOW_QUERY - now for t in in_flight
                                 if now - launched[t] < SLOW_QUERY]
                        while (len(fresh) < ALPHA and len(in_flight) < MAX_IN_FLIGHT
                               and len(queried) < MAX_QUERIES):
                            waiting = [a for a in candidates if a not in queried]
                            if not waiting:
                                break
                            address = min(waiting, key=distance)
                            queried.add(address)
                            task = asyncio.create_task(probe(address))
                            in_flight.add(task)
                            launched[task] = now
                            fresh.append(SLOW_QUERY)
                        if not in_flight:
                            break
                        done, in_flight = await asyncio.wait(
                            in_flight, timeout=max(0.001, min(fresh)) if fresh else None,
                            return_when=asyncio.FIRST_COMPLETED)
                        for task in done:
                            del launched[task]
                            task.result()  # a failing on_peers callback ends the lookup
                    if not peers:
                        self.note(f"lookup found no peers: {len(queried)} queries, {replies} replies"
                                  + (f", closest node shares {shared_prefix_bits(closest, info_hash)} "
                                     "prefix bits" if closest is not None else ""))
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
            self.note(f"lookup deadline of {timeout:g}s expired: {len(queried)} queries, "
                      f"{replies} replies, {len(peers)} peers")
        finally:
            # Drain on deadline, cancellation, or callback failure.
            for task in in_flight:
                task.cancel()
            await asyncio.gather(*in_flight, return_exceptions=True)
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
        self.setup_errors = []
        self.task = None

    @property
    def errors(self):
        """Setup failures first, then the node's bounded query/lookup sample."""
        return (self.setup_errors + self.node.errors)[:MAX_ERRORS]

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
                peers = await self.node.discover(self.torrent.info_hash, port=self.port,
                                                 timeout=self.timeout, on_peers=found)
                self.first_done.set()
                self.changed.set()
                # An empty lookup is retried sooner; the table now holds nodes
                # that replied, so the next lookup starts closer to the hash.
                await asyncio.sleep(REFRESH_SECONDS if peers else RETRY_SECONDS)
        except (OSError, ValueError) as error:
            self.node.metrics.dht_failures += 1
            if len(self.setup_errors) < MAX_ERRORS:
                self.setup_errors.append(f"setup: {describe_error(error)}")
        finally:
            self.first_done.set()
            self.changed.set()
            await self.node.close()

    async def close(self):
        if self.task is not None:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
