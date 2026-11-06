"""Concurrent session with bounded peer scheduling and verified disk commits."""
import asyncio
import ipaddress
import math
import os
from dataclasses import asdict
from hashlib import sha1
from pathlib import Path
from time import perf_counter

from .metrics import Metrics
from .dht import DhtDiscovery
from .diagnostics import describe_error, install_reset_filter
from .observe import build_snapshot
from .policy import ActiveTransfer, Observation, SchedulingContext, ThroughputPolicy
from .pex import PexSession
from .portmap import PortMapper
from .seeder import SeedServer
from .storage import Storage
from .tracker import announce
from .wire import Peer, PieceBuffer, message


class DownloadError(Exception):
    def __init__(self, message, report):
        super().__init__(message)
        self.report = report


class StreamAbandoned(Exception):
    """The policy ended a slow stream; its unfinished pieces return to the pool."""


class MixedPieceError(ValueError):
    """A mixed-source hash failure cannot identify the corrupt contributor."""


# Half-open connection attempts (TCP connect or handshake) opened ahead of
# transfer slots. Unreachable addresses then cost a dial slot for one timeout
# instead of idling a piece transfer. Connected peers that have not unchoked us
# yet wait outside this limit, bounded by max_connections.
DIAL_AHEAD = 8


def loopback_host(host):
    """A listener on loopback cannot be reached through the gateway, so never map it."""
    if host in ("localhost", ""):
        return host == "localhost"
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


async def download(torrent, peers, output: Path, *, timeout=15.0, piece_timeout=120.0,
                   pipeline=8, policy=None, concurrency=4, max_connections=16,
                   resume=False, use_trackers=True, listen_host="127.0.0.1",
                   listen_port=0, progress=None, observe=None,
                   use_dht=False, dht_bootstrap=None, peer_retries=2, retry_delay=0.5,
                   endgame=True, endgame_delay=1.0, endgame_budget=128 * 1024,
                   use_pex=True, port_mapping=False):
    """Each task owns one connection. Endgame can share one piece assembly.

    Storage operations run on the event loop, preventing shared seek races.
    Verified partial pieces are available through the incoming TCP listener.
    With port_mapping, the listener is mapped on the LAN gateway in the
    background (NAT-PMP, then UPnP) so peers behind NAT can connect to us.
    """
    peers = list(dict.fromkeys(peers))
    if len(peers) > 200:
        raise ValueError("provide at most 200 distinct peers")
    if (not math.isfinite(timeout) or not math.isfinite(piece_timeout)
            or timeout <= 0 or piece_timeout <= 0 or not 1 <= pipeline <= 128):
        raise ValueError("timeouts must be positive; pipeline must be 1..128")
    if not 1 <= concurrency <= max_connections <= 64:
        raise ValueError("require 1 <= concurrency <= max_connections <= 64")
    if (not isinstance(peer_retries, int) or not 0 <= peer_retries <= 8
            or not math.isfinite(retry_delay) or not 0.01 <= retry_delay <= 30):
        raise ValueError("peer_retries must be 0..8; retry_delay must be 0.01..30")
    if (not math.isfinite(endgame_delay) or not 0.01 <= endgame_delay <= 60
            or not isinstance(endgame_budget, int) or not 0 <= endgame_budget <= 1024 * 1024):
        raise ValueError("endgame_delay must be 0.01..60; endgame_budget must be 0..1048576")
    if any(not host or not 1 <= port <= 65535 for host, port in peers):
        raise ValueError("invalid peer address")
    if not 0 <= listen_port <= 65535:
        raise ValueError("invalid listen port")
    output = Path(output)
    if output.exists():
        raise FileExistsError(output)
    metrics = Metrics()
    metrics.start()
    policy = policy or ThroughputPolicy()
    slots = [concurrency]  # transfer slots now; a policy may adapt it within max_connections

    def update_slots():
        if hasattr(policy, "target_concurrency"):
            target = policy.target_concurrency()
            if type(target) is not int:
                raise ValueError("policy returned a non-integer concurrency")
            slots[0] = max(1, min(max_connections, target))
        return slots[0]

    def depth_for(address):
        if not hasattr(policy, "pipeline_for"):
            return pipeline
        depth = policy.pipeline_for(address)
        if type(depth) is not int:
            raise ValueError("policy returned a non-integer pipeline depth")
        return max(1, min(128, depth))
    observations, sessions, tasks, dialing = {}, {}, {}, {}  # dialing: task -> (address, step)
    active = {}
    retired, claimed = set(), set()
    retry_at, failures = {}, {}
    pieces, isolated, endgame_pending = {}, set(), set()
    endgame_reserved = 0
    schedule_changed = asyncio.Event()
    peer_errors, tracker_errors = [], []
    failure_phases = {}
    peer_id = b"-CB0002-" + os.urandom(12)
    pex_sources, pex_hosts = {}, set()
    pex_task = None
    storage = server = tracker_task = None
    discovery = None
    mapper = None
    tracker_urls = torrent.trackers[:8] if use_trackers else ()
    started_trackers = set()
    intervals = {}
    port = 0  # the port we announce: the router's external port once mapped

    def connected_peers():
        return tuple(peer.writer.get_extra_info("peername")[:2] for peer in sessions.values()
                     if peer.handshaken and not peer.writer.is_closing())

    def make_pex(writer):
        if not use_pex or torrent.private:
            return None
        remote = writer.get_extra_info("peername")[:2]
        def discovered(addresses):
            if remote[0] not in pex_sources and len(pex_sources) >= 32:
                return
            source_hosts = pex_sources.setdefault(remote[0], set())
            for address in addresses:
                if len(peers) >= 200 or len(pex_hosts) >= 100 or len(source_hosts) >= 25:
                    break
                if address[0] in pex_hosts or address in peers:
                    continue
                source_hosts.add(address[0])
                pex_hosts.add(address[0])
                peers.append(address)
                metrics.pex_peers += 1
                schedule_changed.set()
        return PexSession(remote, discover=discovered, connected=connected_peers)

    async def refresh_pex():
        while True:
            await asyncio.sleep(60)
            # Send only on fully handshaken outbound connections. Inbound
            # contacts' source ports are not evidence of reachable listen ports.
            connected = set(sessions.values()) | set(server.peers)
            await asyncio.gather(*(peer.pex_update() for peer in connected
                                   if peer.handshaken), return_exceptions=True)

    def report(complete=False):
        result = metrics.report(complete=complete)
        result["policy"] = type(policy).__name__
        result["peer_observations"] = {f"{host}:{peer_port}": asdict(value)
                                       for (host, peer_port), value in observations.items()}
        result["peer_errors"] = list(peer_errors)
        result["peer_failure_phases"] = dict(failure_phases)
        result["tracker_errors"] = list(tracker_errors)
        result["dht_errors"] = list(discovery.errors) if discovery else []
        result["port_mapping"] = mapper.status() if mapper else None
        if hasattr(policy, "diagnostics"):
            result["policy_diagnostics"] = policy.diagnostics()
        return result

    last_emit = [0.0]

    def emit(status="running", error=None):
        if observe is None:
            return
        now = perf_counter()
        if status == "running" and now - last_emit[0] < 0.2:
            return
        last_emit[0] = now
        observe(build_snapshot(
            name=torrent.name, length=torrent.length, metrics=metrics,
            observations=observations, sessions=sessions, active=active,
            retired=retired, status=status, error=error))

    async def tracker_update(url, event):
        metrics.tracker_requests += 1
        try:
            result = await announce(url, torrent.info_hash, peer_id, port=port,
                                    downloaded=metrics.payload_received_bytes,
                                    uploaded=metrics.uploaded_bytes,
                                    left=torrent.length - metrics.verified_bytes - metrics.resumed_bytes,
                                    event=event, timeout=min(timeout, 10.0))
            metrics.tracker_response_bytes += result.response_bytes
            for address in result.peers:
                if address not in peers and len(peers) < 200:
                    peers.append(address)
                    schedule_changed.set()
            intervals[url] = perf_counter() + max(1, result.interval)
            started_trackers.add(url)
        except (OSError, ValueError, asyncio.TimeoutError) as error:
            metrics.tracker_failures += 1
            intervals[url] = perf_counter() + 60
            if len(tracker_errors) < 20:
                tracker_errors.append(f"{url}: {describe_error(error)}")

    async def refresh_trackers():
        while True:
            delay = max(0.1, min(intervals.values()) - perf_counter())
            await asyncio.sleep(delay)
            for url in tracker_urls:
                if intervals[url] <= perf_counter():
                    await tracker_update(url, "" if url in started_trackers else "started")

    async def retire(address, error, phase, detail=None):
        metrics.peer_failures += 1
        failure_phases[phase] = failure_phases.get(phase, 0) + 1
        failures[address] = failures.get(address, 0) + 1
        permanent = isinstance(error, ValueError) and not isinstance(error, MixedPieceError)
        if permanent or failures[address] > peer_retries:
            retired.add(address)
            retry_at.pop(address, None)
            if permanent:
                metrics.peers_banned += 1
        else:
            retry_at[address] = perf_counter() + min(30, retry_delay * 2 ** (failures[address] - 1))
        if len(peer_errors) < 30:
            peer_errors.append(f"{address[0]}:{address[1]}: {phase}: {detail or describe_error(error)}")
        peer = sessions.pop(address, None)
        if peer is not None:
            await peer.close()

    def failure_detail(error, phase, deadline, index=None):
        detail = None
        if isinstance(error, TimeoutError):
            limit = piece_timeout if deadline is not None and deadline.expired() else timeout
            detail = f"timed out after {limit:g}s"
        if phase in ("piece", "verify") and index is not None:
            detail = f"piece {index}: {detail or describe_error(error)}"
        return detail

    async def open_peer(address, step):
        """Connect, handshake, and wait for an unchoke; step[0] names the stage.

        The session is registered before the handshake, so callers find it in
        `sessions` to count its bytes or close it if a later stage fails.
        """
        if address in retry_at:
            metrics.peer_retries += 1
        retry_at.pop(address, None)
        metrics.connections += 1
        async with asyncio.timeout(timeout):
            reader, writer = await asyncio.open_connection(*address)
        peer = Peer(reader, writer, torrent, metrics, timeout, pex=make_pex(writer))
        sessions[address] = peer
        step[0] = "handshake"
        await peer.handshake(peer_id)
        step[0] = "unchoke"
        schedule_changed.set()  # no longer half-open: the scheduler may dial another
        await peer.ready()
        peer.handshaken = True
        return peer

    async def dial(address, step):
        """Open a session ahead of a transfer slot; the policy chooses later.

        A failure is recorded exactly like a failed first transfer. Success adds
        no observation, so policies still treat the ready peer as unexplored.
        """
        started = perf_counter()
        deadline = None
        try:
            async with asyncio.timeout(piece_timeout) as deadline:
                await open_peer(address, step)
        except (OSError, ValueError, asyncio.TimeoutError, asyncio.IncompleteReadError) as error:
            peer = sessions.get(address)
            wire_bytes = peer.sent_bytes + peer.received_bytes if peer else 0
            observations.setdefault(address, Observation()).record(
                0, perf_counter() - started, wire_bytes, True)
            await retire(address, error, step[0], failure_detail(error, step[0], deadline))
        except asyncio.CancelledError:
            peer = sessions.pop(address, None)
            if peer is not None:
                await peer.close()
            raise
        finally:
            schedule_changed.set()

    async def transfer(address, duplicate=None):
        observation = observations.setdefault(address, Observation())
        started = perf_counter()
        peer = sessions.get(address)
        before = peer.sent_bytes + peer.received_bytes if peer else 0
        index = None
        useful_bytes = 0
        failed = False
        transfer_seconds = None
        work = None
        completed = False
        interrupted = False
        # The failure phase separates dead addresses from slow or choking peers.
        step, deadline = ["connect"], None
        try:
            async with asyncio.timeout(piece_timeout) as deadline:
                if peer is None:
                    peer = await open_peer(address, step)
                step[0] = "wait"
                if duplicate is not None:
                    if (duplicate not in remaining or duplicate not in peer.available
                            or duplicate not in pieces or pieces[duplicate].invalid):
                        return
                    index = duplicate
                else:
                    while True:
                        missing = remaining - claimed
                        if not missing:
                            return
                        useful = peer.available & missing
                        if useful:
                            break
                        await peer.receive()
                    index = min(useful, key=lambda i: (sum(i in p.available for p in sessions.values()), i))
                claimed.add(index)
                work = pieces.get(index)
                if work is None:
                    work = pieces[index] = PieceBuffer(torrent.piece_size(index))
                work.owners.add(address)
                if duplicate is not None:
                    work.raced = True
                    metrics.endgame_transfers += 1
                transfer_started = perf_counter()
                state = ActiveTransfer(index, torrent.piece_size(index), 0, transfer_started)
                active[address] = state
                schedule_changed.set()
                def on_block(received):
                    if hasattr(policy, "record_bytes") and received > state.received:
                        policy.record_bytes(received - state.received)
                    state.received = received
                    state.last_progress = perf_counter()
                    if endgame and len(remaining) <= slots[0]:
                        schedule_changed.set()
                def on_data(offset):
                    if work.raced:
                        for owner in tuple(work.owners):
                            other = sessions.get(owner)
                            if owner != address and other is not None:
                                other.cancel_block(index, offset)
                step[0] = "piece"
                data = await peer.download_piece(index, depth_for(address), on_block=on_block,
                                                  buffer=work, on_data=on_data,
                                                  endgame=duplicate is not None,
                                                  stall_delay=endgame_delay)
                transfer_seconds = perf_counter() - transfer_started
                if work.invalid or index not in remaining:
                    return
                step[0] = "verify"
                if sha1(data).digest() != torrent.hashes[index]:
                    metrics.hash_failures += 1
                    work.invalid = True
                    for other_task, owner in tuple(tasks.items()):
                        if owner in work.owners and other_task is not asyncio.current_task():
                            other_task.cancel()
                    if work.raced:
                        isolated.add(index)
                        raise MixedPieceError("mixed-source piece hash mismatch; retrying without endgame")
                    raise ValueError("piece hash mismatch")
                try:
                    storage.write(index, data)
                except OSError as error:
                    raise DownloadError(str(error), report()) from error
                remaining.remove(index)
                metrics.verified_bytes += len(data)
                completed = True
                # A mixed assembly is not a measured single-peer service sample.
                useful_bytes = len(data) if not work.raced else 0
                if work.raced:
                    metrics.endgame_verified_bytes += len(data)
                for other_task, owner in tuple(tasks.items()):
                    if owner in work.owners and other_task is not asyncio.current_task():
                        other_task.cancel()
                step[0] = "have"
                await server.have(index)
                await peer.send(message(4, index.to_bytes(4, "big")))
                if progress is not None:
                    progress(metrics.resumed_bytes + metrics.verified_bytes, torrent.length)
                emit()
        except (OSError, ValueError, asyncio.TimeoutError, asyncio.IncompleteReadError) as error:
            failed = True
            peer = peer or sessions.get(address)  # a failed open_peer registered it
            await retire(address, error, step[0], failure_detail(error, step[0], deadline, index))
        except asyncio.CancelledError:
            interrupted = True
            peer = peer or sessions.get(address)
            raise
        finally:
            if work is not None:
                work.owners.discard(address)
                if not work.owners:
                    claimed.discard(index)
                    pieces.pop(index, None)
            if duplicate is not None:
                endgame_pending.discard(duplicate)
            if peer is not None and (interrupted or (work is not None and work.raced)):
                if sessions.get(address) is peer:
                    sessions.pop(address)
                await peer.close()
            wire_bytes = (peer.sent_bytes + peer.received_bytes - before) if peer else 0
            if work is not None and work.raced:
                observation.seconds += perf_counter() - started
                observation.wire_bytes += wire_bytes
                observation.failures += int(failed)
            else:
                observation.record(useful_bytes, perf_counter() - started, wire_bytes, failed)
            active.pop(address, None)
            if useful_bytes and completed and transfer_seconds is not None and hasattr(policy, "observe"):
                update_started = perf_counter()
                policy.observe(address, useful_bytes, transfer_seconds)
                metrics.policy_update_seconds += perf_counter() - update_started
            if hasattr(policy, "attempt_finished"):
                update_started = perf_counter()
                policy.attempt_finished(address, perf_counter() - started)
                metrics.policy_update_seconds += perf_counter() - update_started

    def make_context():
        unscheduled = remaining - claimed
        # Tail planning needs at most `slots` piece sizes, not a full copied
        # bitfield/dictionary for every scheduling decision.
        tail = unscheduled if len(unscheduled) <= slots[0] else set()
        return SchedulingContext(
            perf_counter(), len(unscheduled), {i: torrent.piece_size(i) for i in tail},
            {p: frozenset(session.available & tail) for p, session in sessions.items()},
            active.copy(), slots[0], torrent.piece_length)

    def idle_ready_peers():
        busy = set(tasks.values()) | {address for address, _ in dialing.values()}
        wanted = remaining - claimed
        now = perf_counter()
        return [p for p, s in sessions.items() if p not in busy and p not in retired
                and retry_at.get(p, 0) <= now and s.handshaken and s.available & wanted]

    async def stream(address):
        """Like transfer(), but keep one connection busy across consecutive pieces.

        Each piece is verified and committed exactly as in transfer(). The policy
        decides before every new claim whether this peer should take more work.
        """
        observation = observations.setdefault(address, Observation())
        started = perf_counter()
        peer = sessions.get(address)
        before = peer.sent_bytes + peer.received_bytes if peer else 0
        owned = {}
        failed = interrupted = raced = False
        step, deadline = ["connect"], None
        mark = [None, 0]  # time and wire bytes at the last completed piece
        claims = [0]
        delivered = [0, 0.0]  # payload bytes this stream, last abandon check time
        abandoned = False

        def release(index):
            work = owned.pop(index, None)
            if work is None:
                return
            work.owners.discard(address)
            if not work.owners:
                claimed.discard(index)
                pieces.pop(index, None)

        try:
            async with asyncio.timeout(piece_timeout) as deadline:
                if peer is None:
                    peer = await open_peer(address, step)
                step[0] = "wait"
                while True:
                    missing = remaining - claimed
                    if not missing:
                        return
                    if peer.available & missing:
                        break
                    await peer.receive()
                state = ActiveTransfer(-1, 0, 0, perf_counter())
                active[address] = state
                mark[0], mark[1] = perf_counter(), peer.sent_bytes + peer.received_bytes
                mark_start = mark[0]

                def claim():
                    # The scheduler chose this peer for the first piece; later
                    # pieces need the policy's consent.
                    if claims[0] and not policy.continue_stream(address, make_context(), idle_ready_peers()):
                        return None
                    useful = peer.available & (remaining - claimed)
                    if not useful:
                        return None
                    index = min(useful, key=lambda i: (sum(i in p.available for p in sessions.values()), i))
                    claimed.add(index)
                    work = pieces[index] = PieceBuffer(torrent.piece_size(index))
                    work.owners.add(address)
                    owned[index] = work
                    claims[0] += 1
                    if state.index < 0 or state.index not in owned:
                        state.index, state.size, state.received = index, torrent.piece_size(index), 0
                    schedule_changed.set()
                    return index, work

                def on_block(index, size):
                    if hasattr(policy, "record_bytes"):
                        policy.record_bytes(size)
                    if index == state.index:
                        state.received += size
                    now = state.last_progress = perf_counter()
                    delivered[0] += size
                    if endgame and len(remaining) <= slots[0]:
                        schedule_changed.set()
                    elapsed = now - mark_start
                    if hasattr(policy, "abandon") and elapsed >= 1.0 and now - delivered[1] >= 0.5:
                        delivered[1] = now
                        if policy.abandon(address, delivered[0] / elapsed, make_context(), idle_ready_peers()):
                            raise StreamAbandoned()

                def on_data(index, offset):
                    work = owned.get(index)
                    if work is not None and work.raced:
                        for owner in tuple(work.owners):
                            other = sessions.get(owner)
                            if owner != address and other is not None:
                                other.cancel_block(index, offset)

                async def on_piece(index, data):
                    nonlocal raced
                    work = owned[index]
                    if work.invalid or index not in remaining:
                        release(index)
                        return
                    step[0] = "verify"
                    if sha1(data).digest() != torrent.hashes[index]:
                        metrics.hash_failures += 1
                        work.invalid = True
                        for other_task, owner in tuple(tasks.items()):
                            if owner in work.owners and other_task is not asyncio.current_task():
                                other_task.cancel()
                        if work.raced:
                            raced = True
                            isolated.add(index)
                            raise MixedPieceError("mixed-source piece hash mismatch; retrying without endgame")
                        raise ValueError("piece hash mismatch")
                    try:
                        storage.write(index, data)
                    except OSError as error:
                        raise DownloadError(str(error), report()) from error
                    remaining.remove(index)
                    metrics.verified_bytes += len(data)
                    if work.raced:
                        raced = True
                        metrics.endgame_verified_bytes += len(data)
                    for other_task, owner in tuple(tasks.items()):
                        if owner in work.owners and other_task is not asyncio.current_task():
                            other_task.cancel()
                    release(index)
                    now = perf_counter()
                    wire = peer.sent_bytes + peer.received_bytes
                    if not work.raced:
                        # Back-to-back pieces overlap, so the interval between
                        # completions is this connection's real delivery time.
                        observation.record(len(data), now - mark[0], wire - mark[1])
                        if hasattr(policy, "observe"):
                            update_started = perf_counter()
                            policy.observe(address, len(data), now - mark[0])
                            metrics.policy_update_seconds += perf_counter() - update_started
                    mark[0], mark[1] = now, wire
                    deadline.reschedule(asyncio.get_running_loop().time() + piece_timeout)
                    if owned:
                        head = next(iter(owned))
                        state.index, state.size = head, torrent.piece_size(head)
                        state.received = owned[head].received_bytes
                    else:
                        state.index = -1
                    step[0] = "have"
                    await server.have(index)
                    await peer.send(message(4, index.to_bytes(4, "big")))
                    if progress is not None:
                        progress(metrics.resumed_bytes + metrics.verified_bytes, torrent.length)
                    emit()
                    schedule_changed.set()
                    step[0] = "piece"

                step[0] = "piece"
                await peer.download_stream(claim, lambda: depth_for(address), on_piece,
                                           on_block=on_block, on_data=on_data)
        except (OSError, ValueError, asyncio.TimeoutError, asyncio.IncompleteReadError) as error:
            failed = True
            peer = peer or sessions.get(address)
            index = next(iter(owned), None)
            await retire(address, error, step[0], failure_detail(error, step[0], deadline, index))
        except StreamAbandoned:
            # Not a peer failure. Learn its measured rate so it is not picked
            # again soon; close the session, since cancelled blocks may still arrive.
            abandoned = True
            elapsed = perf_counter() - mark_start
            if delivered[0] and elapsed > 0 and hasattr(policy, "observe"):
                policy.observe(address, delivered[0], elapsed)
        except asyncio.CancelledError:
            interrupted = True
            peer = peer or sessions.get(address)
            raise
        finally:
            for index in tuple(owned):
                release(index)
            if peer is not None and (interrupted or raced or abandoned):
                if sessions.get(address) is peer:
                    sessions.pop(address)
                await peer.close()
            if peer is not None and (failed or mark[0] is None):
                now_wire = peer.sent_bytes + peer.received_bytes
                since = mark[0] if mark[0] is not None else started
                base = mark[1] if mark[0] is not None else before
                observation.record(0, perf_counter() - since, now_wire - base, failed)
            elif peer is None:
                observation.record(0, perf_counter() - started, 0, failed)
            active.pop(address, None)
            if hasattr(policy, "attempt_finished"):
                policy.attempt_finished(address, perf_counter() - started)

    install_reset_filter()
    try:
        storage = Storage(torrent, output, resume=resume)
        remaining = set(range(len(torrent.hashes))) - storage.verified
        metrics.resumed_bytes = sum(torrent.piece_size(i) for i in storage.verified)
        server = SeedServer(torrent, storage, metrics=metrics, peer_id=peer_id,
                            timeout=timeout, max_clients=max_connections, pex_factory=make_pex)
        port = await server.start(listen_host, listen_port)
        if port_mapping and not loopback_host(listen_host):
            def mapped(mapping):
                nonlocal port
                port = mapping.external_port
                if discovery is not None:
                    discovery.port = port
            mapper = PortMapper(port, on_mapped=mapped)
            mapper.start()
        if use_pex and not torrent.private:
            pex_task = asyncio.create_task(refresh_pex())
        if remaining and use_dht and not torrent.private:
            def found_peers(addresses):
                for address in addresses:
                    if address not in peers and len(peers) < 200:
                        peers.append(address)
                        schedule_changed.set()
            discovery = DhtDiscovery(torrent, port, metrics, bootstrap=dht_bootstrap,
                                     timeout=min(timeout, 15.0), on_peers=found_peers,
                                     bind_host=listen_host)
            discovery.start()
        if remaining and tracker_urls:
            await asyncio.gather(*(tracker_update(url, "started") for url in tracker_urls))
            tracker_task = asyncio.create_task(refresh_trackers())
        emit()

        def candidates_for(busy):
            now = perf_counter()
            return [p for p in peers if p not in retired and p not in busy
                    and retry_at.get(p, 0) <= now]

        def open_connections(busy):
            return len(sessions) + sum(p not in sessions for p in busy)

        async def make_room(address, busy):
            if address not in sessions and open_connections(busy) >= max_connections:
                victim = next((p for p in sessions if p not in busy), None)
                if victim is None:
                    return False
                await sessions.pop(victim).close()
            return True

        while remaining:
            emit()
            busy = set(tasks.values()) | {address for address, _ in dialing.values()}
            deferred = False
            if remaining - claimed:
                half_open = sum(step[0] != "unchoke" for _, step in dialing.values())
                for address in candidates_for(busy):
                    if half_open >= DIAL_AHEAD or open_connections(busy) >= max_connections:
                        break
                    if address not in sessions:
                        step = ["connect"]
                        dialing[asyncio.create_task(dial(address, step))] = (address, step)
                        busy.add(address)
                        half_open += 1
            update_slots()
            while len(tasks) < slots[0] and remaining - claimed:
                candidates = candidates_for(busy)
                if not candidates:
                    break
                # Offer the policy connected, unchoked peers with wanted pieces.
                # While dials are pending, a slot never waits on a dead address.
                ready = [p for p in candidates if p in sessions and sessions[p].handshaken
                         and sessions[p].available & (remaining - claimed)]
                if ready:
                    candidates = ready
                elif dialing:
                    break
                useful_candidates = [p for p in candidates if p not in sessions
                                     or sessions[p].available & (remaining - claimed)]
                if useful_candidates:
                    candidates = useful_candidates
                started = perf_counter()
                if hasattr(policy, "choose_with_context"):
                    unscheduled = remaining - claimed
                    # Tail planning needs at most concurrency piece sizes, not a
                    # full copied bitfield/dictionary for every scheduling decision.
                    tail = unscheduled if len(unscheduled) <= slots[0] else set()
                    context = SchedulingContext(
                        perf_counter(), len(unscheduled), {i: torrent.piece_size(i) for i in tail},
                        {p: frozenset(session.available & tail) for p, session in sessions.items()},
                        active.copy(), slots[0], torrent.piece_length)
                    address = policy.choose_with_context(candidates, observations, context)
                else:
                    address = policy.choose(candidates, observations)
                metrics.policy_seconds += perf_counter() - started
                if address is None and tasks:
                    metrics.policy_deferrals += 1
                    deferred = True
                    break
                if address not in candidates:
                    raise ValueError("policy selected an ineligible peer")
                if not await make_room(address, busy):
                    break
                task = asyncio.create_task(stream(address) if getattr(policy, "stream", False)
                                           else transfer(address))
                tasks[task] = address
                busy.add(address)

            tail_wait = None
            # One spare transfer slot can rescue a stalled tail without adding
            # another piece buffer. At most two owners share any one assembly.
            if (endgame and endgame_reserved < endgame_budget and remaining <= claimed
                    and len(remaining) <= slots[0] and len(tasks) < min(max_connections, slots[0] + 1)):
                for index, work in tuple(pieces.items()):
                    if (index not in remaining or index in isolated or index in endgame_pending
                            or len(work.owners) != 1 or work.invalid or not work.missing_bytes
                            or work.missing_bytes > endgame_budget - endgame_reserved):
                        continue
                    owner = next(iter(work.owners))
                    state = active.get(owner)
                    if state is None:
                        continue
                    candidates = [p for p in candidates_for(busy) if p not in sessions
                                  or index in sessions[p].available]
                    if not candidates:
                        continue
                    delay = endgame_delay - (perf_counter() - state.last_progress)
                    if delay > 0:
                        tail_wait = delay if tail_wait is None else min(tail_wait, delay)
                        continue
                    started = perf_counter()
                    address = policy.choose(candidates, observations)
                    metrics.policy_seconds += perf_counter() - started
                    if address not in candidates:
                        raise ValueError("policy selected an ineligible endgame peer")
                    if not await make_room(address, busy):
                        continue
                    endgame_reserved += work.missing_bytes
                    endgame_pending.add(index)
                    task = asyncio.create_task(transfer(address, index))
                    tasks[task] = address
                    busy.add(address)
                    break

            waits = [at - perf_counter() for p, at in retry_at.items()
                     if p not in busy and p not in retired and at > perf_counter()]
            if tail_wait is not None:
                waits.append(tail_wait)
            if deferred:
                waits.append(0.05)
            wait_timeout = max(0.001, min(waits)) if waits else None
            if not tasks and not dialing:
                if waits:
                    if discovery is not None:
                        try:
                            async with asyncio.timeout(wait_timeout):
                                await discovery.changed.wait()
                            discovery.changed.clear()
                        except asyncio.TimeoutError:
                            pass
                    else:
                        await asyncio.sleep(wait_timeout)
                    continue
                if discovery is not None and not discovery.first_done.is_set():
                    await discovery.changed.wait()
                    discovery.changed.clear()
                    continue
                raise ConnectionError("no usable peers remain; partial download can be resumed")
            # New discovery results can fill free slots while existing peers
            # are still connecting or stalled. Drain the event waiter on every
            # exit so cancellation never leaves an orphan task behind.
            wake = asyncio.create_task(discovery.changed.wait()) if discovery else None
            progress_wake = asyncio.create_task(schedule_changed.wait())
            try:
                waiting = set(tasks) | set(dialing)
                if wake is not None:
                    waiting.add(wake)
                if progress_wake is not None:
                    waiting.add(progress_wake)
                done, _ = await asyncio.wait(waiting, return_when=asyncio.FIRST_COMPLETED,
                                             timeout=wait_timeout)
                if wake in done:
                    discovery.changed.clear()
                    done.remove(wake)
                if progress_wake in done:
                    schedule_changed.clear()
                    done.remove(progress_wake)
            finally:
                waiters = [t for t in (wake, progress_wake) if t is not None]
                for waiter in waiters:
                    waiter.cancel()
                await asyncio.gather(*waiters, return_exceptions=True)
            for task in done:
                if dialing.pop(task, None) is None:
                    del tasks[task]
                if not task.cancelled():  # Verified winners cancel their sibling requests.
                    task.result()
        for task in (*tasks, *dialing):
            task.cancel()
        await asyncio.gather(*tasks, *dialing, return_exceptions=True)
        tasks.clear()
        dialing.clear()
        if discovery is not None:
            await discovery.close()
        await server.close()
        await storage.publish_async()
        result = report(complete=True)
        emit(status="complete")
        if started_trackers:
            await asyncio.gather(*(tracker_update(url, "completed") for url in tuple(started_trackers)))
        return result
    except (OSError, ValueError, ConnectionError) as error:
        emit(status="error", error=str(error))
        raise DownloadError(str(error), report()) from error
    except asyncio.CancelledError:
        emit(status="cancelled")
        raise
    finally:
        if discovery is not None:
            await discovery.close()
        if tracker_task is not None:
            tracker_task.cancel()
            await asyncio.gather(tracker_task, return_exceptions=True)
        if pex_task is not None:
            pex_task.cancel()
            await asyncio.gather(pex_task, return_exceptions=True)
        for task in (*tasks, *dialing):
            task.cancel()
        await asyncio.gather(*tasks, *dialing, return_exceptions=True)
        if server is not None:
            await server.close()
        if mapper is not None:
            await mapper.close()
        await asyncio.gather(*(peer.close() for peer in sessions.values()))
        if storage is not None:
            storage.close()
        if started_trackers:
            await asyncio.gather(*(tracker_update(url, "stopped") for url in tuple(started_trackers)))
