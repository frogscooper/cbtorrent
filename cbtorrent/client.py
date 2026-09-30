"""Concurrent session with bounded peer scheduling and verified disk commits."""
import asyncio
import math
import os
from dataclasses import asdict
from hashlib import sha1
from pathlib import Path
from time import perf_counter

from .metrics import Metrics
from .dht import DhtDiscovery
from .observe import build_snapshot
from .policy import ActiveTransfer, Observation, SchedulingContext, ThroughputPolicy
from .pex import PexSession
from .seeder import SeedServer
from .storage import Storage
from .tracker import announce
from .wire import Peer, PieceBuffer, message


class DownloadError(Exception):
    def __init__(self, message, report):
        super().__init__(message)
        self.report = report


class MixedPieceError(ValueError):
    """A mixed-source hash failure cannot identify the corrupt contributor."""


async def download(torrent, peers, output: Path, *, timeout=15.0, piece_timeout=120.0,
                   pipeline=8, policy=None, concurrency=4, max_connections=16,
                   resume=False, use_trackers=True, listen_host="127.0.0.1",
                   listen_port=0, progress=None, observe=None,
                   use_dht=False, dht_bootstrap=None, peer_retries=2, retry_delay=0.5,
                   endgame=True, endgame_delay=1.0, endgame_budget=128 * 1024,
                   use_pex=True):
    """Each task owns one connection. Endgame can share one piece assembly.

    Storage operations run on the event loop, preventing shared seek races.
    Verified partial pieces are available through the incoming TCP listener.
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
    observations, sessions, tasks = {}, {}, {}
    active = {}
    retired, claimed = set(), set()
    retry_at, failures = {}, {}
    pieces, isolated, endgame_pending = {}, set(), set()
    endgame_reserved = 0
    schedule_changed = asyncio.Event()
    peer_errors, tracker_errors = [], []
    peer_id = b"-CB0002-" + os.urandom(12)
    pex_sources, pex_hosts = {}, set()
    pex_task = None
    storage = server = tracker_task = None
    discovery = None
    tracker_urls = torrent.trackers[:8] if use_trackers else ()
    started_trackers = set()
    intervals = {}
    port = 0

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
        result["tracker_errors"] = list(tracker_errors)
        result["dht_errors"] = list(discovery.errors) if discovery else []
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
                tracker_errors.append(f"{url}: {error}")

    async def refresh_trackers():
        while True:
            delay = max(0.1, min(intervals.values()) - perf_counter())
            await asyncio.sleep(delay)
            for url in tracker_urls:
                if intervals[url] <= perf_counter():
                    await tracker_update(url, "" if url in started_trackers else "started")

    async def retire(address, error):
        metrics.peer_failures += 1
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
            peer_errors.append(f"{address[0]}:{address[1]}: {error}")
        peer = sessions.pop(address, None)
        if peer is not None:
            await peer.close()

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
        try:
            async with asyncio.timeout(piece_timeout):
                if peer is None:
                    if address in retry_at:
                        metrics.peer_retries += 1
                    retry_at.pop(address, None)
                    metrics.connections += 1
                    reader, writer = await asyncio.wait_for(asyncio.open_connection(*address), timeout)
                    peer = Peer(reader, writer, torrent, metrics, timeout, pex=make_pex(writer))
                    sessions[address] = peer
                    await peer.handshake(peer_id)
                    await peer.ready()
                    peer.handshaken = True
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
                    state.received = received
                    state.last_progress = perf_counter()
                    if endgame and len(remaining) <= concurrency:
                        schedule_changed.set()
                def on_data(offset):
                    if work.raced:
                        for owner in tuple(work.owners):
                            other = sessions.get(owner)
                            if owner != address and other is not None:
                                other.cancel_block(index, offset)
                data = await peer.download_piece(index, pipeline, on_block=on_block,
                                                  buffer=work, on_data=on_data,
                                                  endgame=duplicate is not None)
                transfer_seconds = perf_counter() - transfer_started
                if work.invalid or index not in remaining:
                    return
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
                await server.have(index)
                await peer.send(message(4, index.to_bytes(4, "big")))
                if progress is not None:
                    progress(metrics.resumed_bytes + metrics.verified_bytes, torrent.length)
                emit()
        except (OSError, ValueError, asyncio.TimeoutError, asyncio.IncompleteReadError) as error:
            failed = True
            await retire(address, error)
        except asyncio.CancelledError:
            interrupted = True
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

    try:
        storage = Storage(torrent, output, resume=resume)
        remaining = set(range(len(torrent.hashes))) - storage.verified
        metrics.resumed_bytes = sum(torrent.piece_size(i) for i in storage.verified)
        server = SeedServer(torrent, storage, metrics=metrics, peer_id=peer_id,
                            timeout=timeout, max_clients=max_connections, pex_factory=make_pex)
        port = await server.start(listen_host, listen_port)
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

        async def make_room(address, busy):
            connecting = sum(p not in sessions for p in busy)
            if address not in sessions and len(sessions) + connecting >= max_connections:
                victim = next((p for p in sessions if p not in busy), None)
                if victim is None:
                    return False
                await sessions.pop(victim).close()
            return True

        while remaining:
            emit()
            busy = set(tasks.values())
            deferred = False
            while len(tasks) < concurrency and remaining - claimed:
                candidates = candidates_for(busy)
                if not candidates:
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
                    tail = unscheduled if len(unscheduled) <= concurrency else set()
                    context = SchedulingContext(
                        perf_counter(), len(unscheduled), {i: torrent.piece_size(i) for i in tail},
                        {p: frozenset(session.available & tail) for p, session in sessions.items()},
                        active.copy(), concurrency, torrent.piece_length)
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
                task = asyncio.create_task(transfer(address))
                tasks[task] = address
                busy.add(address)

            tail_wait = None
            # One spare transfer slot can rescue a stalled tail without adding
            # another piece buffer. At most two owners share any one assembly.
            if (endgame and endgame_reserved < endgame_budget and remaining <= claimed
                    and len(remaining) <= concurrency and len(tasks) < min(max_connections, concurrency + 1)):
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
            if not tasks:
                if waits:
                    if discovery is not None:
                        try:
                            await asyncio.wait_for(discovery.changed.wait(), wait_timeout)
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
                waiting = set(tasks)
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
                del tasks[task]
                if not task.cancelled():  # Verified winners cancel their sibling requests.
                    task.result()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        tasks.clear()
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
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if server is not None:
            await server.close()
        await asyncio.gather(*(peer.close() for peer in sessions.values()))
        if storage is not None:
            storage.close()
        if started_trackers:
            await asyncio.gather(*(tracker_update(url, "stopped") for url in tuple(started_trackers)))
