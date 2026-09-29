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
from .seeder import SeedServer
from .storage import Storage
from .tracker import announce
from .wire import Peer, message


class DownloadError(Exception):
    def __init__(self, message, report):
        super().__init__(message)
        self.report = report


async def download(torrent, peers, output: Path, *, timeout=15.0, piece_timeout=120.0,
                   pipeline=8, policy=None, concurrency=4, max_connections=16,
                   resume=False, use_trackers=True, listen_host="127.0.0.1",
                   listen_port=0, progress=None, observe=None,
                   use_dht=False, dht_bootstrap=None):
    """Each task owns one connection and reserves at most one piece.

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
    peer_errors, tracker_errors = [], []
    peer_id = b"-CB0002-" + os.urandom(12)
    storage = server = tracker_task = None
    discovery = None
    tracker_urls = torrent.trackers[:8] if use_trackers else ()
    started_trackers = set()
    intervals = {}
    port = 0

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
        retired.add(address)
        metrics.peer_failures += 1
        if len(peer_errors) < 30:
            peer_errors.append(f"{address[0]}:{address[1]}: {error}")
        peer = sessions.pop(address, None)
        if peer is not None:
            await peer.close()

    async def transfer(address):
        observation = observations.setdefault(address, Observation())
        started = perf_counter()
        peer = sessions.get(address)
        before = peer.sent_bytes + peer.received_bytes if peer else 0
        index = None
        useful_bytes = 0
        failed = False
        transfer_seconds = None
        try:
            async with asyncio.timeout(piece_timeout):
                if peer is None:
                    metrics.connections += 1
                    reader, writer = await asyncio.wait_for(asyncio.open_connection(*address), timeout)
                    peer = Peer(reader, writer, torrent, metrics, timeout)
                    sessions[address] = peer
                    await peer.handshake(peer_id)
                    await peer.ready()
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
                transfer_started = perf_counter()
                state = ActiveTransfer(index, torrent.piece_size(index), 0, transfer_started)
                active[address] = state
                def on_block(received):
                    state.received = received
                    state.last_progress = perf_counter()
                data = await peer.download_piece(index, pipeline, on_block=on_block)
                transfer_seconds = perf_counter() - transfer_started
                if sha1(data).digest() != torrent.hashes[index]:
                    metrics.hash_failures += 1
                    raise ValueError("piece hash mismatch")
                try:
                    storage.write(index, data)
                except OSError as error:
                    raise DownloadError(str(error), report()) from error
                remaining.remove(index)
                metrics.verified_bytes += len(data)
                useful_bytes = len(data)
                await server.have(index)
                await peer.send(message(4, index.to_bytes(4, "big")))
                if progress is not None:
                    progress(metrics.resumed_bytes + metrics.verified_bytes, torrent.length)
                emit()
        except (OSError, ValueError, asyncio.TimeoutError, asyncio.IncompleteReadError) as error:
            failed = True
            await retire(address, error)
        finally:
            if index is not None:
                claimed.discard(index)
            wire_bytes = (peer.sent_bytes + peer.received_bytes - before) if peer else 0
            observation.record(useful_bytes, perf_counter() - started, wire_bytes, failed)
            active.pop(address, None)
            if useful_bytes and transfer_seconds is not None and hasattr(policy, "observe"):
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
                            timeout=timeout, max_clients=max_connections)
        port = await server.start(listen_host, listen_port)
        if remaining and use_dht and not torrent.private:
            def found_peers(addresses):
                for address in addresses:
                    if address not in peers and len(peers) < 200:
                        peers.append(address)
            discovery = DhtDiscovery(torrent, port, metrics, bootstrap=dht_bootstrap,
                                     timeout=min(timeout, 15.0), on_peers=found_peers,
                                     bind_host=listen_host)
            discovery.start()
        if remaining and tracker_urls:
            await asyncio.gather(*(tracker_update(url, "started") for url in tracker_urls))
            tracker_task = asyncio.create_task(refresh_trackers())
        emit()
        while remaining:
            emit()
            busy = set(tasks.values())
            deferred = False
            while len(tasks) < concurrency and remaining - claimed:
                candidates = [p for p in peers if p not in retired and p not in busy]
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
                connecting = sum(p not in sessions for p in busy)
                if address not in sessions and len(sessions) + connecting >= max_connections:
                    victim = next((p for p in sessions if p not in busy), None)
                    if victim is None:
                        break
                    await sessions.pop(victim).close()
                task = asyncio.create_task(transfer(address))
                tasks[task] = address
                busy.add(address)
            if not tasks:
                if discovery is not None and not discovery.first_done.is_set():
                    await discovery.changed.wait()
                    discovery.changed.clear()
                    continue
                raise ConnectionError("no usable peers remain; partial download can be resumed")
            # New discovery results can fill free slots while existing peers
            # are still connecting or stalled. Drain the event waiter on every
            # exit so cancellation never leaves an orphan task behind.
            wake = asyncio.create_task(discovery.changed.wait()) if discovery else None
            try:
                waiting = set(tasks)
                if wake is not None:
                    waiting.add(wake)
                done, _ = await asyncio.wait(waiting, return_when=asyncio.FIRST_COMPLETED,
                                             timeout=0.05 if deferred else None)
                if wake in done:
                    discovery.changed.clear()
                    done.remove(wake)
            finally:
                if wake is not None:
                    wake.cancel()
                    await asyncio.gather(wake, return_exceptions=True)
            for task in done:
                del tasks[task]
                task.result()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        tasks.clear()
        if discovery is not None:
            await discovery.close()
        await server.close()
        storage.publish()
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
