# Short PR explanations

These notes are a starting point for understanding and changing the code yourself.

## [#11 — Queue controls](https://github.com/frogscooper/cbtorrent/pull/11)

**What changed:** Start Queue downloads items in order. Stop keeps the current
item queued; Pause leaves it paused. Errors stop automatic advancement.

**How it works:** `Session` stores the list, order, statuses, and whether the queue
should run. `QueueScheduler` chooses the next item. `DownloadController` runs the
async download on a background thread so the window stays responsive. The GUI polls
the scheduler; it waits for the old worker to exit before starting another download,
even if the old worker has already reported completion.

**Read first:** `cbtorrent/gui/scheduler.py`, especially `poll()` and
`_settle_finished()`. Tests live in `tests/test_queue_scheduler.py`.

**Try yourself:** Write a test with three items where the middle one fails. Check
that the third stays queued. This teaches the difference between a saved status
and a worker that is actually running.

## [#12 — Multi-file torrents](https://github.com/frogscooper/cbtorrent/pull/12)

**What changed:** A torrent can contain a directory of files, including nested and
empty files. Download, resume, seeding, and the queue understand that layout.

**How it works:** BitTorrent treats the files as one continuous byte stream. A
piece can begin in one file and end in another. The manifest records each file's
path, length, and starting offset. Downloads first go into one `.part` file; only
hash-verified pieces count as complete. At completion, storage reconstructs the
directory using exclusive file creation. Failed publication keeps `.part` for
resume. Paths are validated to prevent traversal and conflicting destinations.

**Read first:** `manifest()` in `cbtorrent/metainfo.py`, then `publish_async()` in
`cbtorrent/storage.py`. Tests live in `tests/test_multifile.py`.

**Tradeoff:** Publication needs roughly twice the payload size temporarily and is
not atomic across a crash.

**Try yourself:** Make two tiny files whose boundary falls inside a piece. Trace
their offsets and check that changing either file changes that piece's hash.

## [#13 — Magnet links](https://github.com/frogscooper/cbtorrent/pull/13)

**What changed:** Paste a v1 magnet in the GUI or pass it to `download`. It can
find peers through trackers, DHT, or explicit addresses, then download files or
directories through the existing engine.

**How it works:** A magnet initially contains an info hash, not file sizes or
piece hashes. BEP 10 negotiates a peer's metadata message number. BEP 9 requests
16 KiB blocks containing the info dictionary. We assemble those blocks and check
their SHA-1 against the magnet before trusting the layout. Only then does
`client.download()` create storage and choose payload peers. Metadata exchange
does not train the ML policies.

**Tradeoff:** Three metadata peers can race, improving resilience at some extra
traffic cost. Deadlines and size limits bound that cost. Reports separate metadata
traffic and include its time in magnet completion time. Metadata is cached only
until the app closes; private magnets require the original `.torrent`.

**Read first:** `magnet.py` (`fetch_metadata`, `download_magnet`), `extensions.py`,
and `tests/test_magnet.py`.

**Try it:** Run the corrupt-metadata fallback test. Change one byte of a peer's
metadata and follow why the downloader never creates a `.part` file from it.

## [#14 — Independent-client testing](https://github.com/frogscooper/cbtorrent/pull/14)

**What changed:** An optional test harness downloads between cbtorrent and
qBittorrent in both directions. Nine cases cover single files, nested directories,
empty files, torrent files, magnets, and cancelling/resuming a directory download.
CI runs the same harness and saves its JSON report, including failures.

**How it works:** The harness starts a separate qBittorrent process with a disposable
profile and uses its local Web API to add torrents and connect explicit peers.
Discovery and update checks are disabled. Before downloads, a bounded handshake
probe waits for the seed to actually accept connections: an API progress flag can
be ready earlier. Completed files are compared with the fixtures and rehashed.

**Tradeoff:** This needs an external executable for integration testing; the app
still uses only the standard library. Loopback correctness does not prove faster
public downloads.

**Read first:** `integration/qbittorrent.py`, `tests/test_interop.py`, and the
`interoperability` job in `.github/workflows/tests.yml`.

**Try yourself:** Run the cancellation-readiness test, then the nine-case harness.
Find the resumed byte count and explain why it reduces newly received payload.

## [#15 — Peer recovery and endgame](https://github.com/frogscooper/cbtorrent/pull/15)

**What changed:** Temporary disconnects, timeouts, and chokes can reconnect with
backoff. A stalled download tail can use another peer to fetch only its missing
blocks. Both features share the normal connection limits and work with magnets.

**How it works:** The scheduler tracks a finite failure budget per address and
lets healthy peers work during cooldown. Endgame adds at most one transfer slot,
shares a piece buffer between two owners, and reserves a limited byte allowance.
Arriving blocks trigger cancel messages to the other owner. Raced connections
close before reuse, preventing late responses from leaking into another piece.

**Correctness:** Only `client.py` commits a fully hash-verified assembly, once.
If mixed-source data fails its hash, the piece is retried without mixing sources.
Mixed assemblies do not train bandit rewards or peer service-time models because
their contribution cannot be attributed reliably.

**Read first:** `transfer()` and the endgame loop in `client.py`, `PieceBuffer`
and `download_piece()` in `wire.py`, then `tests/test_lifecycle.py`.

**Try yourself:** Run the missing-block endgame test. Check its cancel message and
why the helper requests one block rather than the whole piece.

## [#16 — Peer exchange](https://github.com/frogscooper/cbtorrent/pull/16)

**What changed:** Public downloads can learn peers from existing connections
through BEP 11 peer exchange. This supplements explicit peers, trackers, and DHT;
it does not replace piece verification or the scheduler's limits.

**How it works:** Extension negotiation assigns separate metadata and PEX message
numbers. A PEX message contains compact IP addresses and ports. The parser checks
sizes, counts, flags, duplicates, and message rate. The session admits only a
limited number of candidates per source and suggested IP. New hints wake the
scheduler even while another peer is waiting. Outgoing updates describe successful
outbound connections and include drops, at most once per minute.

**Correctness:** Private torrents never enable PEX, and magnet metadata lookup
waits until the private flag is known. A public source cannot redirect us into
local networks. Hints cannot mark pieces verified or remove tracker peers.

**Read first:** `pex.py`, `Peer.receive()` in `wire.py`, `make_pex()` in `client.py`,
and `tests/test_pex.py`.

**Try yourself:** Run the PEX-only discovery test. Follow how an empty bootstrap
peer supplies an address that leads to a complete, hash-verified download.
