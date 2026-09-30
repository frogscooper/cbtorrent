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

## Magnet links

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
