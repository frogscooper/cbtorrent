# Changelog

Versions follow the `version` in `pyproject.toml`. Pull-request numbers link to
[docs/PR_NOTES.md](docs/PR_NOTES.md) where a short explanation exists. The project
is pre-1.0 and experimental: minor versions may change CLI flags and report formats.

## Unreleased

### Changed

- Connection attempts run ahead of transfer slots (up to 8 half-open), so
  unreachable addresses no longer stall downloads at startup. A peer's first
  observation now excludes its connection setup time; see README "Connection
  dial-ahead".

### Fixed

- `dht_errors` now samples failed queries, bootstrap DNS failures, and empty or
  expired lookups instead of staying empty (#20).
- Peer errors are never blank. Each names its phase and timeout limit, and the new
  `peer_failure_phases` report field counts all failures by phase (#20).
- Windows no longer prints a traceback when a peer resets a closing connection
  (WinError 10054), and dropped connections no longer trigger asyncio's
  `socket.send() raised exception.` warnings (#20).
- DHT lookups keep converging after the referral shortlist fills, accept replies
  up to 2048 bytes and with unsorted keys, stop dead nodes holding lookup slots, and
  recover from degenerate bootstrap replies with `find_node`. More default
  bootstrap routers are used, and an empty lookup is retried after one minute (#20).

## 0.3.0

Everything since the 0.2.0 baseline (the instrumented single-file client with the
heuristic and bandit policies). All results below are from local fixtures and a
local qBittorrent; public-swarm behaviour has not been measured.

### Added

- Opt-in peer policies `adaptive`, `optimistic`, `recovery`, and `timed`, each with a
  written experiment and held-out results in `benchmarks/`. `heuristic` stays the
  default (#1–#6).
- Desktop GUI with live progress snapshots, a multi-torrent queue, session
  persistence, and automatic advancement with start/stop/pause controls (#7, #9, #11).
- Complete GUI add, download-folder, retry, and open-folder workflows (#18).
- Bounded IPv4 DHT discovery and announcements (#10).
- Multi-file and nested-directory v1 torrents, including empty files, resume,
  seeding, and safe publication (#12).
- V1 magnet links with hash-verified BEP 9 metadata exchange over BEP 10 (#13), and a
  persistent verified-metadata cache for restart and offline resume (#17).
- Bounded BEP 11 peer exchange for public torrents (#16).
- Bounded peer recovery with backoff and block-level endgame (#15).
- Automated two-way interoperability tests against qBittorrent, run in CI (#14).

### Changed

- Reports distinguish metadata traffic from payload traffic, and magnet completion
  time includes metadata fetch.
- README rewritten to state limits plainly: no uTP, encryption, v2 torrents, NAT
  mapping, or classic tit-for-tat upload slots (#8).

### Known limits

See "Limits" in the README. Hashing and disk writes still run on the event loop, and
directory publication is not crash-atomic.
