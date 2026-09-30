# Working on cbtorrent

This is an experimental Python BitTorrent v1 implementation, with peer policy
experiments judged by download completion time and resource overhead. Preserve
protocol correctness and useful measurements before optimizing a policy.

## Setup and validation

- Python 3.11+; the runtime and tests use the standard library.
- Run `python -m unittest discover -s tests -v` from the repository root.
- Run `python -m cbtorrent benchmark --trials 3 --report benchmarks/my-run.json`
  for a paired local TCP comparison. Reports are exclusive writes: use a new name.
- Keep tests local and deterministic. Public torrents are not CI fixtures.
- Add protocol/regression tests for functional changes; test partial failure,
  cancellation, corrupt data, and resource bounds where relevant.

## Architecture and collaboration

- `wire.py`: peer framing, state, bounded reads, and pipelined requests.
- `client.py`: concurrent scheduling, connection cap, policy decisions, discovery,
  and session lifecycle. Only this module commits downloaded pieces.
- `storage.py`: hash-verified writes, resume, exclusive publication. Never write
  unverified data into the verified-piece set or trust a resume bitmap alone.
- `seeder.py`: upload listener and verified file source.
- `tracker.py`: HTTP(S)/UDP announces. All network operations need deadlines and
  input size limits; keep cancellation effective.
- `dht.py`: bounded IPv4 KRPC, discovery and announcements. Disable DHT for private
  torrents; use injected loopback bootstrap nodes in tests, never public routers.
- `magnet.py` / `extensions.py`: bounded BEP 9/10 metadata exchange. Verify the
  raw info hash and validate the manifest before handing metadata to `client.py`.
- `policy.py`: observations, heuristic, and optional bandit. Do not train with
  benchmark ground truth or future peer behavior. Keep the heuristic available.
- `metrics.py` / `benchmark.py`: measurement definitions and paired experiments.

Keep PRs focused, fetch before starting a branch, and preserve other agents'
work. Document changed measurement definitions in README. A benchmark win in a
small localhost scenario is not evidence of public-swarm improvement. Include
sample sizes and failures alongside timing and byte counts.

For every PR from now on, add a short explanation to `docs/PR_NOTES.md` and link
the PR when its number is known. Aim for 100–180 words: what changed, how the main
pieces work together, the important correctness rule or tradeoff, the first code
and test files to read, and one small hands-on exercise. Keep these notes useful
to the owner learning the code; do not replace them with a changelog or jargon.

The old `mltorrent` handshake/ranker prototype is preserved in Git history before
the `cbtorrent` implementation. Do not reintroduce the duplicate package. The
current package name and CLI are both `cbtorrent`.
