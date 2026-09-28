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
- `dht.py`: IPv4 DHT (BEP 5) get_peers / announce_peer. Discovery stays in the client;
  keep bootstrap injectable and tests on local UDP only (no public swarm as CI fixture).
- `policy.py`: observations, heuristic, and optional bandit. Do not train with
  benchmark ground truth or future peer behavior. Keep the heuristic available.
- `metrics.py` / `benchmark.py`: measurement definitions and paired experiments.

Keep PRs focused, fetch before starting a branch, and preserve other agents'
work. Document changed measurement definitions in README. A benchmark win in a
small localhost scenario is not evidence of public-swarm improvement. Include
sample sizes and failures alongside timing and byte counts.

The old `mltorrent` handshake/ranker prototype is preserved in Git history before
the `cbtorrent` implementation. Do not reintroduce the duplicate package. The
current package name and CLI are both `cbtorrent`.
