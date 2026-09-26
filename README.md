# mltorrent

Python BitTorrent client with **online machine learning for peer connection**.

A small online linear model ranks tracker peers on features collected from previous swarms (prefix history, fail rate, RTT, bitfield overlap, port). Weights update with a margin perceptron whenever a handshake succeeds/fails or pieces arrive. Persisted in `~/.mltorrent/ranker.json`.

## Install

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e .
```

## Usage

```bash
python -m mltorrent path/to/file.torrent --out ./downloads --max-peers 20
```

## Layout

- `mltorrent/bencode.py` — BEP 3 encode/decode
- `mltorrent/torrent.py` — metainfo + info-hash
- `mltorrent/tracker.py` — HTTP compact announce
- `mltorrent/protocol.py` — peer handshake
- `mltorrent/ranker.py` — online linear scorer
- `mltorrent/client.py` — session loop

Not production: no magnet/DHT/v2, no full piece pipeline yet.

## License

MIT
