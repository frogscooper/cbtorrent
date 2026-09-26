from __future__ import annotations

import os
from pathlib import Path
from .protocol import connect_peer
from .ranker import PeerRanker
from .torrent import Torrent, load_torrent
from .tracker import announce

PEER_ID_PREFIX = b"-ML0001-"


def make_peer_id() -> bytes:
    return (PEER_ID_PREFIX + os.urandom(12))[:20]


class Session:
    def __init__(self, torrent_path: str, out_dir: str = "./downloads", max_peers: int = 16):
        self.torrent: Torrent = load_torrent(torrent_path)
        self.out = Path(out_dir)
        self.out.mkdir(parents=True, exist_ok=True)
        self.max_peers = max_peers
        self.peer_id = make_peer_id()
        self.ranker = PeerRanker.load(Path.home() / ".mltorrent" / "ranker.json")
        self.connected: list[tuple[str, int]] = []

    def run(self):
        print(f"info_hash = {self.torrent.info_hash.hex()}")
        print(f"name      = {self.torrent.name}")
        print(f"size      = {self.torrent.length} bytes, {self.torrent.num_pieces} pieces")
        peers = announce(self.torrent, self.peer_id)
        print(f"tracker returned {len(peers)} peers")
        ranked = self.ranker.rank(peers)
        print("top-10 ranked peers:")
        for ip, port in ranked[:10]:
            print(f"  {self.ranker.score(ip, port):+.3f}  {ip}:{port}")
        for ip, port in ranked:
            if len(self.connected) >= self.max_peers:
                break
            try:
                sock, remote, rtt = connect_peer(ip, port, self.torrent.info_hash, self.peer_id)
                print(f"connected {ip}:{port} rtt={rtt:.0f}ms id={remote[:8]!r}")
                self.ranker.observe_success(ip, pieces_per_sec=0.1, rtt_ms=rtt, port=port)
                self.connected.append((ip, port))
                sock.close()
            except Exception as exc:
                print(f"fail {ip}:{port} ({exc})")
                self.ranker.observe_failure(ip, port)
        self.ranker.save(Path.home() / ".mltorrent" / "ranker.json")
        print(f"connected {len(self.connected)} / attempted, weights={self.ranker.weights}")
