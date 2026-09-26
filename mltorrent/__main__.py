from __future__ import annotations

import argparse
from .client import Session


def main():
    p = argparse.ArgumentParser(description="BitTorrent client with ML peer ranking")
    p.add_argument("torrent", help=".torrent file")
    p.add_argument("--out", default="./downloads")
    p.add_argument("--max-peers", type=int, default=16)
    args = p.parse_args()
    Session(args.torrent, args.out, args.max_peers).run()


if __name__ == "__main__":
    main()
