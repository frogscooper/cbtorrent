"""Master-detail multi-torrent UI bound to Session + DownloadController."""
from __future__ import annotations

from pathlib import Path

def run(*args, **kwargs):
    base = Path(__file__).resolve().parent
    ns = {"__name__": __name__, "__file__": str(Path(__file__)), "__package__": __package__}
    source = (base / "_queue_window_a.py").read_text(encoding="utf-8")
    source += (base / "_queue_window_b.py").read_text(encoding="utf-8")
    exec(compile(source, __file__, "exec"), ns)
    return ns["run"](*args, **kwargs)

__all__ = ["run"]
