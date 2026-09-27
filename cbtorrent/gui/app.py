"""Minimal tkinter download window. Protocol stays in client/; this only paints snapshots."""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from ..metainfo import Torrent
from ..observe import format_eta, format_percent, format_rate
from .controller import DownloadController

BG = "#1e1e1e"
PANEL = "#2a2a2a"
TEXT = "#e8e8e8"
ACCENT = "#4a9eff"
MUTED = "#888888"


def open_folder(path: Path):
    path = Path(path)
    folder = path if path.is_dir() else path.parent
    folder.mkdir(parents=True, exist_ok=True)
    if sys.platform == "win32":
        os.startfile(folder)  # type: ignore[attr-defined]
    elif sys.platform == "darwin":
        subprocess.Popen(["open", str(folder)], start_new_session=True)
    else:
        subprocess.Popen(["xdg-open", str(folder)], start_new_session=True)


def run(torrent: Path, output: Path, *, peers=(), resume=False, use_trackers=True,
        listen_host="0.0.0.0", listen_port=0, timeout=15.0, piece_timeout=120.0,
        pipeline=8, concurrency=4, max_connections=16):
    try:
        import tkinter as tk
        from tkinter import ttk
    except ImportError as error:
        raise SystemExit(
            "tkinter is not available in this Python. Install the OS tk package "
            "(e.g. python3-tk) or use `cbtorrent download` instead."
        ) from error

    meta = Torrent.load(torrent)
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    controller = DownloadController()

    root = tk.Tk()
    root.title(f"cbtorrent — {meta.name}")
    root.geometry("720x420")
    root.minsize(560, 320)
    root.configure(bg=BG)

    style = ttk.Style(root)
    try:
        style.theme_use("clam")
    except tk.TclError:
        pass
    style.configure("TFrame", background=BG)
    style.configure("Panel.TFrame", background=PANEL)
    style.configure("TLabel", background=BG, foreground=TEXT)
    style.configure("Muted.TLabel", background=BG, foreground=MUTED)
    style.configure("Panel.TLabel", background=PANEL, foreground=TEXT)
    style.configure("MutedPanel.TLabel", background=PANEL, foreground=MUTED)
    style.configure("Accent.TLabel", background=BG, foreground=ACCENT)
    style.configure("TButton", background=PANEL, foreground=TEXT, padding=6)
    style.map("TButton", background=[("active", ACCENT)])
    style.configure("Accent.Horizontal.TProgressbar", troughcolor=PANEL,
                    background=ACCENT, thickness=18)
    style.configure("Peer.Treeview", background=PANEL, foreground=TEXT,
                    fieldbackground=PANEL, borderwidth=0, rowheight=22)
    style.configure("Peer.Treeview.Heading", background=PANEL, foreground=MUTED,
                    relief="flat")
    style.map("Peer.Treeview", background=[("selected", ACCENT)],
              foreground=[("selected", TEXT)])

    outer = ttk.Frame(root, padding=16)
    outer.pack(fill="both", expand=True)

    header = ttk.Label(outer, text=meta.name, style="Accent.TLabel",
                       font=("Segoe UI", 14, "bold"))
    header.pack(anchor="w")

    progress_row = ttk.Frame(outer)
    progress_row.pack(fill="x", pady=(12, 4))
    bar = ttk.Progressbar(progress_row, style="Accent.Horizontal.TProgressbar",
                          maximum=100, mode="determinate")
    bar.pack(side="left", fill="x", expand=True)
    percent = ttk.Label(progress_row, text="0.0%", width=7, style="TLabel")
    percent.pack(side="left", padx=(8, 0))

    stats = ttk.Frame(outer, style="Panel.TFrame", padding=10)
    stats.pack(fill="x", pady=(8, 8))
    down_var = tk.StringVar(value="↓ 0 B/s")
    up_var = tk.StringVar(value="↑ 0 B/s")
    peers_var = tk.StringVar(value="Peers 0")
    eta_var = tk.StringVar(value="ETA —")
    status_var = tk.StringVar(value="Starting…")
    for i, var in enumerate((down_var, up_var, peers_var, eta_var)):
        ttk.Label(stats, textvariable=var, style="Panel.TLabel").grid(
            row=0, column=i, sticky="w", padx=(0 if i == 0 else 16, 0))
    ttk.Label(stats, textvariable=status_var, style="MutedPanel.TLabel").grid(
        row=1, column=0, columnspan=4, sticky="w", pady=(6, 0))

    peer_frame = ttk.Frame(outer)
    peer_frame.pack(fill="both", expand=True, pady=(4, 8))
    columns = ("addr", "rate", "state")
    tree = ttk.Treeview(peer_frame, columns=columns, show="headings",
                        style="Peer.Treeview", selectmode="browse")
    tree.heading("addr", text="Peer")
    tree.heading("rate", text="Rate")
    tree.heading("state", text="State")
    tree.column("addr", width=360, anchor="w")
    tree.column("rate", width=120, anchor="e")
    tree.column("state", width=120, anchor="w")
    scroll = ttk.Scrollbar(peer_frame, orient="vertical", command=tree.yview)
    tree.configure(yscrollcommand=scroll.set)
    tree.pack(side="left", fill="both", expand=True)
    scroll.pack(side="right", fill="y")

    footer = ttk.Frame(outer)
    footer.pack(fill="x")

    def on_cancel(_event=None):
        controller.cancel()
        status_var.set("Cancelling…")
        cancel_btn.state(["disabled"])

    def on_open(_event=None):
        target = controller.output or output
        try:
            open_folder(target)
        except OSError as error:
            status_var.set(f"Could not open folder: {error}")

    cancel_btn = ttk.Button(footer, text="Cancel", command=on_cancel)
    cancel_btn.pack(side="left")
    open_btn = ttk.Button(footer, text="Open folder", command=on_open)
    open_btn.pack(side="left", padx=(8, 0))
    ttk.Label(footer, text="Esc cancels", style="Muted.TLabel").pack(side="right")

    root.bind("\u003cEscape\u003e", on_cancel)

    def paint(snapshot):
        if snapshot is None:
            return
        bar["value"] = snapshot.percent
        percent.configure(text=format_percent(snapshot.done_bytes, snapshot.length))
        down_var.set(f"↓ {format_rate(snapshot.down_rate)}")
        up_var.set(f"↑ {format_rate(snapshot.up_rate)}")
        peers_var.set(f"Peers {snapshot.peer_count}")
        eta_var.set(f"ETA {format_eta(snapshot.eta_seconds)}")
        if snapshot.status == "complete":
            status_var.set("Complete")
            cancel_btn.state(["disabled"])
        elif snapshot.status == "cancelled":
            status_var.set("Cancelled — partial file kept as .part")
            cancel_btn.state(["disabled"])
        elif snapshot.status == "error":
            status_var.set(snapshot.error or "Error")
            cancel_btn.state(["disabled"])
        elif snapshot.status == "starting":
            status_var.set("Starting…")
        else:
            done = format_percent(snapshot.done_bytes, snapshot.length)
            status_var.set(f"Downloading {done}")
        existing = set(tree.get_children())
        seen = set()
        for peer in snapshot.peers:
            seen.add(peer.address)
            values = (peer.address, format_rate(peer.down_rate), peer.state)
            if peer.address in existing:
                tree.item(peer.address, values=values)
            else:
                tree.insert("", "end", iid=peer.address, values=values)
        for iid in existing - seen:
            tree.delete(iid)

    def tick():
        paint(controller.snapshot)
        if controller.busy:
            root.after(250, tick)
        else:
            paint(controller.snapshot)
            if controller.snapshot and controller.snapshot.status == "running":
                # Thread ended without a terminal status.
                status_var.set(controller._error or "Finished")
                cancel_btn.state(["disabled"])

    def on_close():
        controller.cancel()
        controller.join(timeout=5)
        root.destroy()

    root.protocol("WM_DELETE_WINDOW", on_close)
    controller.start(
        meta, peers, output, resume=resume, use_trackers=use_trackers,
        listen_host=listen_host, listen_port=listen_port, timeout=timeout,
        piece_timeout=piece_timeout, pipeline=pipeline, concurrency=concurrency,
        max_connections=max_connections)
    root.after(100, tick)
    root.mainloop()
    controller.join(timeout=5)
    snap = controller.snapshot
    if snap is not None and snap.status == "error":
        return 1
    if snap is not None and snap.status == "cancelled":
        return 130
    return 0
