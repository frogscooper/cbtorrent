"""Minimal tkinter download window. Protocol stays in client/; this only paints snapshots."""
from __future__ import annotations

from pathlib import Path

from ..metainfo import Torrent
from ..observe import format_eta, format_percent, format_rate
from .chrome import ACCENT, BG, MUTED, PANEL, POLICIES, TEXT
from .controller import DownloadController


def run(torrent: Path | None = None, output: Path | None = None, *, peers=(),
        resume=False, use_trackers=True, listen_host="0.0.0.0", listen_port=0,
        timeout=15.0, piece_timeout=120.0, pipeline=8, concurrency=4,
        max_connections=16, policy_name="heuristic"):
    _NO_DISPLAY = (
        "Desktop GUI needs a display and tkinter. Install the OS tk package "
        "(e.g. python3-tk) or use `cbtorrent download` instead."
    )
    try:
        import tkinter as tk
        from tkinter import filedialog, ttk
    except ImportError as error:
        raise SystemExit(_NO_DISPLAY) from error

    controller = DownloadController()
    state = {
        "torrent_path": Path(torrent) if torrent else None,
        "output": Path(output) if output else None,
        "meta": None,
        "peers": list(peers),
        "policy": policy_name if policy_name in POLICIES else "heuristic",
    }
    if state["torrent_path"] is not None:
        state["meta"] = Torrent.load(state["torrent_path"])
        if state["output"] is None:
            state["output"] = Path("downloads") / state["meta"].name

    try:
        root = tk.Tk()
    except tk.TclError as error:
        raise SystemExit(_NO_DISPLAY) from error
    root.title("cbtorrent")
    root.geometry("720x480")
    root.minsize(720, 480)
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
    style.configure("Status.TLabel", background=PANEL, foreground=MUTED)
    style.configure("TButton", background=PANEL, foreground=TEXT, padding=(10, 6))
    style.map("TButton",
              background=[("active", ACCENT), ("disabled", PANEL)],
              foreground=[("disabled", MUTED)])
    style.configure("TMenubutton", background=PANEL, foreground=TEXT, padding=(10, 6))
    style.configure("Accent.Horizontal.TProgressbar", troughcolor=PANEL,
                    background=ACCENT, thickness=22)
    style.configure("Peer.Treeview", background=PANEL, foreground=TEXT,
                    fieldbackground=PANEL, borderwidth=0, rowheight=22)
    style.configure("Peer.Treeview.Heading", background=PANEL, foreground=MUTED,
                    relief="flat")
    style.map("Peer.Treeview", background=[("selected", ACCENT)],
              foreground=[("selected", TEXT)])

    outer = ttk.Frame(root, padding=16)
    outer.pack(fill="both", expand=True)

    # 1. Toolbar
    toolbar = ttk.Frame(outer)
    toolbar.pack(fill="x", pady=(0, 8))

    def set_status(text):
        status_var.set(text)

    def refresh_actions():
        has_torrent = state["meta"] is not None
        busy = controller.busy
        add_btn.state(["!disabled"] if not busy else ["disabled"])
        policy_btn.state(["!disabled"] if not busy else ["disabled"])
        start_btn.state(["!disabled"] if has_torrent and not busy else ["disabled"])
        stop_btn.state(["!disabled"] if busy else ["disabled"])

    def add_torrent():
        path = filedialog.askopenfilename(
            title="Add torrent",
            filetypes=[("Torrent files", "*.torrent"), ("All files", "*.*")])
        if not path:
            return
        try:
            meta = Torrent.load(Path(path))
        except (OSError, ValueError) as error:
            set_status(f"Error: {error}")
            return
        state["torrent_path"] = Path(path)
        state["meta"] = meta
        state["output"] = Path("downloads") / meta.name
        header_var.set(meta.name)
        root.title(f"cbtorrent — {meta.name}")
        bar["value"] = 0
        percent.configure(text="0.0%")
        set_status("Idle — ready to start")
        refresh_actions()

    def choose_policy(name):
        state["policy"] = name
        policy_var.set(f"Policy: {name}")

    def start_download():
        meta = state["meta"]
        if meta is None or controller.busy:
            return
        output = state["output"] or (Path("downloads") / meta.name)
        output = Path(output)
        # Avoid colliding with a finished file from a prior run in this session.
        if output.exists():
            stem, suffix = output.stem, output.suffix
            n = 1
            while True:
                candidate = output.with_name(f"{stem}-{n}{suffix}")
                if not candidate.exists() and not candidate.with_name(candidate.name + ".part").exists():
                    output = candidate
                    break
                n += 1
            state["output"] = output
        output.parent.mkdir(parents=True, exist_ok=True)
        set_status("Downloading…")
        refresh_actions()
        try:
            controller.start(
                meta, state["peers"], output, resume=resume,
                use_trackers=use_trackers, listen_host=listen_host,
                listen_port=listen_port, timeout=timeout,
                piece_timeout=piece_timeout, pipeline=pipeline,
                concurrency=concurrency, max_connections=max_connections,
                policy=POLICIES[state["policy"]]())
        except (OSError, ValueError, RuntimeError) as error:
            set_status(f"Error: {error}")
            refresh_actions()
            return
        root.after(100, tick)

    def stop_download(_event=None):
        if not controller.busy:
            return "break" if _event else None
        controller.cancel()
        set_status("Stopping…")
        stop_btn.state(["disabled"])
        return "break" if _event else None

    add_btn = ttk.Button(toolbar, text="Add torrent…", command=add_torrent)
    add_btn.pack(side="left")

    policy_var = tk.StringVar(value=f"Policy: {state['policy']}")
    policy_btn = ttk.Menubutton(toolbar, textvariable=policy_var)
    policy_menu = tk.Menu(policy_btn, tearoff=0, bg=PANEL, fg=TEXT,
                          activebackground=ACCENT, activeforeground=TEXT,
                          bd=0)
    for name in POLICIES:
        policy_menu.add_command(label=name, command=lambda n=name: choose_policy(n))
    policy_btn["menu"] = policy_menu
    policy_btn.pack(side="left", padx=(8, 0))

    start_btn = ttk.Button(toolbar, text="Start", command=start_download)
    start_btn.pack(side="left", padx=(8, 0))
    stop_btn = ttk.Button(toolbar, text="Stop", command=stop_download)
    stop_btn.pack(side="left", padx=(8, 0))

    # 2. Header
    header_var = tk.StringVar(
        value=state["meta"].name if state["meta"] is not None else "no torrent")
    ttk.Label(outer, textvariable=header_var, style="Accent.TLabel",
              font=("Segoe UI", 14, "bold")).pack(anchor="w", pady=(8, 0))

    # 3. Progress
    progress_row = ttk.Frame(outer)
    progress_row.pack(fill="x", pady=(8, 0))
    bar = ttk.Progressbar(progress_row, style="Accent.Horizontal.TProgressbar",
                          maximum=100, mode="determinate")
    bar.pack(side="left", fill="x", expand=True)
    percent = ttk.Label(progress_row, text="0.0%", width=7, style="TLabel")
    percent.pack(side="left", padx=(8, 0))

    # 4. Stats
    stats = ttk.Frame(outer, style="Panel.TFrame", padding=10)
    stats.pack(fill="x", pady=(8, 0))
    down_var = tk.StringVar(value="↓ 0 B/s")
    up_var = tk.StringVar(value="↑ 0 B/s")
    peers_var = tk.StringVar(value="Peers 0")
    eta_var = tk.StringVar(value="ETA —")
    for i, var in enumerate((down_var, up_var, peers_var, eta_var)):
        ttk.Label(stats, textvariable=var, style="Panel.TLabel").grid(
            row=0, column=i, sticky="w", padx=(0 if i == 0 else 16, 0))

    # 5. Peer list
    peer_frame = ttk.Frame(outer)
    peer_frame.pack(fill="both", expand=True, pady=(8, 0))
    columns = ("addr", "rate", "state")
    tree = ttk.Treeview(peer_frame, columns=columns, show="headings",
                        style="Peer.Treeview", selectmode="browse")
    tree.heading("addr", text="Peer")
    tree.heading("rate", text="↓ Rate")
    tree.heading("state", text="State")
    tree.column("addr", width=360, anchor="w")
    tree.column("rate", width=120, anchor="e")
    tree.column("state", width=120, anchor="w")
    scroll = ttk.Scrollbar(peer_frame, orient="vertical", command=tree.yview)
    tree.configure(yscrollcommand=scroll.set)
    tree.pack(side="left", fill="both", expand=True)
    scroll.pack(side="right", fill="y")

    # 6. Status strip
    status_frame = ttk.Frame(outer, style="Panel.TFrame", padding=(10, 8))
    status_frame.pack(fill="x", pady=(8, 0))
    status_var = tk.StringVar(
        value="Idle — ready to start" if state["meta"] is not None else "add a torrent to start")
    ttk.Label(status_frame, textvariable=status_var, style="Status.TLabel").pack(anchor="w")

    root.bind("\u003cEscape\u003e", stop_download)

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
            set_status("Complete")
        elif snapshot.status == "cancelled":
            set_status("Stopped — partial file kept as .part")
        elif snapshot.status == "error":
            set_status(f"Error: {snapshot.error or 'download failed'}")
        elif snapshot.status == "starting":
            set_status("Starting…")
        elif snapshot.status == "running":
            set_status(f"Downloading {format_percent(snapshot.done_bytes, snapshot.length)}")
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
            refresh_actions()
            snap = controller.snapshot
            if snap is None or snap.status == "running":
                set_status(controller._error or "Idle")

    def on_close():
        controller.cancel()
        controller.join(timeout=5)
        root.destroy()

    root.protocol("WM_DELETE_WINDOW", on_close)
    refresh_actions()
    if state["meta"] is not None:
        root.title(f"cbtorrent — {state['meta'].name}")
    root.mainloop()
    controller.join(timeout=5)
    snap = controller.snapshot
    if snap is not None and snap.status == "error":
        return 1
    if snap is not None and snap.status == "cancelled":
        return 130
    return 0
