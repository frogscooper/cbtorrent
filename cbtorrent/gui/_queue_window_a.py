"""Master-detail multi-torrent UI bound to Session + DownloadController."""
from __future__ import annotations

from pathlib import Path

from ..metainfo import Torrent
from ..observe import format_eta, format_percent, format_rate
from ..session import Session, default_session_path
from .chrome import ACCENT, BG, MUTED, PANEL, POLICIES, TEXT
from .controller import DownloadController

def run(torrent: Path | None = None, output: Path | None = None, *, peers=(),
        resume=False, use_trackers=True, listen_host="0.0.0.0", listen_port=0,
        timeout=15.0, piece_timeout=120.0, pipeline=8, concurrency=4,
        max_connections=16, policy_name="heuristic",
        session_path: Path | None = None):
    _NO_DISPLAY = (
        "Desktop GUI needs a display and tkinter. Install the OS tk package "
        "(e.g. python3-tk) or use `cbtorrent download` instead."
    )
    try:
        import tkinter as tk
        from tkinter import filedialog, ttk
    except ImportError as error:
        raise SystemExit(_NO_DISPLAY) from error

    session = Session(
        Path(session_path) if session_path is not None else default_session_path(),
        default_policy=policy_name if policy_name in POLICIES else "heuristic",
    )
    restored = session.load()
    controller = DownloadController()
    metas: dict[str, Torrent] = {}
    selected_id: list[str | None] = [None]
    download_opts = dict(
        peers=list(peers), resume=resume, use_trackers=use_trackers,
        listen_host=listen_host, listen_port=listen_port, timeout=timeout,
        piece_timeout=piece_timeout, pipeline=pipeline, concurrency=concurrency,
        max_connections=max_connections,
    )

    def load_meta(item) -> Torrent | None:
        if item.id in metas:
            return metas[item.id]
        try:
            meta = Torrent.load(item.torrent_path)
        except (OSError, ValueError):
            return None
        metas[item.id] = meta
        return meta

    try:
        root = tk.Tk()
    except tk.TclError as error:
        raise SystemExit(_NO_DISPLAY) from error
    root.title("cbtorrent")
    root.geometry("900x560")
    root.minsize(900, 560)
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
    style.configure("Queue.Treeview", background=PANEL, foreground=TEXT,
                    fieldbackground=PANEL, borderwidth=0, rowheight=24)
    style.configure("Queue.Treeview.Heading", background=PANEL, foreground=MUTED,
                    relief="flat")
    style.map("Queue.Treeview", background=[("selected", ACCENT)],
              foreground=[("selected", TEXT)])

    outer = ttk.Frame(root, padding=16)
    outer.pack(fill="both", expand=True)

    # 1. Toolbar: Add | Remove | Pause | Resume | Policy
    toolbar = ttk.Frame(outer)
    toolbar.pack(fill="x", pady=(0, 8))

    def set_status(text):
        status_var.set(text)

    def selected_item():
        sid = selected_id[0]
        return session.get(sid) if sid else None

    def refresh_actions():
        item = selected_item()
        has = item is not None
        busy = controller.busy
        add_btn.state(["!disabled"])
        remove_btn.state(["!disabled"] if has else ["disabled"])
        pause_btn.state(["!disabled"] if has and item.status == "downloading" else ["disabled"])
        can_resume = has and item.status in ("queued", "paused", "error")
        if can_resume and busy and controller.active_id == item.id:
            can_resume = False
        resume_btn.state(["!disabled"] if can_resume else ["disabled"])
        policy_btn.state(["!disabled"])

    def paint_queue():
        snap = controller.snapshot
        existing = set(queue.get_children())
        seen = set()
        for index, item in enumerate(session.items(), start=1):
            seen.add(item.id)
            meta = load_meta(item)
            name = meta.name if meta is not None else item.torrent_path.name
            progress = ""
            down = ""
            up = ""
            if item.status == "complete":
                progress = "100%"
            elif item.id == controller.active_id and snap is not None:
                progress = format_percent(snap.done_bytes, snap.length)
                down = format_rate(snap.down_rate)
                up = format_rate(snap.up_rate)
            elif item.status == "downloading":
                progress = "..."
            values = (str(index), name, progress, down, up, item.status)
            if item.id in existing:
                queue.item(item.id, values=values)
            else:
                queue.insert("", "end", iid=item.id, values=values)
        for iid in existing - seen:
            queue.delete(iid)
        if selected_id[0] and selected_id[0] in seen:
            queue.selection_set(selected_id[0])
            queue.focus(selected_id[0])
        elif selected_id[0] and selected_id[0] not in seen:
            selected_id[0] = None

    def paint_detail():
        item = selected_item()
        snap = controller.snapshot
        if item is None:
            header_var.set("no torrents")
            root.title("cbtorrent")
            bar["value"] = 0
            percent.configure(text="0.0%")
            down_var.set("↓ 0 B/s")
            up_var.set("↑ 0 B/s")
            peers_var.set("Peers 0")
            eta_var.set("ETA unknown")
            for iid in tree.get_children():
                tree.delete(iid)
            return
        meta = load_meta(item)
        name = meta.name if meta is not None else item.torrent_path.name
        header_var.set(f"{name} ({len(meta.files)} files)" if meta and meta.multi_file else name)
        root.title(f"cbtorrent: {name}")
        if item.id == controller.active_id and snap is not None:
            bar["value"] = snap.percent
            percent.configure(text=format_percent(snap.done_bytes, snap.length))
            down_var.set(f"↓ {format_rate(snap.down_rate)}")
            up_var.set(f"↑ {format_rate(snap.up_rate)}")
            peers_var.set(f"Peers {snap.peer_count}")
            eta_var.set(f"ETA {format_eta(snap.eta_seconds)}")
            existing = set(tree.get_children())
            seen = set()
            for peer in snap.peers:
                seen.add(peer.address)
                values = (peer.address, format_rate(peer.down_rate), peer.state)
                if peer.address in existing:
                    tree.item(peer.address, values=values)
                else:
                    tree.insert("", "end", iid=peer.address, values=values)
            for iid in existing - seen:
                tree.delete(iid)
        else:
            bar["value"] = 100 if item.status == "complete" else 0
            percent.configure(text="100%" if item.status == "complete" else "0.0%")
            down_var.set("↓ 0 B/s")
            up_var.set("↑ 0 B/s")
            peers_var.set("Peers 0")
            eta_var.set("ETA unknown")
            for iid in tree.get_children():
                tree.delete(iid)

    def refresh_ui():
        paint_queue()
        paint_detail()
        refresh_actions()

    def on_select(_event=None):
        selection = queue.selection()
        selected_id[0] = selection[0] if selection else None
        refresh_ui()

    def start_item(item, *, force_resume=None):
        meta = load_meta(item)
        if meta is None:
            session.set_status(item.id, "error")
            set_status(f"Error: cannot load {item.torrent_path}")
            refresh_ui()
            return
        if controller.busy:
            if controller.active_id and controller.active_id != item.id:
                controller.cancel()
                controller.join(timeout=5)
                if controller.active_id and session.get(controller.active_id):
                    session.pause(controller.active_id)
            else:
                return
        output_path = Path(item.output)
        part = output_path.with_name(output_path.name + ".part")
        use_resume = force_resume if force_resume is not None else part.exists()
        if output_path.exists() and not use_resume:
            stem, suffix = output_path.stem, output_path.suffix
            n = 1
            while True:
                candidate = output_path.with_name(f"{stem}-{n}{suffix}")
                cand_part = candidate.with_name(candidate.name + ".part")
                if not candidate.exists() and not cand_part.exists():
                    output_path = candidate
                    item.output = output_path
                    session.save()
                    break
                n += 1
        output_path.parent.mkdir(parents=True, exist_ok=True)
        session.resume(item.id)
        set_status("Downloading...")
        refresh_ui()
        try:
            controller.start(
                meta, download_opts["peers"], output_path, resume=use_resume,
                use_trackers=download_opts["use_trackers"],
                listen_host=download_opts["listen_host"],
                listen_port=download_opts["listen_port"],
                timeout=download_opts["timeout"],
                piece_timeout=download_opts["piece_timeout"],
                pipeline=download_opts["pipeline"],
                concurrency=download_opts["concurrency"],
                max_connections=download_opts["max_connections"],
                policy=POLICIES.get(item.policy, POLICIES["heuristic"])(),
                item_id=item.id)
        except (OSError, ValueError, RuntimeError) as error:
            session.set_status(item.id, "error")
            set_status(f"Error: {error}")
            refresh_ui()
            return
        root.after(100, tick)

    def add_torrent(path: Path | None = None, output_path: Path | None = None,
                    select=True):
        if path is None:
            chosen = filedialog.askopenfilename(
                title="Add torrent",
                filetypes=[("Torrent files", "*.torrent"), ("All files", "*.*")])
            if not chosen:
                return None
            path = Path(chosen)
        try:
            item = session.add(
                path, output=output_path, policy=session.default_policy)
        except (OSError, ValueError) as error:
            set_status(f"Error: {error}")
            return None
        load_meta(item)
        if select:
            selected_id[0] = item.id
        refresh_ui()
        if not session.items():
            set_status("add a torrent to start")
        return item

    def remove_torrent():
        item = selected_item()
        if item is None:
            return
        if item.status == "downloading" or (
                controller.busy and controller.active_id == item.id):
            controller.cancel()
            controller.join(timeout=5)
            session.pause(item.id)
        session.remove(item.id)
        metas.pop(item.id, None)
        selected_id[0] = None
        set_status("Removed")
        refresh_ui()

    def pause_selected(_event=None):
        item = selected_item()
        if item is None:
            return "break" if _event else None
        if item.status != "downloading" and not (
                controller.busy and controller.active_id == item.id):
            return "break" if _event else None
        if controller.busy and controller.active_id == item.id:
            controller.cancel()
            controller.join(timeout=5)
        session.pause(item.id)
        set_status("Paused: partial file kept as .part")
        refresh_ui()
        return "break" if _event else None

    def resume_selected():
        item = selected_item()
        if item is None:
            return
        if item.status == "complete":
            set_status("Already complete")
            return
        start_item(item)

    def choose_policy(name):
        session.set_default_policy(name)
        policy_var.set(f"Policy: {name}")
        item = selected_item()
        if item is not None:
            session.set_policy(item.id, name)
        refresh_ui()

    add_btn = ttk.Button(toolbar, text="Add", command=lambda: add_torrent())
    add_btn.pack(side="left")
    remove_btn = ttk.Button(toolbar, text="Remove", command=remove_torrent)
    remove_btn.pack(side="left", padx=(8, 0))
    pause_btn = ttk.Button(toolbar, text="Pause", command=pause_selected)
    pause_btn.pack(side="left", padx=(8, 0))
    resume_btn = ttk.Button(toolbar, text="Resume", command=resume_selected)
    resume_btn.pack(side="left", padx=(8, 0))

    policy_var = tk.StringVar(value=f"Policy: {session.default_policy}")
    policy_btn = ttk.Menubutton(toolbar, textvariable=policy_var)
    policy_menu = tk.Menu(policy_btn, tearoff=0, bg=PANEL, fg=TEXT,
                          activebackground=ACCENT, activeforeground=TEXT,
                          bd=0)
    for name in POLICIES:
        policy_menu.add_command(label=name, command=lambda n=name: choose_policy(n))
    policy_btn["menu"] = policy_menu
    policy_btn.pack(side="left", padx=(8, 0))

