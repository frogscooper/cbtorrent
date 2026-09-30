"""Master-detail multi-torrent UI bound to Session + DownloadController."""
from __future__ import annotations

from pathlib import Path
from dataclasses import dataclass, field

from .dialogs import AddDialog
from .folders import open_folder

from ..metainfo import Torrent
from ..magnet import load_source
from ..observe import format_eta, format_percent, format_rate
from ..session import Session, default_session_path
from .chrome import ACCENT, BG, MUTED, PANEL, POLICIES, TEXT
from .controller import DownloadController
from .scheduler import QueueScheduler

@dataclass
class QueueWindow:
    root: object
    session: Session
    controller: DownloadController
    buttons: dict = field(default_factory=dict)
    add_dialog: object = None
    closed: bool = False


def create_window(torrent: Path | None = None, output: Path | None = None, *, peers=(),
        resume=False, use_trackers=True, listen_host="0.0.0.0", listen_port=0,
        timeout=15.0, piece_timeout=120.0, pipeline=8, concurrency=4,
        max_connections=16, policy_name="heuristic",
        session_path: Path | None = None, use_dht=True, dht_bootstrap=None, metadata_timeout=60.0, root=None):
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
        download_folder=Path.home() / "Downloads" / "cbtorrent",
    )
    restored = session.load()
    from ..metadata_cache import MetadataCache
    metadata_cache = MetadataCache(session.path.parent / "metadata")
    controller = DownloadController()
    metas: dict[str, Torrent] = {}
    selected_id: list[str | None] = [None]
    download_opts = dict(
        peers=list(peers), resume=resume, use_trackers=use_trackers,
        use_dht=use_dht, dht_bootstrap=dht_bootstrap,
        listen_host=listen_host, listen_port=listen_port, timeout=timeout,
        piece_timeout=piece_timeout, pipeline=pipeline, concurrency=concurrency,
        max_connections=max_connections,
    )

    def load_meta(item) -> Torrent | None:
        resolved = getattr(controller, "resolved_torrent", None)
        if isinstance(resolved, Torrent) and item.id == controller.active_id:
            metas[item.id] = resolved
        if item.id in metas:
            return metas[item.id]
        try:
            meta = load_source(item.magnet_uri or item.torrent_path)
        except (OSError, ValueError):
            return None
        metas[item.id] = meta
        return meta

    try:
        root = root if root is not None else tk.Tk()
    except tk.TclError as error:
        raise SystemExit(_NO_DISPLAY) from error
    window = QueueWindow(root, session, controller)
    root.title("cbtorrent")
    root.geometry("900x640")
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

    # Common actions stay visible; the engine remains in the controller.
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
        can_resume = has and item.status in ("queued", "paused")
        if can_resume and busy:
            can_resume = False
        resume_btn.state(["!disabled"] if can_resume else ["disabled"])
        retry_btn.state(["!disabled"] if has and item.status == "error" and not busy else ["disabled"])
        open_btn.state(["!disabled"] if has else ["disabled"])
        policy_btn.state(["!disabled"])
        start_queue_btn.state(["disabled"] if session.queue_running else ["!disabled"])
        stop_queue_btn.state(["!disabled"] if session.queue_running or busy else ["disabled"])
        move_up_btn.state(["!disabled"] if has else ["disabled"])
        move_down_btn.state(["!disabled"] if has else ["disabled"])

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
            queue.move(item.id, "", index - 1)
        for iid in existing - seen:
            queue.delete(iid)
        if selected_id[0] and selected_id[0] in seen:
            if queue.selection() != (selected_id[0],):
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
        item = selected_item()
        if item is not None and item.status == "error":
            set_status(f"Failed: {item.error or 'Download failed.'} Use Retry to resume this destination.")
        elif item is not None and not controller.busy:
            set_status("Complete. Use Open Folder to view the download." if item.status == "complete"
                       else f"{item.status.capitalize()}. Use Resume to begin.")

    def start_item(item):
        meta = load_meta(item)
        if meta is None:
            item.error = f"Cannot load {item.torrent_path}. Restore the torrent file and use Retry."
            set_status(item.error)
            return False
        output_path = Path(item.output)
        part = output_path.with_name(output_path.name + ".part")
        use_resume = part.exists()
        try:
            if output_path.exists():
                raise FileExistsError("The destination already exists; it will not be overwritten. Use Open Folder.")
            output_path.parent.mkdir(parents=True, exist_ok=True)
            controller.start(
                meta, download_opts["peers"], output_path, resume=use_resume,
                use_trackers=download_opts["use_trackers"],
                use_dht=download_opts["use_dht"], dht_bootstrap=download_opts["dht_bootstrap"],
                listen_host=download_opts["listen_host"],
                listen_port=download_opts["listen_port"],
                timeout=download_opts["timeout"],
                piece_timeout=download_opts["piece_timeout"],
                pipeline=download_opts["pipeline"],
                concurrency=download_opts["concurrency"],
                max_connections=download_opts["max_connections"],
                policy=POLICIES.get(item.policy, POLICIES["heuristic"])(),
                item_id=item.id, metadata_timeout=metadata_timeout, metadata_cache=metadata_cache)
        except (OSError, ValueError, RuntimeError) as error:
            item.error = str(error)
            set_status(f"Failed: {error}")
            return False
        set_status("Downloading...")
        return True

    scheduler = QueueScheduler(session, controller, start_item)

    def show_added(item):
        load_meta(item)
        selected_id[0] = item.id
        refresh_ui()
        set_status("Added to queue. Use Resume or Start Queue to begin.")

    def add_torrent(path, output_path=None):
        try:
            item = session.add(path, output=output_path, policy=session.default_policy)
        except (OSError, ValueError) as error:
            set_status(f"Error: {error}")
            return None
        show_added(item)
        return item

    def show_add_dialog():
        if window.add_dialog is not None and window.add_dialog.window.winfo_exists():
            window.add_dialog.window.lift()
            return
        window.add_dialog = AddDialog(root, session, show_added)

    def remove_torrent():
        item = selected_item()
        if item is None:
            return
        if item.status == "downloading" or (
                controller.busy and controller.active_id == item.id):
            scheduler.stop(paused=True)
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
        scheduler.stop(paused=True)
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
        scheduler.resume(item)
        refresh_ui()

    def retry_selected():
        item = selected_item()
        if item is not None and item.status == "error" and not controller.busy:
            scheduler.resume(item)
            refresh_ui()

    def open_selected_folder():
        item = selected_item()
        if item is None:
            return
        folder = item.output if item.output.is_dir() else item.output.parent
        try:
            open_folder(folder)
            set_status(f"Opened {folder}")
        except (OSError, ValueError) as error:
            set_status(str(error))

    def start_queue():
        scheduler.start()
        refresh_ui()

    def stop_queue():
        scheduler.stop()
        set_status("Queue stopped: partial file kept")
        refresh_ui()

    def move_selected(direction):
        item = selected_item()
        if item is None:
            return
        ordered = session.items()
        index = ordered.index(item)
        target = index + direction
        if 0 <= target < len(ordered):
            if direction < 0:
                before = ordered[target].id
            else:
                before = ordered[target + 1].id if target + 1 < len(ordered) else None
            session.reorder(item.id, before)
            refresh_ui()

    def choose_policy(name):
        session.set_default_policy(name)
        policy_var.set(f"Policy: {name}")
        item = selected_item()
        if item is not None:
            session.set_policy(item.id, name)
        refresh_ui()

    add_btn = ttk.Button(toolbar, text="Add Torrent", command=show_add_dialog)
    add_btn.pack(side="left")
    remove_btn = ttk.Button(toolbar, text="Remove", command=remove_torrent)
    remove_btn.pack(side="left", padx=(8, 0))
    pause_btn = ttk.Button(toolbar, text="Pause", command=pause_selected)
    pause_btn.pack(side="left", padx=(8, 0))
    resume_btn = ttk.Button(toolbar, text="Resume", command=resume_selected)
    resume_btn.pack(side="left", padx=(8, 0))

    retry_btn = ttk.Button(toolbar, text="Retry", command=retry_selected)
    retry_btn.pack(side="left", padx=(8, 0))
    open_btn = ttk.Button(toolbar, text="Open Folder", command=open_selected_folder)
    open_btn.pack(side="left", padx=(8, 0))

    policy_var = tk.StringVar(value=f"Policy: {session.default_policy}")
    policy_btn = ttk.Menubutton(toolbar, textvariable=policy_var)
    policy_menu = tk.Menu(policy_btn, tearoff=0, bg=PANEL, fg=TEXT,
                          activebackground=ACCENT, activeforeground=TEXT,
                          bd=0)
    for name in POLICIES:
        policy_menu.add_command(label=name, command=lambda n=name: choose_policy(n))
    policy_btn["menu"] = policy_menu
    policy_btn.pack(side="left", padx=(8, 0))

    queue_controls = ttk.Frame(outer)
    queue_controls.pack(fill="x")
    start_queue_btn = ttk.Button(queue_controls, text="Start Queue", command=start_queue)
    start_queue_btn.pack(side="left")
    stop_queue_btn = ttk.Button(queue_controls, text="Stop Queue", command=stop_queue)
    stop_queue_btn.pack(side="left", padx=(8, 0))
    move_up_btn = ttk.Button(queue_controls, text="Move Up", command=lambda: move_selected(-1))
    move_up_btn.pack(side="left", padx=(8, 0))
    move_down_btn = ttk.Button(queue_controls, text="Move Down", command=lambda: move_selected(1))
    move_down_btn.pack(side="left", padx=(8, 0))

    # 2. Queue Treeview
    queue_frame = ttk.Frame(outer)
    queue_frame.pack(fill="both", expand=True, pady=(8, 0))
    qcols = ("num", "name", "progress", "down", "up", "status")
    queue = ttk.Treeview(queue_frame, columns=qcols, show="headings",
                         style="Queue.Treeview", selectmode="browse", height=6)
    queue.heading("num", text="#")
    queue.heading("name", text="Name")
    queue.heading("progress", text="Progress")
    queue.heading("down", text="↓")
    queue.heading("up", text="↑")
    queue.heading("status", text="Status")
    queue.column("num", width=40, anchor="e")
    queue.column("name", width=320, anchor="w")
    queue.column("progress", width=80, anchor="e")
    queue.column("down", width=90, anchor="e")
    queue.column("up", width=90, anchor="e")
    queue.column("status", width=110, anchor="w")
    qscroll = ttk.Scrollbar(queue_frame, orient="vertical", command=queue.yview)
    queue.configure(yscrollcommand=qscroll.set)
    queue.pack(side="left", fill="both", expand=True)
    qscroll.pack(side="right", fill="y")
    queue.bind("<<TreeviewSelect>>", on_select)

    # 3. Detail (selected)
    header_var = tk.StringVar(value="no torrents")
    ttk.Label(outer, textvariable=header_var, style="Accent.TLabel",
              font=("Segoe UI", 14, "bold")).pack(anchor="w", pady=(8, 0))

    progress_row = ttk.Frame(outer)
    progress_row.pack(fill="x", pady=(8, 0))
    bar = ttk.Progressbar(progress_row, style="Accent.Horizontal.TProgressbar",
                          maximum=100, mode="determinate")
    bar.pack(side="left", fill="x", expand=True)
    percent = ttk.Label(progress_row, text="0.0%", width=7, style="TLabel")
    percent.pack(side="left", padx=(8, 0))

    stats = ttk.Frame(outer, style="Panel.TFrame", padding=10)
    stats.pack(fill="x", pady=(8, 0))
    down_var = tk.StringVar(value="↓ 0 B/s")
    up_var = tk.StringVar(value="↑ 0 B/s")
    peers_var = tk.StringVar(value="Peers 0")
    eta_var = tk.StringVar(value="ETA unknown")
    for i, var in enumerate((down_var, up_var, peers_var, eta_var)):
        ttk.Label(stats, textvariable=var, style="Panel.TLabel").grid(
            row=0, column=i, sticky="w", padx=(0 if i == 0 else 16, 0))

    peer_frame = ttk.Frame(outer)
    peer_frame.pack(fill="both", expand=True, pady=(8, 0))
    columns = ("addr", "rate", "state")
    tree = ttk.Treeview(peer_frame, columns=columns, show="headings",
                        style="Peer.Treeview", selectmode="browse", height=5)
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

    # 4. Status strip
    status_frame = ttk.Frame(outer, style="Panel.TFrame", padding=(10, 8))
    status_frame.pack(fill="x", pady=(8, 0))
    status_var = tk.StringVar(value="add a torrent to start")
    ttk.Label(status_frame, textvariable=status_var, style="Status.TLabel", wraplength=820).pack(anchor="w")

    root.bind("<Escape>", pause_selected)

    timer = [None]

    def tick():
        if window.closed:
            return
        previous_active = scheduler.active_id
        scheduler.poll()
        snap = controller.snapshot
        refresh_ui()
        if controller.busy:
            if snap is not None and snap.status == "running":
                set_status(f"Downloading {format_percent(snap.done_bytes, snap.length)}")
            elif snap is not None and snap.status == "metadata":
                set_status("Finding peers and fetching metadata...")
            elif snap is not None and snap.status == "starting":
                set_status("Starting...")
        elif selected_item() is not None and selected_item().status == "error":
            set_status(f"Failed: {selected_item().error or 'Download failed.'} Use Retry to resume.")
        elif previous_active and not session.queue_running:
            set_status("Complete" if snap is not None and snap.status == "complete" else "Queue stopped")
        timer[0] = root.after(250, tick)

    def on_close():
        if window.closed:
            return
        window.closed = True
        if timer[0] is not None:
            root.after_cancel(timer[0])
        scheduler.close()
        controller.join(timeout=5)
        root.destroy()

    root.protocol("WM_DELETE_WINDOW", on_close)

    # CLI preload: add then select
    if torrent is not None:
        pre = add_torrent(torrent, output_path=Path(output) if output else None)
        if pre is not None:
            selected_id[0] = pre.id

    refresh_ui()
    if restored:
        set_status(f"restored {restored} torrents")
    elif not session.items():
        set_status("add a torrent to start")
        header_var.set("no torrents")

    # Auto-resume at most one downloading intent (first in session order)
    auto = session.downloading_id()
    if auto and session.get(auto):
        if selected_id[0] is None:
            selected_id[0] = auto
        scheduler.resume(session.get(auto))
        refresh_ui()

    window.buttons = {
        "add": add_btn, "remove": remove_btn, "pause": pause_btn,
        "resume": resume_btn, "retry": retry_btn, "open_folder": open_btn,
        "start_queue": start_queue_btn, "stop_queue": stop_queue_btn,
    }
    window.queue, window.status = queue, status_var
    window.scheduler, window.refresh, window.close = scheduler, refresh_ui, on_close
    timer[0] = root.after(100, tick)
    return window


def run(*args, **kwargs):
    window = create_window(*args, **kwargs)
    window.root.mainloop()
    window.controller.join(timeout=5)
    window.session.save()
    snap = window.controller.snapshot
    if snap is not None and snap.status == "error":
        return 1
    if snap is not None and snap.status == "cancelled":
        return 130
    return 0
