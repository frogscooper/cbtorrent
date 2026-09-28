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

    # 4. Status strip
    status_frame = ttk.Frame(outer, style="Panel.TFrame", padding=(10, 8))
    status_frame.pack(fill="x", pady=(8, 0))
    status_var = tk.StringVar(value="add a torrent to start")
    ttk.Label(status_frame, textvariable=status_var, style="Status.TLabel").pack(anchor="w")

    root.bind("<Escape>", pause_selected)

    def tick():
        snap = controller.snapshot
        active = controller.active_id
        if active and session.get(active):
            if snap is not None and snap.status == "complete":
                session.set_status(active, "complete")
            elif snap is not None and snap.status == "error":
                session.set_status(active, "error")
            elif snap is not None and snap.status == "cancelled":
                if session.get(active) and session.get(active).status == "downloading":
                    session.pause(active)
        refresh_ui()
        if controller.busy:
            if snap is not None and snap.status == "running":
                set_status(f"Downloading {format_percent(snap.done_bytes, snap.length)}")
            elif snap is not None and snap.status == "starting":
                set_status("Starting...")
            root.after(250, tick)
        else:
            snap = controller.snapshot
            if snap is not None and snap.status == "complete":
                set_status("Complete")
            elif snap is not None and snap.status == "error":
                set_status(f"Error: {snap.error or 'download failed'}")
            elif snap is not None and snap.status == "cancelled":
                set_status("Paused: partial file kept as .part")
            refresh_actions()

    def on_close():
        if controller.busy and controller.active_id:
            controller.cancel()
            controller.join(timeout=5)
            if session.get(controller.active_id):
                session.pause(controller.active_id)
        session.save()
        root.destroy()

    root.protocol("WM_DELETE_WINDOW", on_close)

    # CLI preload: add then select
    if torrent is not None:
        pre = add_torrent(Path(torrent), output_path=Path(output) if output else None)
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
        start_item(session.get(auto), force_resume=True)
        refresh_ui()

    root.mainloop()
    controller.join(timeout=5)
    session.save()
    snap = controller.snapshot
    if snap is not None and snap.status == "error":
        return 1
    if snap is not None and snap.status == "cancelled":
        return 130
    return 0
