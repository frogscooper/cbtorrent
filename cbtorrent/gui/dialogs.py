"""The ordinary add workflow: source, destination folder, and local validation."""
from pathlib import Path


class AddDialog:
    def __init__(self, parent, session, on_added):
        import tkinter as tk
        from tkinter import filedialog, ttk
        self.session, self.on_added = session, on_added
        self.window = tk.Toplevel(parent)
        self.window.title("Add torrent")
        self.window.transient(parent)
        self.window.resizable(True, False)
        self.source = tk.StringVar(self.window)
        self.folder = tk.StringVar(self.window, value=str(session.download_folder.absolute()))
        self.error = tk.StringVar(self.window)
        body = ttk.Frame(self.window, padding=16)
        body.grid(sticky="nsew")
        self.window.columnconfigure(0, weight=1)
        body.columnconfigure(0, weight=1)
        ttk.Label(body, text="Torrent file or magnet link").grid(row=0, column=0, sticky="w")
        self.source_entry = ttk.Entry(body, textvariable=self.source, width=64)
        self.source_entry.grid(row=1, column=0, sticky="ew", pady=(4, 12))
        def browse_source():
            chosen = filedialog.askopenfilename(parent=self.window, title="Choose torrent file",
                         filetypes=[("Torrent files", "*.torrent"), ("All files", "*.*")])
            if chosen:
                self.source.set(chosen)
        self.browse_button = ttk.Button(body, text="Browse…", command=browse_source)
        self.browse_button.grid(row=1, column=1, padx=(8, 0), pady=(4, 12))
        ttk.Label(body, text="Download folder").grid(row=2, column=0, sticky="w")
        self.folder_entry = ttk.Entry(body, textvariable=self.folder)
        self.folder_entry.grid(row=3, column=0, sticky="ew", pady=(4, 4))
        def browse_folder():
            chosen = filedialog.askdirectory(parent=self.window, title="Choose download folder",
                                            initialdir=self.folder.get(), mustexist=True)
            if chosen:
                self.folder.set(chosen)
        self.folder_button = ttk.Button(body, text="Choose…", command=browse_folder)
        self.folder_button.grid(row=3, column=1, padx=(8, 0), pady=(4, 4))
        ttk.Label(body, text="Remembered for future downloads. Adding puts this torrent in the queue.").grid(
            row=4, column=0, columnspan=2, sticky="w")
        ttk.Label(body, textvariable=self.error, foreground="#ff9a9a", wraplength=520).grid(
            row=5, column=0, columnspan=2, sticky="w", pady=(8, 0))
        actions = ttk.Frame(body)
        actions.grid(row=6, column=0, columnspan=2, sticky="e", pady=(12, 0))
        self.cancel_button = ttk.Button(actions, text="Cancel", command=self.window.destroy)
        self.cancel_button.pack(side="left")
        self.add_button = ttk.Button(actions, text="Add to Queue", command=self.submit)
        self.add_button.pack(side="left", padx=(8, 0))
        self.window.bind("<Return>", lambda event: self.submit())
        self.window.bind("<Escape>", lambda event: self.window.destroy())
        self.window.grab_set()
        self.source_entry.focus_set()

    def submit(self):
        try:
            source, folder = self.source.get().strip(), self.folder.get().strip()
            if not source:
                raise ValueError("Choose a torrent file or paste a magnet link.")
            if not folder:
                raise ValueError("Choose a download folder.")
            if len(folder) > 4096 or "\x00" in folder:
                raise ValueError("Invalid download folder.")
            folder = Path(folder).expanduser().absolute()
            if folder.exists() and not folder.is_dir():
                raise ValueError("The download folder points to a file. Choose a directory.")
            if not source.lower().startswith("magnet:"):
                source = Path(source).expanduser().absolute()
            item = self.session.add(source, download_folder=folder)
        except (OSError, ValueError) as error:
            self.error.set(str(error))
            return
        self.on_added(item)
        self.window.destroy()
