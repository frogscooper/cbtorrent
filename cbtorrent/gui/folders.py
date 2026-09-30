"""Open directories only, using OS argument APIs rather than a command shell."""
import os
import subprocess
import sys
from pathlib import Path


def open_folder(path):
    path = Path(path).expanduser().absolute()
    if not path.is_dir():
        raise FileNotFoundError("Download folder does not exist yet. Start the download first.")
    if sys.platform == "win32":
        os.startfile(str(path))
    else:
        command = "open" if sys.platform == "darwin" else "xdg-open"
        subprocess.Popen([command, str(path)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
