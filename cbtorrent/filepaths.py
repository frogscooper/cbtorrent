"""Portable torrent paths and checked reads of local payload files."""
import os
import stat
import unicodedata
from pathlib import Path


def component(value):
    if not isinstance(value, bytes):
        raise ValueError("torrent path components must be UTF-8 strings")
    name = value.decode("utf-8")
    reserved = {"con", "prn", "aux", "nul"} | {
        f"{prefix}{n}" for prefix in ("com", "lpt") for n in range(1, 10)}
    reserved |= {prefix + n for prefix in ("com", "lpt") for n in "¹²³"}
    if (not name or name in (".", "..") or name.endswith((".", " "))
            or any(c in '/\\:<>"|?*' or ord(c) < 32 or ord(c) == 127 for c in name)
            or name.split(".")[0].rstrip(" ").casefold() in reserved
            or len(value) > 255):
        raise ValueError(f"unsafe torrent path component: {name!r}")
    return name


def path_key(parts):
    return tuple(unicodedata.normalize("NFC", part).casefold() for part in parts)


def reject_symlinks(path):
    path = Path(path).absolute()
    for candidate in (path, *path.parents):
        try:
            attributes = getattr(candidate.lstat(), "st_file_attributes", 0)
        except FileNotFoundError:
            attributes = 0
        if candidate.is_symlink() or attributes & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0):
            raise ValueError(f"payload path must not be a symbolic link: {candidate}")


def open_payload(path):
    """Open a regular file, refusing links and special files before any read."""
    path = Path(path)
    reject_symlinks(path)
    if not stat.S_ISREG(path.stat().st_mode):
        raise ValueError(f"payload must be a regular file: {path}")
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
                 | getattr(os, "O_BINARY", 0) | getattr(os, "O_NONBLOCK", 0))
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise ValueError(f"payload must be a regular file: {path}")
        return os.fdopen(fd, "rb")
    except BaseException:
        os.close(fd)
        raise
