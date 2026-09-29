"""Verified partial-file storage. Resume trusts hashes, never saved flags."""
import os
import asyncio
import tempfile
import stat
from hashlib import sha1
from pathlib import Path
from .filepaths import reject_symlinks


class Storage:
    def __init__(self, torrent, output, *, resume=False):
        self.torrent = torrent
        self.output = Path(output)
        self.part = self.output.with_name(self.output.name + ".part")
        self.verified = set()
        self.stream = None
        if torrent.multi_file:
            reject_symlinks(self.output)
            reject_symlinks(self.part)
            if self.part.exists():
                saved = self.part.stat()
                if not stat.S_ISREG(saved.st_mode) or saved.st_nlink != 1:
                    raise ValueError("partial payload must be a regular file with no hard links")
        if self.output.exists():
            raise FileExistsError(self.output)
        if self.part.is_symlink():
            raise ValueError("partial file must not be a symbolic link")
        self.stream = self.part.open("r+b" if resume and self.part.exists() else "x+b", buffering=0)
        try:
            if resume:
                for index, expected in enumerate(torrent.hashes):
                    self.stream.seek(index * torrent.piece_length)
                    data = self.stream.read(torrent.piece_size(index))
                    if len(data) == torrent.piece_size(index) and sha1(data).digest() == expected:
                        self.verified.add(index)
            self.stream.truncate(torrent.length)
        except BaseException:
            self.close()
            raise

    def write(self, index, data):
        if len(data) != self.torrent.piece_size(index) or sha1(data).digest() != self.torrent.hashes[index]:
            raise ValueError("cannot commit an unverified piece")
        self.stream.seek(index * self.torrent.piece_length)
        view = memoryview(data)
        while view:
            written = self.stream.write(view)
            if not written:
                raise OSError("short disk write")
            view = view[written:]
        self.verified.add(index)

    def read(self, index, offset, length):
        if index not in self.verified or offset < 0 or not 0 < length <= 16384:
            raise ValueError("invalid or unavailable block")
        if offset + length > self.torrent.piece_size(index):
            raise ValueError("block crosses piece boundary")
        self.stream.seek(index * self.torrent.piece_length + offset)
        data = self.stream.read(length)
        if len(data) != length:
            raise OSError("short disk read")
        return data

    def publish(self):
        if self.torrent.multi_file:
            raise ValueError("multi-file publication requires publish_async")
        if len(self.verified) != len(self.torrent.hashes):
            raise ValueError("download is incomplete")
        self.stream.flush()
        os.fsync(self.stream.fileno())
        self.close()
        os.link(self.part, self.output)
        self.part.unlink()

    async def publish_async(self):
        if not self.torrent.multi_file:
            self.publish()
            return
        if len(self.verified) != len(self.torrent.hashes):
            raise ValueError("download is incomplete")
        self.stream.flush()
        os.fsync(self.stream.fileno())
        reject_symlinks(self.output)
        # Build a complete directory privately. Keep the verified spool until
        # every destination has been exclusively linked, so failures can resume.
        with tempfile.TemporaryDirectory(dir=self.output.parent,
                                         prefix=".cbtorrent-publish-") as temporary:
            stage = Path(temporary)
            for file in self.torrent.files:
                target = stage.joinpath(*file.path)
                target.parent.mkdir(parents=True, exist_ok=True)
                self.stream.seek(file.offset)
                with target.open("xb") as destination:
                    remaining = file.length
                    while remaining:
                        data = self.stream.read(min(remaining, 1024 * 1024))
                        if not data:
                            raise OSError("short disk read during publication")
                        destination.write(data)
                        remaining -= len(data)
                        await asyncio.sleep(0)
                    destination.flush()
                    os.fsync(destination.fileno())
                await asyncio.sleep(0)
            reject_symlinks(self.output)
            self.output.mkdir()  # Exclusive, including an existing empty folder.
            linked, directories = [], [self.output]
            try:
                for file in self.torrent.files:
                    target = self.output.joinpath(*file.path)
                    parent = self.output
                    for part in file.path[:-1]:
                        parent = parent / part
                        if not parent.exists():
                            parent.mkdir()
                            directories.append(parent)
                        reject_symlinks(parent)
                    reject_symlinks(target)
                    os.link(stage.joinpath(*file.path), target)
                    linked.append(target)
                    await asyncio.sleep(0)
            except BaseException:
                # Remove only entries created here, never unrelated user data.
                for target in reversed(linked):
                    target.unlink()
                for directory in reversed(directories):
                    try:
                        directory.rmdir()
                    except OSError:
                        pass
                raise
        self.close()
        self.part.unlink()

    def close(self):
        if self.stream is not None:
            self.stream.close()
            self.stream = None
