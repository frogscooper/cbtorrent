"""Verified partial-file storage. Resume trusts hashes, never saved flags."""
import os
from hashlib import sha1
from pathlib import Path


class Storage:
    def __init__(self, torrent, output, *, resume=False):
        self.torrent = torrent
        self.output = Path(output)
        self.part = self.output.with_name(self.output.name + ".part")
        self.verified = set()
        self.stream = None
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
        if len(self.verified) != len(self.torrent.hashes):
            raise ValueError("download is incomplete")
        self.stream.flush()
        os.fsync(self.stream.fileno())
        self.close()
        os.link(self.part, self.output)
        self.part.unlink()

    def close(self):
        if self.stream is not None:
            self.stream.close()
            self.stream = None
