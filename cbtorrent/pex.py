"""Bounded BEP 11 peer exchange. Contacts are hints, never verified content."""
import ipaddress
import struct
from time import monotonic

from .bencode import decode, encode
from .extensions import MAX_EXTENDED, extended

PEX_ID = 2
MAX_PEX = 4096
INTERVAL = 60.0


def contact(host, port):
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        return None
    if (not 1 <= port <= 65535 or address.is_unspecified or address.is_multicast
            or address.is_link_local or (address.is_reserved and not address.is_loopback)):
        return None
    return str(address), port


def parse(payload, *, initial=True):
    fields = decode(payload, max_size=MAX_PEX)
    names = (b"added", b"added6", b"dropped", b"dropped6")
    if not isinstance(fields, dict) or not any(name in fields for name in names):
        raise ValueError("PEX requires contact fields")
    added, dropped = [], []
    counts = {"added": 0, "dropped": 0}
    for name in names:
        raw = fields.get(name, b"")
        width = 18 if name.endswith(b"6") else 6
        if not isinstance(raw, bytes) or len(raw) % width:
            raise ValueError("invalid PEX compact contacts")
        direction = "added" if name.startswith(b"added") else "dropped"
        count = len(raw) // width
        counts[direction] += count
        flags = fields.get(name + b".f") if direction == "added" else None
        if flags is not None and (not isinstance(flags, bytes) or len(flags) != count):
            raise ValueError("invalid PEX peer flags")
        target = added if direction == "added" else dropped
        for start in range(0, len(raw), width):
            item = raw[start:start + width]
            host = str(ipaddress.ip_address(item[:-2]))
            port = struct.unpack("!H", item[-2:])[0]
            address = contact(host, port)
            if address is not None:
                target.append(address)
    if counts["added"] > (200 if initial else 50) or counts["dropped"] > (200 if initial else 50):
        raise ValueError("PEX contact count exceeds limit")
    if len(set(added)) != len(added) or len(set(dropped)) != len(dropped) or set(added) & set(dropped):
        raise ValueError("duplicate or contradictory PEX contacts")
    return tuple(added), tuple(dropped)


class PexSession:
    def __init__(self, remote, *, discover=None, connected=lambda: (), clock=monotonic):
        self.remote = contact(*remote)
        self.discover, self.connected, self.clock = discover, connected, clock
        self.remote_id = 0
        self.sent = set()
        self.last_sent = None
        self.received = 0
        self.tokens = 2.0  # Tolerate initial burst/jitter; refill one per minute.
        self.refilled = clock()

    def negotiate(self, payload, metadata_id=0):
        fields = decode(payload, max_size=MAX_EXTENDED)
        mapping = fields.get(b"m", {}) if isinstance(fields, dict) else None
        if not isinstance(mapping, dict):
            raise ValueError("invalid PEX negotiation")
        if b"ut_pex" in mapping:
            identifier = mapping[b"ut_pex"]
            if type(identifier) is not int or not 0 <= identifier <= 255:
                raise ValueError("invalid ut_pex extension ID")
            self.remote_id = identifier
        if self.remote_id and self.remote_id == metadata_id:
            raise ValueError("PEX and metadata extension IDs collide")

    def permitted(self, address):
        # A public peer must not redirect us into a private/local network.
        return not (self.remote and ipaddress.ip_address(self.remote[0]).is_global
                    and not ipaddress.ip_address(address[0]).is_global)

    def receive(self, payload):
        now = self.clock()
        self.tokens = min(2, self.tokens + max(0, now - self.refilled) / INTERVAL)
        self.refilled = now
        if self.tokens < 1:
            raise ValueError("PEX message rate exceeded")
        self.tokens -= 1
        added, _ = parse(payload, initial=self.received == 0)
        self.received += 1
        if self.discover:
            self.discover(tuple(address for address in added if address != self.remote
                                and self.permitted(address)))
        # Drops describe the sender's connections, not the liveness of ours.

    def outgoing(self):
        now = self.clock()
        if not self.remote_id or (self.last_sent is not None and now - self.last_sent < INTERVAL):
            return None
        current = {address for raw in self.connected()
                   if (address := contact(*raw)) is not None and address != self.remote
                   and self.permitted(address)}
        added = sorted(current - self.sent)[:50]
        dropped = sorted(self.sent - current)[:50]
        if not added and not dropped:
            return None
        fields = {}
        for direction, addresses in (("added", added), ("dropped", dropped)):
            for version in (4, 6):
                packed = [ipaddress.ip_address(host).packed + struct.pack("!H", port)
                          for host, port in addresses if ipaddress.ip_address(host).version == version]
                if packed:
                    name = (direction + ("6" if version == 6 else "")).encode()
                    fields[name] = b"".join(packed)
                    if direction == "added":
                        fields[name + b".f"] = bytes([0x10]) * len(packed)
        self.sent.difference_update(dropped)
        self.sent.update(added)
        self.last_sent = now
        return extended(self.remote_id, encode(fields))
