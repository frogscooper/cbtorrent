"""Bounded NAT port mapping (NAT-PMP, RFC 6886, and UPnP IGD) for the TCP listener.

Without a mapping, peers behind a home router cannot open connections to us, so
we never upload. Mapping is best effort: every step has a deadline and a size
limit, only private-network gateways are contacted, and failure leaves the
download unchanged. Reports carry the method and external port, never the
public IP address.
"""
import asyncio
import ipaddress
import os
import re
import socket
import struct
import subprocess
import sys
from dataclasses import dataclass
from urllib.parse import urljoin, urlsplit
from xml.etree import ElementTree

from .diagnostics import describe_error

SSDP_ADDRESS = ("239.255.255.250", 1900)
NATPMP_PORT = 5351
MAX_SSDP = 2048
MAX_HTTP = 65536
DESCRIPTION = "cbtorrent"
SERVICE_TYPES = ("urn:schemas-upnp-org:service:WANIPConnection:2",
                 "urn:schemas-upnp-org:service:WANIPConnection:1",
                 "urn:schemas-upnp-org:service:WANPPPConnection:1")
DEVICE_TARGET = "urn:schemas-upnp-org:device:InternetGatewayDevice:1"


class MappingError(Exception):
    pass


@dataclass
class Mapping:
    method: str
    internal_port: int
    external_port: int
    lifetime: int  # seconds; 0 means the router only grants permanent mappings
    gateway: str = ""
    control_url: str = ""
    service_type: str = ""
    internal_client: str = ""


def lan_address(host):
    """True for private or loopback IPv4 addresses (where a home gateway lives)."""
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        return False
    return address.version == 4 and (address.is_private or address.is_loopback) \
        and not address.is_multicast and not address.is_unspecified


def local_address_towards(host, port=1900):
    """The local IPv4 address the OS would use to reach host (no packet is sent)."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
        probe.connect((host, port))
        return probe.getsockname()[0]


def default_gateway(timeout=2.0):
    """Best-effort IPv4 default gateway, or None. Uses OS route tables only."""
    try:
        if sys.platform.startswith("linux"):
            with open("/proc/net/route", encoding="ascii") as table:
                for line in table.readlines()[1:64]:
                    fields = line.split()
                    if len(fields) > 3 and fields[1] == "00000000" and int(fields[3], 16) & 2:
                        gateway = socket.inet_ntoa(struct.pack("<I", int(fields[2], 16)))
                        return gateway if lan_address(gateway) else None
            return None
        if sys.platform == "win32":
            command = ["route", "print", "-4", "0.0.0.0"]
            pattern = re.compile(r"^\s*0\.0\.0\.0\s+0\.0\.0\.0\s+(\d+\.\d+\.\d+\.\d+)\s", re.M)
        else:
            command = ["route", "-n", "get", "default"]
            pattern = re.compile(r"gateway:\s*(\d+\.\d+\.\d+\.\d+)")
        flags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
        output = subprocess.run(command, capture_output=True, text=True, errors="replace",
                                timeout=timeout, creationflags=flags).stdout[:65536]
        for gateway in pattern.findall(output):
            if lan_address(gateway):
                return gateway
    except (OSError, ValueError, subprocess.SubprocessError):
        return None
    return None


# NAT-PMP ---------------------------------------------------------------------------

class _Datagram(asyncio.DatagramProtocol):
    def __init__(self):
        self.replies = asyncio.Queue(maxsize=64)

    def datagram_received(self, data, address):
        if not self.replies.full():
            self.replies.put_nowait((data[:MAX_SSDP], address))

    def error_received(self, exc):
        pass


async def natpmp_request(gateway, internal_port, external_port, lifetime, *,
                         timeout=2.0, port=NATPMP_PORT):
    """One TCP mapping request; returns (external_port, lifetime). RFC 6886 section 3.3."""
    if not lan_address(gateway):
        raise MappingError("NAT-PMP gateway is not a private address")
    loop = asyncio.get_running_loop()
    transport, protocol = await loop.create_datagram_endpoint(
        _Datagram, remote_addr=(gateway, port), family=socket.AF_INET)
    request = struct.pack("!BBHHHI", 0, 2, 0, internal_port, external_port, lifetime)
    try:
        async with asyncio.timeout(timeout):
            wait = 0.25
            while True:
                transport.sendto(request)
                try:
                    async with asyncio.timeout(wait):
                        while True:
                            data, address = await protocol.replies.get()
                            if address[0] != gateway or len(data) < 16:
                                continue
                            version, op, result, _epoch, internal, external, granted = \
                                struct.unpack("!BBHIHHI", data[:16])
                            if version != 0 or op != 130 or internal != internal_port:
                                continue
                            if result != 0:
                                raise MappingError(f"NAT-PMP result code {result}")
                            return external, granted
                except TimeoutError:
                    wait = min(wait * 2, 1.0)
    except TimeoutError:
        raise MappingError("NAT-PMP gateway did not reply") from None
    finally:
        transport.close()


# UPnP IGD --------------------------------------------------------------------------

def parse_ssdp(data):
    """Return the LOCATION of an SSDP search reply, or None."""
    try:
        text = data.decode("ascii")
    except UnicodeDecodeError:
        return None
    lines = text.split("\r\n")
    if not lines[0].startswith("HTTP/1.1 200"):
        return None
    for line in lines[1:]:
        key, _, value = line.partition(":")
        if key.strip().lower() == "location":
            return value.strip() or None
    return None


async def ssdp_search(timeout=2.0, address=SSDP_ADDRESS, interface=None):
    """Gateway description URLs that answered a search, served from the responder's own IP.

    interface is the local IPv4 address facing the gateway. Without it, the OS may
    send multicast out of a VPN or virtual adapter and no router ever hears it.
    """
    loop = asyncio.get_running_loop()
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.bind((interface or "0.0.0.0", 0))
        if interface:
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton(interface))
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
    except OSError:
        sock.close()
        raise
    transport, protocol = await loop.create_datagram_endpoint(_Datagram, sock=sock)
    locations = []
    try:
        for target in (DEVICE_TARGET, *SERVICE_TYPES[1:]):
            transport.sendto((f"M-SEARCH * HTTP/1.1\r\nHOST: {address[0]}:{address[1]}\r\n"
                              f"MAN: \"ssdp:discover\"\r\nMX: 1\r\nST: {target}\r\n\r\n").encode("ascii"),
                             address)
        deadline = loop.time() + timeout
        while len(locations) < 4 and (left := deadline - loop.time()) > 0:
            try:
                async with asyncio.timeout(left):
                    data, source = await protocol.replies.get()
            except TimeoutError:
                break
            location = parse_ssdp(data)
            if not location:
                continue
            parts = urlsplit(location)
            if parts.scheme == "http" and parts.hostname == source[0] and lan_address(source[0]) \
                    and location not in locations:
                locations.append(location)
    finally:
        transport.close()
    return locations


async def http_request(url, *, method="GET", body=b"", headers=(), timeout=3.0):
    """Minimal bounded HTTP/1.1 client for a LAN gateway. Returns (status, body)."""
    parts = urlsplit(url)
    if parts.scheme != "http" or not parts.hostname or not lan_address(parts.hostname):
        raise MappingError("gateway URL must be plain HTTP on a private address")
    async with asyncio.timeout(timeout):
        reader, writer = await asyncio.open_connection(parts.hostname, parts.port or 80, limit=MAX_HTTP)
        try:
            path = (parts.path or "/") + (f"?{parts.query}" if parts.query else "")
            head = [f"{method} {path} HTTP/1.1", f"Host: {parts.netloc}", "Connection: close",
                    f"Content-Length: {len(body)}", *headers]
            writer.write(("\r\n".join(head) + "\r\n\r\n").encode("ascii") + body)
            await writer.drain()
            try:
                raw = await reader.readuntil(b"\r\n\r\n")
            except (asyncio.LimitOverrunError, asyncio.IncompleteReadError) as error:
                raise MappingError("invalid gateway HTTP headers") from error
            lines = raw[:-4].decode("latin-1").split("\r\n")
            status = lines[0].split()
            if len(status) < 2 or not status[1].isdigit():
                raise MappingError("invalid gateway HTTP status")
            fields = {}
            for line in lines[1:]:
                key, _, value = line.partition(":")
                fields[key.strip().lower()] = value.strip()
            if "chunked" in fields.get("transfer-encoding", "").lower():
                data = bytearray()
                while True:
                    try:
                        size = int((await reader.readline()).split(b";")[0].strip(), 16)
                    except ValueError as error:
                        raise MappingError("invalid gateway HTTP chunk") from error
                    if size == 0:
                        break
                    if size < 0 or len(data) + size > MAX_HTTP:
                        raise MappingError("gateway response exceeds size limit")
                    data.extend(await reader.readexactly(size))
                    await reader.readexactly(2)
            elif "content-length" in fields:
                size = int(fields["content-length"]) if fields["content-length"].isdigit() else -1
                if not 0 <= size <= MAX_HTTP:
                    raise MappingError("gateway response exceeds size limit")
                data = await reader.readexactly(size)
            else:
                data = bytearray()
                while chunk := await reader.read(MAX_HTTP + 1 - len(data)):
                    data.extend(chunk)
                    if len(data) > MAX_HTTP:
                        raise MappingError("gateway response exceeds size limit")
            return int(status[1]), bytes(data)
        finally:
            writer.close()


def parse_xml(data):
    if b"<!DOCTYPE" in data or b"<!ENTITY" in data:
        raise MappingError("gateway XML must not declare a DTD")
    try:
        return ElementTree.fromstring(data)
    except ElementTree.ParseError as error:
        raise MappingError("invalid gateway XML") from error


def local_name(element):
    return element.tag.rsplit("}", 1)[-1]


def find_service(description, location):
    """(control URL, service type) of the first WAN connection service on the device itself."""
    root = parse_xml(description)
    base = next((e.text.strip() for e in root.iter() if local_name(e) == "URLBase" and e.text), location)
    for service in (e for e in root.iter() if local_name(e) == "service"):
        fields = {local_name(child): (child.text or "").strip() for child in service}
        if fields.get("serviceType") in SERVICE_TYPES and fields.get("controlURL"):
            control = urljoin(base, fields["controlURL"])
            if urlsplit(control).hostname != urlsplit(location).hostname:
                raise MappingError("gateway control URL points to another host")
            return control, fields["serviceType"]
    raise MappingError("gateway has no WAN connection service")


class SoapError(MappingError):
    def __init__(self, code, description):
        super().__init__(f"UPnP error {code}: {description}")
        self.code = code


def _escape(value):
    return str(value).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


async def soap(control_url, service_type, action, arguments, *, timeout=3.0):
    body = ('<?xml version="1.0"?><s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" '
            's:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/"><s:Body>'
            f'<u:{action} xmlns:u="{service_type}">'
            + "".join(f"<{k}>{_escape(v)}</{k}>" for k, v in arguments)
            + f"</u:{action}></s:Body></s:Envelope>").encode("utf-8")
    status, reply = await http_request(
        control_url, method="POST", body=body, timeout=timeout,
        headers=('Content-Type: text/xml; charset="utf-8"', f'SOAPAction: "{service_type}#{action}"'))
    root = parse_xml(reply) if reply.strip() else None
    if status != 200:
        code = next((e.text for e in root.iter() if local_name(e) == "errorCode"), None) if root is not None else None
        text = next((e.text for e in root.iter() if local_name(e) == "errorDescription"), "") if root is not None else ""
        raise SoapError(int(code) if code and code.strip().isdigit() else status, (text or "").strip()[:200])
    return {local_name(e): (e.text or "").strip() for e in root.iter()} if root is not None else {}


async def upnp_add(control_url, service_type, internal_client, internal_port, external_port, lease,
                   *, timeout=3.0):
    arguments = (("NewRemoteHost", ""), ("NewExternalPort", external_port), ("NewProtocol", "TCP"),
                 ("NewInternalPort", internal_port), ("NewInternalClient", internal_client),
                 ("NewEnabled", 1), ("NewPortMappingDescription", DESCRIPTION),
                 ("NewLeaseDuration", lease))
    await soap(control_url, service_type, "AddPortMapping", arguments, timeout=timeout)


async def upnp_delete(control_url, service_type, external_port, *, timeout=2.0):
    await soap(control_url, service_type, "DeletePortMapping",
               (("NewRemoteHost", ""), ("NewExternalPort", external_port), ("NewProtocol", "TCP")),
               timeout=timeout)


# Lifecycle -------------------------------------------------------------------------

class PortMapper:
    """Map one TCP port, keep the lease fresh, and remove it on close."""

    def __init__(self, internal_port, *, lease=3600, timeout=6.0, gateway=None,
                 ssdp_address=SSDP_ADDRESS, natpmp_port=NATPMP_PORT, use_natpmp=True, use_upnp=True,
                 on_mapped=None):
        if not 1 <= internal_port <= 65535 or not 0 < lease <= 86400 or timeout <= 0:
            raise ValueError("invalid port mapping settings")
        self.internal_port, self.lease, self.timeout = internal_port, lease, timeout
        self.gateway, self.ssdp_address, self.natpmp_port = gateway, ssdp_address, natpmp_port
        self.use_natpmp, self.use_upnp, self.on_mapped = use_natpmp, use_upnp, on_mapped
        self.mapping = None
        self.errors = []
        self._task = None
        self._closed = False
        self._settled = asyncio.Event()

    def status(self):
        """Report-safe summary: no public or local IP addresses."""
        mapping = self.mapping
        return dict(mapped=mapping is not None, method=mapping.method if mapping else None,
                    internal_port=self.internal_port,
                    external_port=mapping.external_port if mapping else None,
                    lifetime=mapping.lifetime if mapping else None, errors=list(self.errors))

    def _note(self, text):
        if len(self.errors) < 8:
            self.errors.append(text)

    def start(self):
        if self._task is None:
            self._task = asyncio.create_task(self._run())
        return self._task

    async def _map(self):
        gateway = self.gateway or await asyncio.to_thread(default_gateway)
        if self.use_natpmp:
            if gateway:
                try:
                    external, lifetime = await natpmp_request(
                        gateway, self.internal_port, self.internal_port, self.lease,
                        timeout=min(2.0, self.timeout), port=self.natpmp_port)
                    return Mapping("natpmp", self.internal_port, external, lifetime, gateway=gateway)
                except (OSError, MappingError) as error:
                    self._note(f"natpmp: {describe_error(error)}")
            else:
                self._note("natpmp: no private default gateway")
        if self.use_upnp:
            try:
                async with asyncio.timeout(self.timeout):
                    interface = None
                    if gateway and not ipaddress.ip_address(self.ssdp_address[0]).is_loopback:
                        interface = local_address_towards(gateway)
                    locations = await ssdp_search(min(2.0, self.timeout), self.ssdp_address, interface)
                    if not locations:
                        raise MappingError("no UPnP gateway answered")
                    for location in locations:
                        try:
                            status, description = await http_request(location)
                            if status != 200:
                                raise MappingError(f"description HTTP status {status}")
                            control, service = find_service(description, location)
                            client = local_address_towards(urlsplit(control).hostname)
                            lease = self.lease
                            try:
                                await upnp_add(control, service, client, self.internal_port,
                                               self.internal_port, lease)
                            except SoapError as error:
                                if error.code != 725:  # OnlyPermanentLeasesSupported
                                    raise
                                lease = 0
                                await upnp_add(control, service, client, self.internal_port,
                                               self.internal_port, lease)
                            return Mapping("upnp", self.internal_port, self.internal_port, lease,
                                           control_url=control, service_type=service,
                                           internal_client=client)
                        except (OSError, MappingError) as error:
                            self._note(f"upnp: {describe_error(error)}")
            except TimeoutError:
                self._note("upnp: timed out")
            except (OSError, MappingError) as error:
                self._note(f"upnp: {describe_error(error)}")
        return None

    async def _run(self):
        try:
            self.mapping = await self._map()
        finally:
            self._settled.set()
        if self.mapping is None:
            return
        if self.on_mapped is not None:
            self.on_mapped(self.mapping)
        while self.mapping.lifetime:
            await asyncio.sleep(max(30, self.mapping.lifetime // 2))
            try:
                if self.mapping.method == "natpmp":
                    external, lifetime = await natpmp_request(
                        self.mapping.gateway, self.internal_port, self.mapping.external_port,
                        self.lease, timeout=2.0, port=self.natpmp_port)
                    changed = external != self.mapping.external_port
                    self.mapping.external_port, self.mapping.lifetime = external, lifetime
                    if changed and self.on_mapped is not None:
                        self.on_mapped(self.mapping)
                else:
                    await upnp_add(self.mapping.control_url, self.mapping.service_type,
                                   self.mapping.internal_client, self.internal_port,
                                   self.mapping.external_port, self.mapping.lifetime)
            except (OSError, MappingError) as error:
                self._note(f"refresh: {describe_error(error)}")

    async def close(self):
        if self._closed:
            return
        self._closed = True
        if self._task is not None:
            # A request may already be on the router; wait briefly for its answer so
            # we can delete it, instead of leaving it until the lease expires.
            try:
                async with asyncio.timeout(1.0):
                    await self._settled.wait()
            except TimeoutError:
                self._note("close: mapping still in progress; not removed")
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        mapping = self.mapping
        if mapping is None:
            return
        try:
            async with asyncio.timeout(2.0):
                if mapping.method == "natpmp":
                    await natpmp_request(mapping.gateway, self.internal_port, 0, 0, timeout=2.0,
                                         port=self.natpmp_port)
                else:
                    await upnp_delete(mapping.control_url, mapping.service_type, mapping.external_port)
        except (OSError, MappingError, TimeoutError) as error:
            self._note(f"delete: {describe_error(error)}")
