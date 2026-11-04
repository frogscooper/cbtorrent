"""Port mapping against fake NAT-PMP and UPnP gateways on loopback only."""
import asyncio
import struct
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from cbtorrent import client, portmap
from cbtorrent.client import download
from cbtorrent.metainfo import create
from cbtorrent.seeder import FileSource, SeedServer
from cbtorrent.tracker import Announce


class FakeNatPmp(asyncio.DatagramProtocol):
    """RFC 6886 TCP mapping responder. external_offset simulates a different public port."""

    def __init__(self, external_offset=0, result=0, silent=False):
        self.requests, self.external_offset, self.result, self.silent = [], external_offset, result, silent

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, address):
        version, op, _, internal, external, lifetime = struct.unpack("!BBHHHI", data[:12])
        self.requests.append((op, internal, external, lifetime))
        if self.silent:
            return
        granted = 0 if lifetime == 0 else external + self.external_offset
        self.transport.sendto(struct.pack("!BBHIHHI", 0, 130, self.result, 1, internal, granted,
                                          lifetime), address)


async def natpmp_gateway(test, **options):
    loop = asyncio.get_running_loop()
    transport, fake = await loop.create_datagram_endpoint(
        lambda: FakeNatPmp(**options), local_addr=("127.0.0.1", 0))
    test.addCleanup(transport.close)
    return fake, transport.get_extra_info("sockname")[1]


DESCRIPTION = b"""<?xml version="1.0"?>
<root xmlns="urn:schemas-upnp-org:device-1-0"><device><deviceList><device><deviceList><device>
<serviceList><service>
<serviceType>urn:schemas-upnp-org:service:WANIPConnection:1</serviceType>
<controlURL>/ctl/IPConn</controlURL>
</service></serviceList></device></deviceList></device></deviceList></device></root>"""


class FakeIgd:
    """SSDP responder plus HTTP description and SOAP control on 127.0.0.1."""

    def __init__(self, *, permanent_only=False, description=DESCRIPTION, location_host=None):
        self.actions, self.permanent_only = [], permanent_only
        self.description, self.location_host = description, location_host

    async def start(self, test):
        self.http = await asyncio.start_server(self.serve, "127.0.0.1", 0)
        test.addCleanup(self.http.close)
        self.http_port = self.http.sockets[0].getsockname()[1]
        loop = asyncio.get_running_loop()
        igd = self

        class Ssdp(asyncio.DatagramProtocol):
            def connection_made(self, transport):
                self.transport = transport

            def datagram_received(self, data, address):
                if b"M-SEARCH" in data:
                    host = igd.location_host or "127.0.0.1"
                    self.transport.sendto(
                        (f"HTTP/1.1 200 OK\r\nST: x\r\nLOCATION: http://{host}:{igd.http_port}/desc.xml"
                         "\r\n\r\n").encode(), address)

        transport, _ = await loop.create_datagram_endpoint(Ssdp, local_addr=("127.0.0.1", 0))
        test.addCleanup(transport.close)
        return ("127.0.0.1", transport.get_extra_info("sockname")[1])

    async def serve(self, reader, writer):
        head = await reader.readuntil(b"\r\n\r\n")
        length = next((int(line.split(b":")[1]) for line in head.split(b"\r\n")
                       if line.lower().startswith(b"content-length")), 0)
        body = await reader.readexactly(length)
        status, reply = 200, b""
        if head.startswith(b"GET /desc.xml"):
            reply = self.description
        else:
            action = head.split(b"#", 1)[1].split(b'"', 1)[0].decode()
            lease = body.split(b"<NewLeaseDuration>")[1].split(b"<")[0] if b"NewLeaseDuration" in body else b""
            self.actions.append((action, lease.decode(), body))
            if action == "AddPortMapping" and self.permanent_only and lease != b"0":
                status = 500
                reply = (b'<?xml version="1.0"?><s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">'
                         b'<s:Body><s:Fault><detail><UPnPError><errorCode>725</errorCode>'
                         b'<errorDescription>OnlyPermanentLeasesSupported</errorDescription>'
                         b'</UPnPError></detail></s:Fault></s:Body></s:Envelope>')
            else:
                reply = b'<?xml version="1.0"?><s:Envelope xmlns:s="x"><s:Body/></s:Envelope>'
        writer.write(b"HTTP/1.1 %d X\r\nContent-Length: %d\r\n\r\n" % (status, len(reply)) + reply)
        await writer.drain()
        writer.close()


class NatPmpTests(unittest.IsolatedAsyncioTestCase):
    async def test_maps_and_deletes(self):
        fake, port = await natpmp_gateway(self)
        seen = []
        mapper = portmap.PortMapper(40000, gateway="127.0.0.1", natpmp_port=port, use_upnp=False,
                                    lease=60, on_mapped=seen.append)
        mapper.start()
        for _ in range(50):
            if mapper.mapping:
                break
            await asyncio.sleep(0.02)
        self.assertEqual(mapper.status()["method"], "natpmp")
        self.assertEqual(mapper.status()["external_port"], 40000)
        self.assertEqual(len(seen), 1)
        await mapper.close()
        self.assertEqual(fake.requests[0], (2, 40000, 40000, 60))
        self.assertEqual(fake.requests[-1], (2, 40000, 0, 0))  # RFC 6886 deletion
        await mapper.close()  # idempotent: no second delete
        self.assertEqual(len(fake.requests), 2)

    async def test_different_external_port_is_reported(self):
        fake, port = await natpmp_gateway(self, external_offset=7)
        external, lifetime = await portmap.natpmp_request("127.0.0.1", 40000, 40000, 60, port=port)
        self.assertEqual((external, lifetime), (40007, 60))

    async def test_error_result_and_silence_fail_within_deadline(self):
        _, port = await natpmp_gateway(self, result=2)
        with self.assertRaisesRegex(portmap.MappingError, "result code 2"):
            await portmap.natpmp_request("127.0.0.1", 1, 1, 60, port=port)
        _, port = await natpmp_gateway(self, silent=True)
        loop = asyncio.get_running_loop()
        started = loop.time()
        with self.assertRaisesRegex(portmap.MappingError, "did not reply"):
            await portmap.natpmp_request("127.0.0.1", 1, 1, 60, timeout=0.5, port=port)
        self.assertLess(loop.time() - started, 2)

    async def test_public_gateway_is_refused(self):
        with self.assertRaises(portmap.MappingError):
            await portmap.natpmp_request("8.8.8.8", 1, 1, 60)


class UpnpTests(unittest.IsolatedAsyncioTestCase):
    async def mapper(self, igd, **options):
        ssdp = await igd.start(self)
        mapper = portmap.PortMapper(40001, use_natpmp=False, ssdp_address=ssdp, timeout=3, **options)
        mapper.mapping = await mapper._map()
        return mapper

    async def test_add_and_delete_mapping(self):
        igd = FakeIgd()
        mapper = await self.mapper(igd)
        self.assertEqual(mapper.status()["method"], "upnp")
        self.assertEqual(mapper.mapping.control_url, f"http://127.0.0.1:{igd.http_port}/ctl/IPConn")
        action, lease, body = igd.actions[0]
        self.assertEqual((action, lease), ("AddPortMapping", "3600"))
        self.assertIn(b"<NewInternalClient>127.0.0.1</NewInternalClient>", body)
        self.assertIn(b"<NewProtocol>TCP</NewProtocol>", body)
        await mapper.close()
        self.assertEqual(igd.actions[-1][0], "DeletePortMapping")

    async def test_permanent_only_router_gets_lease_zero(self):
        igd = FakeIgd(permanent_only=True)
        mapper = await self.mapper(igd)
        self.assertEqual([a[1] for a in igd.actions], ["3600", "0"])
        self.assertEqual(mapper.status()["lifetime"], 0)

    async def test_dtd_in_description_is_rejected(self):
        igd = FakeIgd(description=b'<?xml version="1.0"?><!DOCTYPE x [<!ENTITY a "b">]><root/>')
        mapper = await self.mapper(igd)
        self.assertIsNone(mapper.mapping)
        self.assertTrue(any("DTD" in e for e in mapper.errors), mapper.errors)
        self.assertEqual(igd.actions, [])

    async def test_location_on_another_host_is_ignored(self):
        igd = FakeIgd(location_host="10.255.255.1")  # responder IP differs from LOCATION host
        mapper = await self.mapper(igd)
        self.assertIsNone(mapper.mapping)
        self.assertIn("upnp: no UPnP gateway answered", mapper.errors)

    def test_find_service_rejects_control_url_on_other_host(self):
        xml = DESCRIPTION.replace(b"/ctl/IPConn", b"http://192.168.9.9/ctl")
        with self.assertRaisesRegex(portmap.MappingError, "another host"):
            portmap.find_service(xml, "http://192.168.1.1:5000/desc.xml")

    def test_status_never_contains_addresses(self):
        mapper = portmap.PortMapper(5000)
        mapper.mapping = portmap.Mapping("upnp", 5000, 5000, 3600, gateway="192.168.1.1",
                                         control_url="http://192.168.1.1/ctl", internal_client="192.168.1.5")
        self.assertNotIn("192.168", repr(mapper.status()))


class ClientMappingTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.data = bytes(range(256)) * 300
        (self.root / "source").write_bytes(self.data)
        self.torrent = create(self.root / "source", self.root / "a.torrent", piece_length=16384)
        source = FileSource(self.torrent, self.root / "source")
        self.addCleanup(source.close)
        server = SeedServer(self.torrent, source)
        self.addAsyncCleanup(server.close)
        self.seed = ("127.0.0.1", await server.start("127.0.0.1", 0))

    async def test_download_maps_reports_and_removes_the_listener(self):
        fake, port = await natpmp_gateway(self, external_offset=11)
        made = []

        def factory(internal_port, on_mapped):
            mapper = portmap.PortMapper(internal_port, gateway="127.0.0.1", natpmp_port=port,
                                        use_upnp=False, on_mapped=on_mapped)
            made.append(mapper)
            return mapper

        # The download finishes in milliseconds; close() must still see the reply and delete.
        with patch.object(client, "PortMapper", side_effect=factory):
            report = await asyncio.wait_for(download(
                self.torrent, [self.seed], self.root / "out", use_trackers=False,
                listen_host="0.0.0.0", port_mapping=True), 10)
        self.assertEqual((self.root / "out").read_bytes(), self.data)
        self.assertEqual(len(made), 1)
        internal = made[0].internal_port
        self.assertEqual(fake.requests[0][1:3], (internal, internal))
        self.assertEqual(fake.requests[-1], (2, internal, 0, 0))  # removed on exit
        self.assertIn("port_mapping", report)
        self.assertIn("incoming_peers", report)

    async def test_trackers_are_told_the_external_port_once_mapped(self):
        fake, port = await natpmp_gateway(self, external_offset=11)
        torrent = create(self.root / "source", self.root / "t.torrent", piece_length=16384,
                         trackers=["http://tracker.invalid/announce"])
        announced = []
        mapped = asyncio.Event()

        async def fake_announce(url, info_hash, peer_id, *, port, event, **_):
            announced.append((event, port))
            if event == "started":
                await mapped.wait()  # hold the first peer list until the mapping exists
            return Announce(peers=(self.seed,), interval=1, response_bytes=0)

        def factory(internal_port, on_mapped):
            def notify(mapping):
                on_mapped(mapping)
                mapped.set()
            return portmap.PortMapper(internal_port, gateway="127.0.0.1", natpmp_port=port,
                                      use_upnp=False, on_mapped=notify)

        with patch.object(client, "PortMapper", side_effect=factory), \
                patch.object(client, "announce", fake_announce):
            await asyncio.wait_for(download(torrent, [], self.root / "out", listen_host="0.0.0.0",
                                            port_mapping=True), 10)
        internal = fake.requests[0][1]
        self.assertEqual(announced[0], ("started", internal))  # announced before the reply
        later = announced[1:]
        self.assertIn("completed", [event for event, _ in later])
        self.assertEqual({p for _, p in later}, {internal + 11})

    async def test_loopback_listener_or_disabled_option_never_maps(self):
        with patch.object(client, "PortMapper") as mapper:
            report = await asyncio.wait_for(download(
                self.torrent, [self.seed], self.root / "a", use_trackers=False,
                listen_host="127.0.0.1", port_mapping=True), 10)
            await asyncio.wait_for(download(
                self.torrent, [self.seed], self.root / "b", use_trackers=False,
                listen_host="0.0.0.0"), 10)
        mapper.assert_not_called()
        self.assertIsNone(report["port_mapping"])


if __name__ == "__main__":
    unittest.main()
