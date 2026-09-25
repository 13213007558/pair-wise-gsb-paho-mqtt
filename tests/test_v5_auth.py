"""
Tests for MQTT v5.0 enhanced authentication (AUTH packet exchange).

The tests use a local fake broker connected to the client through a
socketpair, so no network access is required. The fake broker sends AUTH
packets in segments (byte by byte) to make sure the client correctly
reassembles fragmented packets.
"""

import socket
import struct
import time
import unittest.mock

import pytest

import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion, MQTTErrorCode
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

AUTH_METHOD = "test-auth"

# AUTH reason codes
RC_SUCCESS = 0
RC_CONTINUE = 24
RC_REAUTHENTICATE = 25

CONNACK_SUCCESS = b"\x20\x03\x00\x00\x00"
PINGREQ = b"\xc0\x00"
PINGRESP = b"\xd0\x00"


def _encode_utf8(value):
    return struct.pack("!H", len(value)) + value


def auth_packet(reason, method=None, data=None):
    """Build a broker->client AUTH packet, or the expected client->broker one.

    data may be b"" which is encoded as a zero-length Authentication Data
    property, which is different from the property being absent (None).
    """
    props = b""
    if method is not None:
        props += b"\x15" + _encode_utf8(method.encode("utf-8"))
    if data is not None:
        props += b"\x16" + _encode_utf8(data)
    if reason == RC_SUCCESS and not props:
        # reason code and properties may be omitted when all default
        return b"\xf0\x00"
    body = bytes([reason]) + bytes([len(props)]) + props
    return b"\xf0" + bytes([len(body)]) + body


class FakeBroker:
    """A minimal fake broker speaking to the client over a socketpair."""

    def __init__(self):
        self.client_sock, self.sock = socket.socketpair()
        self.sock.settimeout(5)

    def close(self):
        self.sock.close()
        self.client_sock.close()

    def recv_exact(self, count):
        data = b""
        while len(data) < count:
            chunk = self.sock.recv(count - len(data))
            if not chunk:
                raise ConnectionError("client closed the connection")
            data += chunk
        return data

    def read_packet(self):
        """Read one MQTT packet, return (command byte, body)."""
        cmd = self.recv_exact(1)[0]
        remaining_length = 0
        multiplier = 1
        while True:
            digit = self.recv_exact(1)[0]
            remaining_length += (digit & 0x7F) * multiplier
            multiplier *= 0x80
            if not digit & 0x80:
                break
        body = self.recv_exact(remaining_length) if remaining_length else b""
        return cmd, body

    def expect_packet(self, name, expected):
        received = self.recv_exact(len(expected))
        assert received == expected, (
            f"{name}: received {received!r}, expected {expected!r}")

    def expect_no_packet(self, delay=0.3):
        previous_timeout = self.sock.gettimeout()
        self.sock.settimeout(delay)
        try:
            data = self.sock.recv(1024)
        except socket.timeout:
            data = None
        finally:
            self.sock.settimeout(previous_timeout)
        assert data is None, f"shouldn't receive any data, got {data!r}"

    def send(self, data, segmented=False):
        """Send data, optionally byte by byte to test packet reassembly."""
        if segmented:
            for i in range(len(data)):
                self.sock.sendall(data[i:i + 1])
                time.sleep(0.005)
        else:
            self.sock.sendall(data)


def pump(client, rounds=5, timeout=0.02):
    """Run the client network loop a few times to process traffic."""
    for _ in range(rounds):
        client.loop(timeout)


def make_client(broker, *, keepalive=60, auth_method=AUTH_METHOD, on_auth=None):
    """Create a MQTT v5 client connected to the fake broker."""
    client = mqtt.Client(
        CallbackAPIVersion.VERSION2,
        client_id="auth-test-client",
        protocol=mqtt.MQTTv5,
    )
    events = {"connect": [], "disconnect": [], "auth": [], "publish": [], "log": []}

    client.on_connect = lambda c, u, flags, rc, props: events["connect"].append((flags, rc, props))
    client.on_disconnect = lambda c, u, flags, rc, props: events["disconnect"].append((flags, rc, props))
    client.on_publish = lambda c, u, mid, rc, props: events["publish"].append(mid)
    client.on_log = lambda c, u, level, buf: events["log"].append(buf)
    if on_auth is not None:
        client.on_auth = on_auth

    properties = None
    if auth_method is not None:
        properties = Properties(PacketTypes.CONNECT)
        properties.AuthenticationMethod = auth_method

    client.connect_async("fake-broker", 1883, keepalive=keepalive, properties=properties)
    with unittest.mock.patch.object(client, "_create_socket", return_value=broker.client_sock):
        client.reconnect()
    # Provide the internal wake-up socketpair ourselves: the default
    # implementation uses a loopback TCP connection which is not always
    # available in restricted test environments.
    client._sockpairR, client._sockpairW = socket.socketpair()
    client._sockpairR.setblocking(False)
    client._sockpairW.setblocking(False)
    return client, events


def read_connect(broker):
    cmd, body = broker.read_packet()
    assert cmd == 0x10, f"expected CONNECT, got command {cmd:#x}"
    return body


def test_two_round_auth_success():
    """Two rounds of Continue authentication before CONNACK, including an
    empty (but present) Authentication Data property from the client."""
    broker = FakeBroker()
    try:
        responses = {b"challenge-1": b"response-1", b"challenge-2": b""}

        def on_auth(client, userdata, reason_code, properties):
            data = None
            if properties is not None and hasattr(properties, "AuthenticationData"):
                data = bytes(properties.AuthenticationData)
            events["auth"].append((reason_code.value, data))
            if reason_code.value == RC_CONTINUE:
                # empty byte string is a valid answer, distinct from None
                assert client.authenticate(auth_data=responses[data]) == MQTTErrorCode.MQTT_ERR_SUCCESS

        client, events = make_client(broker, on_auth=on_auth)
        assert AUTH_METHOD.encode("utf-8") in read_connect(broker)

        # first round, AUTH is sent to the client byte by byte
        broker.send(auth_packet(RC_CONTINUE, AUTH_METHOD, b"challenge-1"), segmented=True)
        pump(client)
        broker.expect_packet("auth round 1", auth_packet(RC_CONTINUE, AUTH_METHOD, b"response-1"))
        # the connection callback must not fire before authentication completes
        assert events["connect"] == []

        # second round, the client answers with empty auth data
        broker.send(auth_packet(RC_CONTINUE, AUTH_METHOD, b"challenge-2"), segmented=True)
        pump(client)
        broker.expect_packet("auth round 2", auth_packet(RC_CONTINUE, AUTH_METHOD, b""))
        assert events["connect"] == []

        # the broker completes the exchange with CONNACK
        broker.send(CONNACK_SUCCESS, segmented=True)
        pump(client)
        assert len(events["connect"]) == 1
        assert events["connect"][0][1].value == RC_SUCCESS
        assert client.is_connected()
        assert events["auth"] == [(RC_CONTINUE, b"challenge-1"), (RC_CONTINUE, b"challenge-2")]

        client.disconnect()
        pump(client)
    finally:
        broker.close()


def test_auth_method_mismatch():
    """The broker may not switch the authentication method mid-connection."""
    broker = FakeBroker()
    try:
        client, events = make_client(broker)
        read_connect(broker)

        broker.send(auth_packet(RC_CONTINUE, "other-method", b"challenge"), segmented=True)
        pump(client)

        # the client must drop the connection without completing the connect
        with pytest.raises(ConnectionError):
            broker.read_packet()
        assert events["connect"] == []
        assert len(events["disconnect"]) == 1
        flags, reason_code, _ = events["disconnect"][0]
        assert not flags.is_disconnect_packet_from_server
        assert reason_code.value == 130  # Protocol error
    finally:
        broker.close()


def test_on_auth_exception():
    """An exception in the on_auth callback is logged and does not break
    the connection when suppress_exceptions is set."""
    broker = FakeBroker()
    try:
        def on_auth(client, userdata, reason_code, properties):
            raise RuntimeError("boom")

        client, events = make_client(broker, on_auth=on_auth)
        client.suppress_exceptions = True
        read_connect(broker)

        broker.send(auth_packet(RC_CONTINUE, AUTH_METHOD, b"challenge"), segmented=True)
        pump(client)
        assert any("Caught exception in on_auth" in msg for msg in events["log"])
        assert events["connect"] == []

        # the client is still alive and can complete the connection
        broker.send(CONNACK_SUCCESS)
        pump(client)
        assert len(events["connect"]) == 1
        assert client.is_connected()

        client.disconnect()
        pump(client)
    finally:
        broker.close()


def test_reauthenticate_while_connected():
    """Re-authentication on an established connection, with normal traffic
    (PUBLISH/PUBACK, keepalive) continuing during the exchange, and an
    explicit reason delivered to the application when it fails."""
    broker = FakeBroker()
    try:
        def on_auth(client, userdata, reason_code, properties):
            data = None
            if properties is not None and hasattr(properties, "AuthenticationData"):
                data = bytes(properties.AuthenticationData)
            events["auth"].append((reason_code.value, data))
            if reason_code.value == RC_CONTINUE:
                assert client.authenticate(auth_data=b"reauth-response") == MQTTErrorCode.MQTT_ERR_SUCCESS

        client, events = make_client(broker, keepalive=2, on_auth=on_auth)
        read_connect(broker)
        broker.send(CONNACK_SUCCESS)
        pump(client)
        assert len(events["connect"]) == 1

        # the application cannot switch to another authentication method
        bad_properties = Properties(PacketTypes.AUTH)
        bad_properties.AuthenticationMethod = "other-method"
        assert client.authenticate(properties=bad_properties) == MQTTErrorCode.MQTT_ERR_INVAL
        broker.expect_no_packet()

        # start re-authentication
        assert client.authenticate(auth_data=b"reauth-data") == MQTTErrorCode.MQTT_ERR_SUCCESS
        pump(client)
        broker.expect_packet("re-authenticate", auth_packet(RC_REAUTHENTICATE, AUTH_METHOD, b"reauth-data"))

        # PUBLISH/PUBACK still work during re-authentication
        info = client.publish("auth/topic", b"payload", qos=1)
        pump(client)
        cmd, body = broker.read_packet()
        assert cmd & 0xF0 == 0x30, f"expected PUBLISH, got command {cmd:#x}"
        topic_length = struct.unpack("!H", body[:2])[0]
        mid = struct.unpack("!H", body[2 + topic_length:4 + topic_length])[0]
        assert mid == info.mid
        broker.send(b"\x40\x02" + struct.pack("!H", mid))  # PUBACK
        pump(client)
        assert info.mid in events["publish"]

        # keepalive still works during re-authentication
        time.sleep(2.1)
        pump(client)
        broker.expect_packet("pingreq", PINGREQ)
        broker.send(PINGRESP)
        pump(client)

        # the broker continues the exchange, the client answers
        broker.send(auth_packet(RC_CONTINUE, AUTH_METHOD, b"reauth-challenge"), segmented=True)
        pump(client)
        broker.expect_packet("reauth continue", auth_packet(RC_CONTINUE, AUTH_METHOD, b"reauth-response"))

        # the broker accepts: AUTH Success with no properties at all
        broker.send(auth_packet(RC_SUCCESS), segmented=True)
        pump(client)
        assert (RC_SUCCESS, None) in events["auth"]

        # the exchange is over, a new re-authentication can be started
        assert client.authenticate() == MQTTErrorCode.MQTT_ERR_SUCCESS
        pump(client)
        broker.expect_packet("second re-authenticate", auth_packet(RC_REAUTHENTICATE, AUTH_METHOD))

        # the broker rejects it: the explicit reason reaches the application
        broker.send(b"\xe0\x02\x87\x00")  # DISCONNECT, Not authorized
        pump(client)
        assert len(events["disconnect"]) == 1
        flags, reason_code, _ = events["disconnect"][0]
        assert flags.is_disconnect_packet_from_server
        assert reason_code.value == 0x87
        assert str(reason_code) == "Not authorized"
    finally:
        broker.close()


def test_authenticate_requires_v5_and_connection():
    client = mqtt.Client(CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv5)
    assert client.authenticate() == MQTTErrorCode.MQTT_ERR_NO_CONN

    client_v3 = mqtt.Client(CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv311)
    assert client_v3.authenticate() == MQTTErrorCode.MQTT_ERR_NOT_SUPPORTED


def test_auth_without_method():
    """Without an Authentication Method in CONNECT the client cannot
    authenticate, and a broker AUTH packet is a protocol error."""
    broker = FakeBroker()
    try:
        client, events = make_client(broker, auth_method=None)
        read_connect(broker)
        broker.send(CONNACK_SUCCESS)
        pump(client)
        assert len(events["connect"]) == 1

        assert client.authenticate() == MQTTErrorCode.MQTT_ERR_INVAL
        broker.expect_no_packet()

        # the broker must not send AUTH packets either
        broker.send(auth_packet(RC_CONTINUE, AUTH_METHOD, b"challenge"), segmented=True)
        pump(client)
        with pytest.raises(ConnectionError):
            broker.read_packet()
        assert len(events["disconnect"]) == 1
    finally:
        broker.close()
