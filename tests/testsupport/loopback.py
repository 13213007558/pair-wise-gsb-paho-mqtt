"""In-memory MQTT transport for enhanced authentication tests.

Unlike `tests.testsupport.broker.FakeBroker` this helper does not bind or
listen on a TCP port (which is not permitted in every sandbox). It connects a
real `paho.mqtt.client.Client` to one end of a `socket.socketpair` and drives
the other end directly, so the full packet read/decode/write stack is exercised
without any network listener.
"""
from __future__ import annotations

import socket
import struct
from typing import Any

from paho.mqtt.enums import CallbackAPIVersion, MQTTProtocolVersion
from paho.mqtt.properties import Properties
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.reasoncodes import ReasonCode
from paho.mqtt import client as paho_client


class LoopbackBroker:
    """User-space peer speaking raw MQTT over one end of a socket pair."""

    def __init__(self, client_sock: socket.socket, broker_sock: socket.socket):
        self.sock = broker_sock
        self._client_sock = client_sock
        self.sock.setblocking(False)
        self._buffer = b""

    # Raw IO --------------------------------------------------------------
    def send_raw(self, data: bytes) -> None:
        self.sock.sendall(data)

    def recv_raw(self, length: int = 65536) -> bytes:
        return self.sock.recv(length)

    # Packet framing ------------------------------------------------------
    def send_packet(self, packet: bytes) -> None:
        self.send_raw(packet)

    def read_packet(self, client: paho_client.Client | None = None) -> dict[str, Any]:
        packet = self._read_until_complete(client)
        return parse_packet(packet)

    def _read_until_complete(self, client: paho_client.Client | None) -> bytes:
        header = self._read_n(1, client)
        remaining_bytes = b""
        multiplier = 1
        remaining_length = 0
        for _ in range(4):
            byte = self._read_n(1, client)[0]
            remaining_bytes += bytes([byte])
            remaining_length += (byte & 127) * multiplier
            multiplier *= 128
            if not (byte & 128):
                break
        body = self._read_n(remaining_length, client)
        return header + remaining_bytes + body

    def _read_n(self, n: int, client: paho_client.Client | None) -> bytes:
        while len(self._buffer) < n:
            try:
                chunk = self.sock.recv(65536)
            except BlockingIOError:
                chunk = b""
            if not chunk:
                if client is not None:
                    client.loop_write()
                    try:
                        chunk = self.sock.recv(65536)
                    except BlockingIOError:
                        chunk = b""
                if not chunk:
                    raise AssertionError("peer closed before enough bytes arrived")
            self._buffer += chunk
        out = self._buffer[:n]
        self._buffer = self._buffer[n:]
        return out

    def close(self) -> None:
        self.sock.close()
        self._client_sock.close()


def make_loopback(
    auth_controller=None,
    *,
    userdata: Any = None,
) -> tuple[paho_client.Client, LoopbackBroker]:
    """Create a client wired to an in-memory broker, ready to connect."""
    client_sock, broker_sock = socket.socketpair()
    mqttc = paho_client.Client(
        CallbackAPIVersion.VERSION2,
        client_id="loopback-client",
        protocol=MQTTProtocolVersion.MQTTv5,
        userdata=userdata,
    )
    if auth_controller is not None:
        mqttc.auth_controller = auth_controller
    # Pretend the TCP connection already exists; reconnect() would call
    # _create_socket(), which we bypass deliberately.
    mqttc._sock = client_sock
    client_sock.setblocking(False)
    mqttc._state = paho_client._ConnectionState.MQTT_CS_CONNECTING
    mqttc._sock.setblocking(False)
    broker = LoopbackBroker(client_sock, broker_sock)
    return mqttc, broker


# ---------------------------------------------------------------------------
# MQTT v5 packet builders / parser (auth-focused, no external broker needed)
# ---------------------------------------------------------------------------

def _remaining_length(length: int) -> bytes:
    out = b""
    while True:
        byte = length % 128
        length //= 128
        if length:
            byte |= 128
        out += bytes([byte])
        if not length:
            return out


def _frame(command: int, body: bytes) -> bytes:
    return bytes([command]) + _remaining_length(len(body)) + body


def _auth_props(method: str | None = None, data: bytes | None = None) -> bytes:
    props = Properties(PacketTypes.AUTH)
    if method is not None:
        props.AuthenticationMethod = method
    if data is not None:
        props.AuthenticationData = data
    return props.pack()


def build_auth(
    reason: int,
    method: str | None = None,
    data: bytes | None = None,
) -> bytes:
    body = bytes([reason]) + _auth_props(method, data)
    return _frame(0xF0, body)


def build_connack(
    reason: int = 0,
    method: str | None = None,
    data: bytes | None = None,
) -> bytes:
    props = Properties(PacketTypes.CONNACK)
    if method is not None:
        props.AuthenticationMethod = method
    if data is not None:
        props.AuthenticationData = data
    packed = props.pack()
    body = bytes([0, reason]) + packed
    return _frame(0x20, body)


def build_disconnect(reason: int = 0) -> bytes:
    if reason == 0:
        return _frame(0xE0, b"")
    return _frame(0xE0, bytes([reason]))


def parse_packet(packet: bytes) -> dict[str, Any]:
    command = packet[0] & 0xF0
    idx = 1
    remaining_length = 0
    multiplier = 1
    for _ in range(4):
        byte = packet[idx]
        idx += 1
        remaining_length += (byte & 127) * multiplier
        multiplier *= 128
        if not (byte & 128):
            break
    body = packet[idx:idx + remaining_length]
    parsed: dict[str, Any] = {"command": command, "body": body}

    if command == 0x10:  # CONNECT
        parsed["connect"] = _parse_connect(body)
    elif command == 0xF0:  # AUTH
        parsed["auth"] = _parse_auth_like(body, PacketTypes.AUTH)
    elif command == 0x20:  # CONNACK
        parsed["connack"] = _parse_connack(body)
    elif command == 0xE0:  # DISCONNECT
        parsed["disconnect"] = {"reason": body[0] if body else 0}
    return parsed


def _parse_connect(body: bytes) -> dict[str, Any]:
    # Variable header starts with the protocol name: length prefix + name,
    # then protocol level, connect flags and keep alive (3 bytes).
    proto_len = struct.unpack("!H", body[:2])[0]
    pos = 2 + proto_len + 1 + 1 + 2
    props = Properties(PacketTypes.CONNECT)
    props.unpack(body[pos:])
    out: dict[str, Any] = {"properties": props}
    if hasattr(props, "AuthenticationMethod"):
        out["method"] = props.AuthenticationMethod
    if hasattr(props, "AuthenticationData"):
        out["data"] = props.AuthenticationData
    return out


def _parse_auth_like(body: bytes, packet_type: int) -> dict[str, Any]:
    reason = body[0]
    props = Properties(packet_type)
    if len(body) > 1:
        props.unpack(body[1:])
    out: dict[str, Any] = {
        "reason": reason,
        "reason_code": ReasonCode(packet_type, identifier=reason),
        "properties": props,
    }
    if hasattr(props, "AuthenticationMethod"):
        out["method"] = props.AuthenticationMethod
    if hasattr(props, "AuthenticationData"):
        out["data"] = props.AuthenticationData
    return out


def _parse_connack(body: bytes) -> dict[str, Any]:
    reason = body[1]
    props = Properties(PacketTypes.CONNACK)
    if len(body) > 2:
        props.unpack(body[2:])
    return {"reason": reason, "properties": props}
