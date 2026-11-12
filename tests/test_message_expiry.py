# Copyright (c) 2026 Roger Light and others
#
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Eclipse Public License v2.0
# and Eclipse Distribution License v1.0 which accompany this distribution.
"""Tests for the local lifetime of MQTT v5.0 outgoing messages.

The Message Expiry Interval of a message starts when the message enters the
local outgoing queue, not when it eventually reaches the broker. These tests
drive the clock, the (dis)connection state and the acknowledgement packets
themselves, so they need no network access: a fake socket captures the
encoded packets and a controllable clock stands in for ``time.monotonic``.
"""
import struct

import pytest

from paho.mqtt import _packet, client
from paho.mqtt.enums import (
    CallbackAPIVersion,
    MessageState,
    MQTTErrorCode,
    MQTTProtocolVersion,
)
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties, VariableByteIntegers


class FakeSocket:
    """Minimal socket stand-in. ``send`` either buffers or raises
    ``BlockingIOError`` so packets stay inspectable in ``_out_packet``."""

    def __init__(self, block: bool = False):
        self.block = block
        self.sent = bytearray()
        self.closed = False

    def send(self, data):
        if self.block:
            raise BlockingIOError
        self.sent.extend(data)
        return len(data)

    def recv(self, buffer_size):
        return b""

    def close(self):
        self.closed = True

    def fileno(self):
        return -1

    def setblocking(self, flag):
        self.block = not flag


class ControlledClock:
    def __init__(self, start: float = 1000.0):
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def mqtt_client(monkeypatch):
    clock = ControlledClock()
    monkeypatch.setattr(client, "time_func", clock)
    c = client.Client(
        CallbackAPIVersion.VERSION2,
        "message-expiry",
        protocol=MQTTProtocolVersion.MQTTv5,
    )
    # Avoid the internal socketpair (which binds a real TCP port) and any
    # interaction with an external event loop.
    c._sockpairW = None
    c._sock = FakeSocket()
    c._state = client._ConnectionState.MQTT_CS_CONNECTED
    yield c, clock
    c._sock = None


def expiry_properties(interval: int) -> Properties:
    props = Properties(PacketTypes.PUBLISH)
    props.MessageExpiryInterval = interval
    return props


def decode_last_publish(c: client.Client):
    assert c._out_packet, "no packet was queued"
    packet = bytes(c._out_packet[-1]["packet"])
    header = packet[0]
    remaining_length, vbi = VariableByteIntegers.decode(packet[1:])
    body = packet[1 + vbi:]
    result = _packet.decode_publish(header, body, MQTTProtocolVersion.MQTTv5)
    return result, header


def drain_packets(c: client.Client) -> None:
    """Simulate the socket accepting every queued packet."""
    c._sock.block = False
    assert c.loop_write() == MQTTErrorCode.MQTT_ERR_SUCCESS


def settled_with_expiry(info: client.MQTTMessageInfo) -> bool:
    """An expired message is settled (``wait_for_publish`` cannot block) but
    carries the expiry error, which ``is_published`` reports by raising."""
    try:
        info.is_published()
    except RuntimeError:
        return info.rc == MQTTErrorCode.MQTT_ERR_MSG_EXPIRED
    return False


def test_expiry_time_is_stamped_from_queue_entry(mqtt_client):
    c, clock = mqtt_client
    props = expiry_properties(30)

    info = c.publish("topic", b"payload", qos=1, properties=props)
    assert info.rc == MQTTErrorCode.MQTT_ERR_SUCCESS

    message = c._out_messages[info.mid]
    assert message.expiry_time == pytest.approx(clock.now + 30)
    # The Properties object supplied by the caller must not be rewritten.
    assert props.MessageExpiryInterval == 30


def test_message_expiring_in_queue_is_never_sent(mqtt_client):
    c, clock = mqtt_client
    # The message cannot be written immediately, so it stays in the queue.
    c._sock.block = True

    info = c.publish("topic", b"payload", qos=1, properties=expiry_properties(10))
    mid = info.mid
    assert mid in c._out_messages

    # Connection drops before anything is transmitted.
    c._sock = None
    clock.advance(11)

    # Reconnect bookkeeping must drop the stale message instead of resending.
    c._messages_reconnect_reset_out()

    assert mid not in c._out_messages
    assert c._inflight_messages == 0
    assert info.rc == MQTTErrorCode.MQTT_ERR_MSG_EXPIRED
    with pytest.raises(RuntimeError, match="Expiry"):
        info.wait_for_publish()


def test_reencoded_packet_carries_only_remaining_interval(mqtt_client):
    c, clock = mqtt_client
    props = expiry_properties(10)

    info = c.publish("topic", b"payload", qos=1, properties=props)
    mid = info.mid
    drain_packets(c)
    assert len(c._out_packet) == 0

    # Lose the connection while the PUBACK is still outstanding, then wait.
    c._sock = None
    clock.advance(4)
    c._messages_reconnect_reset_out()
    message = c._out_messages[mid]
    assert message.state == MessageState.MQTT_MS_PUBLISH
    assert message.dup is True

    # Reconnect and let the message go out again. Use a blocking socket so
    # the encoded packet stays in the outgoing queue for inspection.
    c._sock = FakeSocket(block=True)
    c._inflight_messages = 0
    # Emulate the connack resend pass directly for this single message.
    message.state = MessageState.MQTT_MS_PUBLISH
    with c._out_message_mutex:
        rc = c._send_publish(
            message.mid,
            b"topic",
            message.payload,
            message.qos,
            message.retain,
            message.dup,
            properties=message.properties,
            expiry_time=message.expiry_time,
        )
    assert rc == MQTTErrorCode.MQTT_ERR_SUCCESS

    result, header = decode_last_publish(c)
    assert result.mid == mid  # packet identifier is retained, not leaked/reused
    assert (header & 0x08) == 0x08  # DUP flag marks a retransmission
    assert result.properties.MessageExpiryInterval == 6  # 10 - 4 elapsed


def test_remaining_interval_never_advertises_zero(mqtt_client):
    c, clock = mqtt_client
    props = expiry_properties(10)
    info = c.publish("topic", b"payload", qos=1, properties=props)
    mid = info.mid
    drain_packets(c)

    c._sock = None
    clock.advance(9.5)  # only half a second of life remains
    c._messages_reconnect_reset_out()
    message = c._out_messages[mid]

    c._sock = FakeSocket(block=True)
    rc = c._send_publish(
        message.mid, b"topic", message.payload, message.qos,
        message.retain, message.dup, properties=message.properties,
        expiry_time=message.expiry_time,
    )
    assert rc == MQTTErrorCode.MQTT_ERR_SUCCESS
    result, _ = decode_last_publish(c)
    assert result.properties.MessageExpiryInterval == 1


def test_expired_message_is_dropped_before_reconnect_send(mqtt_client):
    c, clock = mqtt_client
    props = expiry_properties(5)
    info = c.publish("topic", b"payload", qos=1, properties=expiry_properties(5))
    mid = info.mid
    drain_packets(c)

    c._sock = None
    clock.advance(5)  # exactly at the deadline
    c._messages_reconnect_reset_out()

    assert mid not in c._out_messages
    assert info.rc == MQTTErrorCode.MQTT_ERR_MSG_EXPIRED
    assert settled_with_expiry(info)
    with pytest.raises(RuntimeError):
        info.wait_for_publish()


def test_inflight_qos2_kept_until_pubcomp_after_broker_ack(mqtt_client):
    c, clock = mqtt_client
    props = expiry_properties(10)
    info = c.publish("topic", b"payload", qos=2, properties=props)
    mid = info.mid
    drain_packets(c)
    assert c._out_messages[mid].state == MessageState.MQTT_MS_WAIT_FOR_PUBREC

    # Broker acknowledges receipt with PUBREC: the packet identifier is now
    # committed on the broker side.
    c._in_packet["command"] = PacketTypes.PUBREC
    c._in_packet["packet"] = struct.pack("!H", mid)
    assert c._handle_pubrec() == MQTTErrorCode.MQTT_ERR_SUCCESS
    assert c._out_messages[mid].state == MessageState.MQTT_MS_WAIT_FOR_PUBCOMP

    # Even if the local lifetime now runs out, the PUBCOMP handshake must
    # still complete rather than dropping the message (and its packet id).
    clock.advance(50)
    c._expire_out_messages()
    assert mid in c._out_messages
    assert c._out_messages[mid].state == MessageState.MQTT_MS_WAIT_FOR_PUBCOMP

    # Completing the handshake settles the message normally.
    c._in_packet["command"] = PacketTypes.PUBCOMP
    c._in_packet["packet"] = struct.pack("!H", mid)
    assert c._handle_pubackcomp("PUBCOMP") == MQTTErrorCode.MQTT_ERR_SUCCESS
    assert mid not in c._out_messages
    assert info.is_published() is True
    assert info.rc == MQTTErrorCode.MQTT_ERR_SUCCESS


def test_inflight_qos1_expired_before_puback_is_retired(mqtt_client):
    c, clock = mqtt_client
    props = expiry_properties(10)
    info = c.publish("topic", b"payload", qos=1, properties=props)
    mid = info.mid
    drain_packets(c)
    assert c._inflight_messages == 1

    clock.advance(11)
    c._expire_out_messages()

    assert mid not in c._out_messages
    assert c._inflight_messages == 0
    assert info.rc == MQTTErrorCode.MQTT_ERR_MSG_EXPIRED


def test_packet_identifiers_are_not_reused_after_expiry(mqtt_client):
    c, clock = mqtt_client
    first = c.publish("a", b"1", qos=1, properties=expiry_properties(10))
    drain_packets(c)
    expired_mid = first.mid

    clock.advance(11)
    c._expire_out_messages()
    assert expired_mid not in c._out_messages

    second = c.publish("b", b"2", qos=1, properties=expiry_properties(10))
    assert second.mid != expired_mid
    assert second.mid > expired_mid


def test_caller_can_distinguish_three_outcomes(mqtt_client):
    c, clock = mqtt_client

    # 1. Success: PUBACK settles the message.
    ok = c.publish("ok", b"1", qos=1, properties=expiry_properties(30))
    drain_packets(c)
    c._in_packet["command"] = PacketTypes.PUBACK
    c._in_packet["packet"] = struct.pack("!H", ok.mid)
    c._handle_pubackcomp("PUBACK")
    assert ok.is_published() is True
    assert ok.rc == MQTTErrorCode.MQTT_ERR_SUCCESS
    ok.wait_for_publish()  # returns without raising

    # 2. Expired before sending: settled with an explicit error.
    expired = c.publish("exp", b"2", qos=1, properties=expiry_properties(5))
    drain_packets(c)
    clock.advance(6)
    c._expire_out_messages()
    assert expired.rc == MQTTErrorCode.MQTT_ERR_MSG_EXPIRED
    assert settled_with_expiry(expired)
    with pytest.raises(RuntimeError):
        expired.wait_for_publish()

    # 3. Still waiting for an acknowledgement: not published, no error. The
    # condition flag remains unset so wait_for_publish cannot return early;
    # the frozen fake clock would also suspend its timeout loop, so assert the
    # underlying "still waiting" state directly rather than entering it.
    waiting = c.publish("wait", b"3", qos=1, properties=expiry_properties(30))
    drain_packets(c)
    assert waiting.rc == MQTTErrorCode.MQTT_ERR_SUCCESS
    assert waiting.is_published() is False
    with waiting._condition:
        assert waiting._published is False

    # Receiving the PUBACK then settles the message.
    c._in_packet["command"] = PacketTypes.PUBACK
    c._in_packet["packet"] = struct.pack("!H", waiting.mid)
    c._handle_pubackcomp("PUBACK")
    assert waiting.is_published() is True
    waiting.wait_for_publish()  # returns immediately once published


def test_zero_interval_means_no_local_expiry(mqtt_client):
    c, clock = mqtt_client
    props = expiry_properties(0)
    info = c.publish("topic", b"payload", qos=1, properties=props)
    assert c._out_messages[info.mid].expiry_time is None

    clock.advance(10_000)
    c._expire_out_messages()
    assert info.mid in c._out_messages


def test_mqtt3_messages_have_no_local_expiry(mqtt_client):
    c, clock = mqtt_client
    c._protocol = MQTTProtocolVersion.MQTTv311
    info = c.publish("topic", b"payload", qos=1)
    assert c._out_messages[info.mid].expiry_time is None
    clock.advance(10_000)
    c._expire_out_messages()
    assert info.mid in c._out_messages


def test_queued_message_expiring_while_inflight_slot_is_full(mqtt_client):
    c, clock = mqtt_client
    # The client must look unconnected while the inflight limit is changed.
    c._state = client._ConnectionState.MQTT_CS_NEW
    c.max_inflight_messages_set(1)
    c._state = client._ConnectionState.MQTT_CS_CONNECTED

    first = c.publish("a", b"1", qos=1, properties=expiry_properties(30))
    drain_packets(c)
    assert first.rc == MQTTErrorCode.MQTT_ERR_SUCCESS

    # A second message stays queued because the single slot is occupied.
    second = c.publish("b", b"2", qos=1, properties=expiry_properties(10))
    assert c._out_messages[second.mid].state == MessageState.MQTT_MS_QUEUED

    clock.advance(11)
    # The periodic sweep removes the stale queued message without touching
    # the in-flight message or its counter.
    c._expire_out_messages()
    assert second.mid not in c._out_messages
    assert first.mid in c._out_messages
    assert c._inflight_messages == 1
    assert second.rc == MQTTErrorCode.MQTT_ERR_MSG_EXPIRED


def test_loop_misc_sweeps_expired_inflight_message(mqtt_client):
    c, clock = mqtt_client
    info = c.publish("topic", b"payload", qos=1, properties=expiry_properties(10))
    mid = info.mid
    drain_packets(c)
    assert c._inflight_messages == 1

    clock.advance(11)
    assert c.loop_misc() == MQTTErrorCode.MQTT_ERR_SUCCESS
    assert mid not in c._out_messages
    assert c._inflight_messages == 0
    assert info.rc == MQTTErrorCode.MQTT_ERR_MSG_EXPIRED


def test_first_send_advertises_full_remaining_interval(mqtt_client):
    c, clock = mqtt_client
    c._sock = FakeSocket(block=True)
    info = c.publish("topic", b"payload", qos=1, properties=expiry_properties(10))
    result, _ = decode_last_publish(c)
    assert result.properties.MessageExpiryInterval == 10
    # No time has elapsed, but the wire value must never exceed the original.
    assert result.properties.MessageExpiryInterval <= 10


def test_expired_message_is_not_republished_after_reconnect(mqtt_client):
    c, clock = mqtt_client
    c._sock = FakeSocket(block=True)
    info = c.publish("topic", b"payload", qos=1, properties=expiry_properties(10))
    mid = info.mid
    assert mid in c._out_messages

    # Simulate an outage that outlasts the message lifetime.
    c._sock = None
    clock.advance(20)
    c._messages_reconnect_reset_out()

    c._sock = FakeSocket(block=True)
    # Nothing eligible for resend means no new PUBLISH packet is produced.
    before = len(c._out_packet)
    assert mid not in c._out_messages
    assert len(c._out_packet) == before
    assert info.rc == MQTTErrorCode.MQTT_ERR_MSG_EXPIRED


def test_unexpired_message_survives_reconnect_and_keeps_identifier(mqtt_client):
    c, clock = mqtt_client
    c._sock = FakeSocket(block=True)
    info = c.publish("topic", b"payload", qos=1, properties=expiry_properties(10))
    mid = info.mid

    c._sock = None
    clock.advance(3)
    c._messages_reconnect_reset_out()

    assert mid in c._out_messages
    message = c._out_messages[mid]
    assert message.state == MessageState.MQTT_MS_PUBLISH

    c._sock = FakeSocket(block=True)
    rc = c._send_publish(
        message.mid, b"topic", message.payload, message.qos,
        message.retain, message.dup, properties=message.properties,
        expiry_time=message.expiry_time,
    )
    assert rc == MQTTErrorCode.MQTT_ERR_SUCCESS
    result, header = decode_last_publish(c)
    assert result.mid == mid
    assert result.properties.MessageExpiryInterval == 7
    assert (header & 0x08) == 0x08  # DUP set on the retransmission
