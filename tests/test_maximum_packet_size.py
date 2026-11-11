"""Tests for MQTT v5.0 Maximum Packet Size enforcement.

The broker limit announced in CONNACK bounds packets the client sends; the
client limit announced in CONNECT bounds packets it receives. The two must
never be mixed. These tests drive a Client with an in-memory socket so they
need no network binding.
"""

import struct

import pytest

from paho.mqtt import client
from paho.mqtt.enums import CallbackAPIVersion, MQTTErrorCode, MQTTProtocolVersion
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties
from tests import mqtt5_props, paho_test


class FakeSocket:
    """Minimal socket-like object backed by byte buffers."""

    def __init__(self, inbound=b""):
        self._inbound = bytearray(inbound)
        self.sent = bytearray()
        self.closed = False
        self.blocking = True

    def recv(self, bufsize):
        if not self._inbound:
            raise BlockingIOError()
        data = bytes(self._inbound[:bufsize])
        del self._inbound[:bufsize]
        return data

    def send(self, data):
        self.sent.extend(data)
        return len(data)

    def close(self):
        self.closed = True

    def fileno(self):
        return -1

    def setblocking(self, flag):
        self.blocking = flag


def make_client(connect_properties=None):
    mqttc = client.Client(
        CallbackAPIVersion.VERSION2,
        "mps-test",
        protocol=MQTTProtocolVersion.MQTTv5,
    )
    mqttc.connect_async("localhost", 1883, properties=connect_properties)
    sock = FakeSocket()
    mqttc._sock = sock
    mqttc._state = client._ConnectionState.MQTT_CS_CONNECTING
    return mqttc, sock


def connack_with_server_limit(max_packet_size=None):
    properties = b""
    if max_packet_size is not None:
        properties = mqtt5_props.gen_uint32_prop(
            mqtt5_props.PROP_MAXIMUM_PACKET_SIZE, max_packet_size
        )
    return paho_test.gen_connack(rc=0, proto_ver=5, properties=properties)


def feed_connack(mqttc, connack):
    mqttc._in_packet = {
        "command": 0,
        "have_remaining": 0,
        "remaining_count": [],
        "remaining_mult": 1,
        "remaining_length": 0,
        "packet": bytearray(b""),
        "to_process": 0,
        "pos": 0,
    }
    mqttc._sock._inbound.extend(connack)
    rc = mqttc._packet_read()
    assert rc == MQTTErrorCode.MQTT_ERR_SUCCESS
    assert mqttc._state == client._ConnectionState.MQTT_CS_CONNECTED


class TestOutgoingLimit:
    def test_server_limit_stored_from_connack(self):
        mqttc, _sock = make_client()
        feed_connack(mqttc, connack_with_server_limit(1024))
        assert mqttc._server_max_packet_size == 1024
        # Client receive limit must stay unset when not announced by us.
        assert mqttc._client_max_packet_size is None

    def test_missing_server_limit_means_no_limit(self):
        mqttc, _sock = make_client()
        feed_connack(mqttc, connack_with_server_limit(None))
        assert mqttc._server_max_packet_size is None
        info = mqttc.publish("t", b"x" * 4096)
        assert info.rc == MQTTErrorCode.MQTT_ERR_SUCCESS

    def test_qos0_oversized_publish_observable_failure(self):
        mqttc, _sock = make_client()
        feed_connack(mqttc, connack_with_server_limit(64))

        # A small message is accepted.
        ok = mqttc.publish("t", b"x")
        assert ok.rc == MQTTErrorCode.MQTT_ERR_SUCCESS

        # Full packet (fixed header + VBI + variable header + props + payload)
        # exceeds 64 bytes; the error must be visible on MQTTMessageInfo and
        # wait_for_publish() must raise instead of hanging.
        info = mqttc.publish("t", b"y" * 128)
        assert info.rc == MQTTErrorCode.MQTT_ERR_PAYLOAD_SIZE
        with pytest.raises(RuntimeError):
            info.wait_for_publish(1)

        # The oversized packet must never have reached the socket, while the
        # valid one was sent.
        assert b"y" * 128 not in bytes(mqttc._sock.sent)
        assert b"x" in bytes(mqttc._sock.sent)

    def test_qos1_oversized_does_not_break_later_packet_ids(self):
        mqttc, _sock = make_client()
        feed_connack(mqttc, connack_with_server_limit(64))

        bad = mqttc.publish("t", b"z" * 128, qos=1)
        assert bad.rc == MQTTErrorCode.MQTT_ERR_PAYLOAD_SIZE
        with pytest.raises(RuntimeError):
            bad.wait_for_publish(1)

        # Failed message leaves the store; no ack is expected for its mid.
        assert bad.mid not in mqttc._out_messages
        assert mqttc._inflight_messages == 0

        # A later valid QoS1 message gets a fresh mid and flows normally.
        good = mqttc.publish("t", b"ok", qos=1)
        assert good.rc == MQTTErrorCode.MQTT_ERR_SUCCESS
        assert good.mid not in (bad.mid,)
        assert good.mid in mqttc._out_messages
        sent = bytes(mqttc._sock.sent)
        assert struct.pack("!H", good.mid) in sent

        # Its acknowledgement completes cleanly.
        puback = paho_test.gen_puback(good.mid, proto_ver=5)
        mqttc._sock._inbound.extend(puback)
        assert mqttc._packet_read() == MQTTErrorCode.MQTT_ERR_SUCCESS
        assert good.is_published()
        assert good.mid not in mqttc._out_messages

    def test_queued_before_connect_rechecked_after_connack(self):
        mqttc, sock = make_client()
        # Before the TCP connection exists, publish() only stores the message.
        mqttc._sock = None
        queued = mqttc.publish("t", b"q" * 128, qos=1)
        assert queued.rc == MQTTErrorCode.MQTT_ERR_NO_CONN
        assert queued.mid in mqttc._out_messages
        mqttc._sock = sock

        # Broker announces a small limit: the queued message must fail instead
        # of being transmitted.
        feed_connack(mqttc, connack_with_server_limit(64))
        assert queued.rc == MQTTErrorCode.MQTT_ERR_PAYLOAD_SIZE
        with pytest.raises(RuntimeError):
            queued.wait_for_publish(1)
        assert queued.mid not in mqttc._out_messages
        assert b"q" * 128 not in bytes(mqttc._sock.sent)

    def test_smaller_limit_after_reconnect(self):
        mqttc, _sock = make_client()
        feed_connack(mqttc, connack_with_server_limit(4096))
        first = mqttc.publish("t", b"a" * 256, qos=1)
        assert first.rc == MQTTErrorCode.MQTT_ERR_SUCCESS

        # Simulate reconnect: reset state, pretend a new CONNECT/CONNACK with a
        # smaller limit arrives.
        mqttc._state = client._ConnectionState.MQTT_CS_CONNECTING
        feed_connack(mqttc, connack_with_server_limit(64))
        assert mqttc._server_max_packet_size == 64

        second = mqttc.publish("t", b"b" * 256, qos=1)
        assert second.rc == MQTTErrorCode.MQTT_ERR_PAYLOAD_SIZE
        assert second.mid not in mqttc._out_messages

    def test_limit_counts_full_packet_not_payload_only(self):
        mqttc, _sock = make_client()
        # Limit chosen so the payload alone fits but fixed header, VBI, topic
        # and properties push the full packet over the limit.
        feed_connack(mqttc, connack_with_server_limit(20))
        info = mqttc.publish("some/long/topic", b"12345")
        assert info.rc == MQTTErrorCode.MQTT_ERR_PAYLOAD_SIZE

    def test_repeated_failures_keep_successive_mids_distinct(self):
        mqttc, _sock = make_client()
        feed_connack(mqttc, connack_with_server_limit(64))

        mids = set()
        for _ in range(3):
            bad = mqttc.publish("t", b"z" * 256, qos=1)
            assert bad.rc == MQTTErrorCode.MQTT_ERR_PAYLOAD_SIZE
            assert bad.mid not in mids
            mids.add(bad.mid)
        assert mqttc._inflight_messages == 0
        assert not mqttc._out_messages

        good = mqttc.publish("t", b"ok", qos=1)
        assert good.rc == MQTTErrorCode.MQTT_ERR_SUCCESS
        assert good.mid not in mids
        assert mqttc._inflight_messages == 1

    def test_publish_properties_count_toward_packet_size(self):
        mqttc, _sock = make_client()
        feed_connack(mqttc, connack_with_server_limit(64))

        props = Properties(PacketTypes.PUBLISH)
        props.ContentType = "application/octet-stream"
        # Payload alone fits in 64 bytes but the full packet does not.
        info = mqttc.publish("t", b"1" * 40, properties=props)
        assert info.rc == MQTTErrorCode.MQTT_ERR_PAYLOAD_SIZE

    def test_vbi_boundary_is_counted(self):
        # Remaining Length 127 encodes in 1 byte, 128 needs 2 bytes. Build a
        # limit equal to the full size of a packet whose remaining length is
        # exactly 127, then make it one byte larger and confirm the rejection
        # accounts for the extra VBI byte.
        mqttc, _sock = make_client()
        feed_connack(mqttc, connack_with_server_limit(129))
        # PUBLISH: 1 fixed + 1 VBI + (2 + topic) + 1 props + payload = 129
        # remaining length = 127 -> full size 129
        topic = "t"
        payload_len = 127 - 2 - len(topic) - 1
        fitting = mqttc.publish(topic, b"p" * payload_len)
        assert fitting.rc == MQTTErrorCode.MQTT_ERR_SUCCESS
        assert len(mqttc._sock.sent) == 129

        over = mqttc.publish(topic, b"p" * (payload_len + 1))
        assert over.rc == MQTTErrorCode.MQTT_ERR_PAYLOAD_SIZE


class TestIncomingLimit:
    def _client_with_receive_limit(self, limit):
        connect_properties = Properties(PacketTypes.CONNECT)
        connect_properties.MaximumPacketSize = limit
        mqttc, _sock = make_client(connect_properties)
        assert mqttc._client_max_packet_size == limit
        feed_connack(mqttc, connack_with_server_limit(None))
        return mqttc

    def test_within_limit_accepted(self):
        mqttc = self._client_with_receive_limit(256)
        packet = paho_test.gen_publish(
            "t", 0, payload=b"hello", proto_ver=5
        )
        mqttc._sock._inbound.extend(packet)
        assert mqttc._packet_read() == MQTTErrorCode.MQTT_ERR_SUCCESS

    def test_oversized_length_rejected_without_body_buffer(self):
        mqttc = self._client_with_receive_limit(64)

        # Advertise a 1 MiB remaining length but never send the body. The
        # client must reject the packet based purely on the length, without
        # allocating a buffer for the advertised body.
        header = bytes([0x30])
        remaining_length = 1024 * 1024
        vbi = mqtt5_props.pack_varint(remaining_length)
        mqttc._sock._inbound.extend(header + vbi)

        rc = mqttc._packet_read()
        assert rc == MQTTErrorCode.MQTT_ERR_PROTOCOL
        # No body bytes accumulated: only the length was inspected.
        assert len(mqttc._in_packet["packet"]) == 0

    def test_incoming_uses_client_limit_not_server_limit(self):
        # Broker says it will send up to 4096; we only accept 64. A 128-byte
        # incoming packet must be rejected because the client limit governs.
        mqttc = self._client_with_receive_limit(64)
        assert mqttc._server_max_packet_size is None

        packet = paho_test.gen_publish(
            "t", 0, payload=b"x" * 128, proto_ver=5
        )
        mqttc._sock._inbound.extend(packet)
        assert mqttc._packet_read() == MQTTErrorCode.MQTT_ERR_PROTOCOL
