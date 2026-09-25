# Tests for automatic MQTT v5.0 topic alias on outgoing publishes.

import struct
from types import SimpleNamespace

import paho.mqtt.client as client
import pytest
from paho.mqtt.enums import CallbackAPIVersion, MQTTErrorCode, MQTTProtocolVersion
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

import tests.paho_test as paho_test
from tests import mqtt5_props
from tests.testsupport.broker import FakeBroker, fake_broker  # noqa: F401

TOPIC_A = "building/floor1/room101/sensor/temperature"
TOPIC_B = "building/floor1/room102/sensor/humidity"
TOPIC_C = "building/floor2/room201/sensor/pressure"


def recv_exact(broker, num_bytes):
    data = b""
    while len(data) < num_bytes:
        chunk = broker.receive_packet(num_bytes - len(data))
        if not chunk:
            raise ConnectionError("broker connection closed unexpectedly")
        data += chunk
    return data


def read_packet(broker):
    """Read one MQTT packet from the wire, return (command byte, body)."""
    command = recv_exact(broker, 1)[0]
    remaining_length = 0
    multiplier = 1
    while True:
        byte = recv_exact(broker, 1)[0]
        remaining_length += (byte & 0x7F) * multiplier
        multiplier *= 128
        if not byte & 0x80:
            break
    return command, recv_exact(broker, remaining_length)


def decode_publish(command, body):
    dup = bool(command & 0x08)
    qos = (command & 0x06) >> 1
    topic_len = struct.unpack("!H", body[:2])[0]
    topic = body[2:2 + topic_len]
    pos = 2 + topic_len
    mid = None
    if qos > 0:
        mid = struct.unpack("!H", body[pos:pos + 2])[0]
        pos += 2
    properties = Properties(PacketTypes.PUBLISH)
    properties, props_len = properties.unpack(body[pos:])
    payload = body[pos + props_len:]
    return SimpleNamespace(
        dup=dup, qos=qos, topic=topic, mid=mid,
        properties=properties, payload=payload,
    )


class WireTopicTracker:
    """Reconstructs the effective topic of each PUBLISH exactly like a
    broker does, following the Topic Alias mappings seen on the wire."""

    def __init__(self):
        self.aliases = {}

    def resolve(self, publish):
        alias = getattr(publish.properties, "TopicAlias", None)
        if publish.topic:
            if alias is not None:
                self.aliases[alias] = publish.topic
            return publish.topic
        assert alias is not None, "PUBLISH with empty topic must carry a TopicAlias"
        return self.aliases[alias]


def gen_connack_alias_maximum(maximum, session_present=False):
    properties = mqtt5_props.gen_uint16_prop(
        mqtt5_props.PROP_TOPIC_ALIAS_MAXIMUM, maximum)
    return paho_test.gen_connack(
        flags=1 if session_present else 0, rc=0, proto_ver=5,
        properties=properties, property_helper=False)


def make_client(fake_broker):
    mqttc = client.Client(
        CallbackAPIVersion.VERSION2,
        "auto-topic-alias-test",
        protocol=MQTTProtocolVersion.MQTTv5,
        transport=fake_broker.transport,
    )
    mqttc.enable_auto_topic_alias()
    return mqttc


def handshake(mqttc, fake_broker, maximum, session_present=False):
    command, _ = read_packet(fake_broker)
    assert command >> 4 == 1  # CONNECT
    fake_broker.send_packet(
        gen_connack_alias_maximum(maximum, session_present=session_present))
    assert mqttc.loop(timeout=1) == MQTTErrorCode.MQTT_ERR_SUCCESS
    assert mqttc.is_connected()


def test_enable_auto_topic_alias_requires_v5():
    mqttc = client.Client(
        CallbackAPIVersion.VERSION2,
        "auto-topic-alias-v3",
        protocol=MQTTProtocolVersion.MQTTv311,
    )
    with pytest.raises(ValueError):
        mqttc.enable_auto_topic_alias()


def test_auto_topic_alias_no_quota(fake_broker: FakeBroker):
    # Broker grants no alias quota: full topics, no TopicAlias property.
    mqttc = make_client(fake_broker)
    mqttc.connect("localhost", fake_broker.port)
    try:
        fake_broker.start()
        handshake(mqttc, fake_broker, maximum=0)

        for topic in (TOPIC_A, TOPIC_A, TOPIC_B):
            mqttc.publish(topic, payload="x", qos=0)

        for topic in (TOPIC_A, TOPIC_A, TOPIC_B):
            command, body = read_packet(fake_broker)
            assert command >> 4 == 3  # PUBLISH
            pub = decode_publish(command, body)
            assert pub.topic == topic.encode("utf-8")
            assert not hasattr(pub.properties, "TopicAlias")
    finally:
        mqttc.disconnect()


def test_auto_topic_alias_manual_alias_precedence(fake_broker: FakeBroker):
    mqttc = make_client(fake_broker)
    mqttc.connect("localhost", fake_broker.port)
    try:
        fake_broker.start()
        handshake(mqttc, fake_broker, maximum=2)
        tracker = WireTopicTracker()

        mqttc.publish(TOPIC_A, payload="a-1", qos=0)
        mqttc.publish(TOPIC_B, payload="b-1", qos=0)
        mqttc.publish(TOPIC_A, payload="a-2", qos=0)

        wire = [decode_publish(*read_packet(fake_broker)) for _ in range(3)]
        assert wire[0].topic == TOPIC_A.encode("utf-8")
        alias_a = wire[0].properties.TopicAlias
        assert wire[2].topic == b""
        assert wire[2].properties.TopicAlias == alias_a
        for pub, topic in zip(wire, (TOPIC_A, TOPIC_B, TOPIC_A)):
            assert tracker.resolve(pub) == topic.encode("utf-8")

        # The application binds alias 1 to another topic by hand.
        manual = Properties(PacketTypes.PUBLISH)
        manual.TopicAlias = 1
        mqttc.publish(TOPIC_C, payload="c-1", qos=0, properties=manual)
        tracker.resolve(decode_publish(*read_packet(fake_broker)))

        # The automatic mapping that used alias 1 must be dropped: the next
        # automatic publish of TOPIC_A carries the full topic again and must
        # not reuse the manually owned alias.
        mqttc.publish(TOPIC_A, payload="a-3", qos=0)
        pub = decode_publish(*read_packet(fake_broker))
        assert pub.topic == TOPIC_A.encode("utf-8")
        assert pub.properties.TopicAlias != 1
        assert tracker.resolve(pub) == TOPIC_A.encode("utf-8")

        # The manual alias stays excluded from automatic assignment.
        mqttc.publish(TOPIC_B, payload="b-2", qos=0)
        pub = decode_publish(*read_packet(fake_broker))
        assert pub.topic == TOPIC_B.encode("utf-8")
        assert pub.properties.TopicAlias != 1
        assert tracker.resolve(pub) == TOPIC_B.encode("utf-8")
    finally:
        mqttc.disconnect()


def test_auto_topic_alias_qos1_reconnect(fake_broker: FakeBroker):
    mqttc = make_client(fake_broker)
    mqttc.connect("localhost", fake_broker.port)
    try:
        fake_broker.start()
        handshake(mqttc, fake_broker, maximum=2)
        tracker = WireTopicTracker()

        sent = []  # (topic, payload, mid) as passed to publish()

        def publish(topic, payload):
            info = mqttc.publish(topic, payload=payload, qos=1)
            sent.append((topic.encode("utf-8"), payload.encode("utf-8"), info.mid))

        # Three rotating topics with an alias quota of two.
        publish(TOPIC_A, "a-1")
        publish(TOPIC_B, "b-1")
        publish(TOPIC_A, "a-2")
        publish(TOPIC_C, "c-1")
        publish(TOPIC_A, "a-3")
        publish(TOPIC_B, "b-2")

        wire = [decode_publish(*read_packet(fake_broker)) for _ in sent]

        # The topic reconstructed from the wire always matches the publish().
        for pub, (topic, payload, mid) in zip(wire, sent):
            assert tracker.resolve(pub) == topic
            assert pub.payload == payload
            assert pub.mid == mid

        # First use of an alias carries the full topic, known mappings don't.
        alias_a = wire[0].properties.TopicAlias
        alias_b = wire[1].properties.TopicAlias
        assert wire[0].topic == TOPIC_A.encode("utf-8")
        assert wire[1].topic == TOPIC_B.encode("utf-8")
        assert alias_a != alias_b
        assert wire[2].topic == b""
        assert wire[2].properties.TopicAlias == alias_a
        # Quota exhausted: least recently used alias (TOPIC_B) is reassigned,
        # so the full topic goes out again.
        assert wire[3].topic == TOPIC_C.encode("utf-8")
        assert wire[3].properties.TopicAlias == alias_b
        assert wire[4].topic == b""
        assert wire[4].properties.TopicAlias == alias_a
        # TOPIC_B was evicted: full topic again on the reassigned alias.
        assert wire[5].topic == TOPIC_B.encode("utf-8")
        assert wire[5].properties.TopicAlias == alias_b

        # ACK everything but the last message.
        for (_, _, mid) in sent[:-1]:
            fake_broker.send_packet(paho_test.gen_puback(mid))
            assert mqttc.loop(timeout=1) == MQTTErrorCode.MQTT_ERR_SUCCESS
        unacked_mid = sent[-1][2]

        # Connection drops while the QoS 1 message is unacknowledged.
        fake_broker.drop()
        assert mqttc.loop(timeout=1) == MQTTErrorCode.MQTT_ERR_CONN_LOST

        mqttc.reconnect()
        fake_broker.start()
        # The MQTT session is resumed (session present), but topic aliases
        # belong to the connection and must be rebuilt.
        handshake(mqttc, fake_broker, maximum=2, session_present=True)
        tracker = WireTopicTracker()

        # The retransmitted message cannot rely on the old alias mapping:
        # the full topic must be sent even though an alias is assigned.
        command, body = read_packet(fake_broker)
        assert command >> 4 == 3
        dup_pub = decode_publish(command, body)
        assert dup_pub.dup
        assert dup_pub.mid == unacked_mid
        assert dup_pub.topic == TOPIC_B.encode("utf-8")
        assert tracker.resolve(dup_pub) == TOPIC_B.encode("utf-8")
        assert dup_pub.payload == b"b-2"

        fake_broker.send_packet(paho_test.gen_puback(unacked_mid))
        assert mqttc.loop(timeout=1) == MQTTErrorCode.MQTT_ERR_SUCCESS

        # New publishes rebuild the mapping on the new connection.
        publish(TOPIC_A, "a-4")
        publish(TOPIC_B, "b-3")

        pub_a = decode_publish(*read_packet(fake_broker))
        assert pub_a.topic == TOPIC_A.encode("utf-8")
        assert tracker.resolve(pub_a) == TOPIC_A.encode("utf-8")

        pub_b = decode_publish(*read_packet(fake_broker))
        assert pub_b.topic == b""
        assert pub_b.properties.TopicAlias == dup_pub.properties.TopicAlias
        assert tracker.resolve(pub_b) == TOPIC_B.encode("utf-8")

        mqttc.disconnect()
        mqttc.loop(timeout=1)
        command, _ = read_packet(fake_broker)
        assert command >> 4 == 14  # DISCONNECT
    finally:
        mqttc.disconnect()

