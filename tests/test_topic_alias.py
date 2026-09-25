"""Tests for automatic MQTT v5 topic alias support (client side).

These tests use the FakeBroker and decode PUBLISH packets straight from
the wire, reconstructing the topic through a broker-side alias table.
The reconstructed topic must always match the topic passed to publish().
"""

import struct

import paho.mqtt.client as client
import tests.mqtt5_props as mqtt5_props
import tests.paho_test as paho_test
from paho.mqtt.enums import CallbackAPIVersion, MQTTProtocolVersion
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

# Import test fixture
from tests.testsupport.broker import FakeBroker, fake_broker  # noqa: F401


def _recv_exactly(broker, count):
    data = b""
    while len(data) < count:
        chunk = broker.receive_packet(count - len(data))
        if not chunk:
            raise ConnectionError("broker connection closed")
        data += chunk
    return data


def _decode_varint(data, pos):
    multiplier = 1
    value = 0
    while True:
        byte = data[pos]
        pos += 1
        value += (byte & 0x7F) * multiplier
        if not byte & 0x80:
            return value, pos
        multiplier *= 128


class WirePublish:
    def __init__(self, dup, qos, retain, topic, mid, alias, payload):
        self.dup = dup
        self.qos = qos
        self.retain = retain
        self.topic = topic
        self.mid = mid
        self.alias = alias
        self.payload = payload


def read_publish(broker):
    """Read one PUBLISH packet from the wire and decode it."""
    header = _recv_exactly(broker, 1)[0]
    assert header >> 4 == 3, f"expected PUBLISH, got command {header >> 4}"
    dup = bool(header & 0x08)
    qos = (header >> 1) & 0x03
    retain = bool(header & 0x01)

    multiplier = 1
    remaining = 0
    while True:
        byte = _recv_exactly(broker, 1)[0]
        remaining += (byte & 0x7F) * multiplier
        if not byte & 0x80:
            break
        multiplier *= 128

    body = _recv_exactly(broker, remaining)
    topic_len = struct.unpack("!H", body[:2])[0]
    topic = body[2:2 + topic_len]
    pos = 2 + topic_len

    mid = None
    if qos > 0:
        mid = struct.unpack("!H", body[pos:pos + 2])[0]
        pos += 2

    props_len, pos = _decode_varint(body, pos)
    props = body[pos:pos + props_len]
    pos += props_len

    alias = None
    ppos = 0
    while ppos < len(props):
        ident = props[ppos]
        ppos += 1
        if ident == mqtt5_props.PROP_TOPIC_ALIAS:
            alias = struct.unpack("!H", props[ppos:ppos + 2])[0]
            ppos += 2
        else:
            raise AssertionError(f"unexpected property identifier {ident}")

    return WirePublish(dup, qos, retain, topic, mid, alias, body[pos:])


class AliasResolver:
    """Broker-side topic alias table, valid for one connection only."""

    def __init__(self):
        self.aliases = {}

    def resolve(self, publish):
        if publish.topic:
            if publish.alias is not None:
                self.aliases[publish.alias] = publish.topic
            return publish.topic
        assert publish.alias is not None, "PUBLISH without topic and without alias"
        return self.aliases[publish.alias]


def _connect_and_grant_aliases(broker, alias_maximum):
    """Accept the client connection and return a CONNACK granting (or not)
    an outgoing topic alias quota."""
    broker.start()
    connect_packet = broker.receive_packet(1000)
    assert connect_packet  # Check connection was not closed
    properties = b""
    if alias_maximum:
        properties = mqtt5_props.gen_uint16_prop(
            mqtt5_props.PROP_TOPIC_ALIAS_MAXIMUM, alias_maximum)
    connack_packet = paho_test.gen_connack(
        rc=0, proto_ver=5, properties=properties, property_helper=False)
    count = broker.send_packet(connack_packet)
    assert count == len(connack_packet)


def test_auto_alias_rotation_with_quota_two(fake_broker: FakeBroker):
    """Quota of 2 aliases, 3 rotating topics: aliases are re-bound (LRU)
    and every first use / re-bind carries the full topic on the wire."""
    mqttc = client.Client(
        CallbackAPIVersion.VERSION2,
        "auto-topic-alias-rotation",
        protocol=MQTTProtocolVersion.MQTTv5,
    )
    mqttc.enable_topic_alias()
    mqttc.connect_async("localhost", fake_broker.port)
    mqttc.loop_start()
    try:
        _connect_and_grant_aliases(fake_broker, 2)

        topics = [
            b"some/really/long/topic/name/that/benefits/from/aliasing/alpha",
            b"some/really/long/topic/name/that/benefits/from/aliasing/beta",
            b"some/really/long/topic/name/that/benefits/from/aliasing/gamma",
        ]
        resolver = AliasResolver()
        # (topic index, expected wire topic, expected alias)
        expected = [
            (0, topics[0], 1),  # first use of alias 1
            (1, topics[1], 2),  # first use of alias 2
            (0, b"", 1),        # alias 1 reused, topic omitted
            (1, b"", 2),        # alias 2 reused, topic omitted
            (2, topics[2], 1),  # quota full: alias 1 re-bound (LRU)
            (0, topics[0], 2),  # alias 2 re-bound
            (1, topics[1], 1),  # alias 1 re-bound again
            (2, topics[2], 2),
            (0, topics[0], 1),
        ]
        for index, (topic_index, wire_topic, alias) in enumerate(expected):
            topic = topics[topic_index]
            mqttc.publish(topic.decode(), f"payload-{index}", qos=0)
            publish = read_publish(fake_broker)
            assert publish.topic == wire_topic
            assert publish.alias == alias
            # The topic reconstructed from the wire must always match
            # the topic passed to publish().
            assert resolver.resolve(publish) == topic
            assert publish.payload == f"payload-{index}".encode()
    finally:
        mqttc.loop_stop()


def test_auto_alias_qos1_reconnect_rebinds(fake_broker: FakeBroker):
    """Aliases belong to the connection: an unacknowledged QoS 1 message
    re-sent after a reconnect must carry the full topic again, so it can
    never be mis-delivered through a stale alias."""
    mqttc = client.Client(
        CallbackAPIVersion.VERSION2,
        "auto-topic-alias-qos1",
        protocol=MQTTProtocolVersion.MQTTv5,
    )
    mqttc.enable_topic_alias()
    mqttc.connect_async("localhost", fake_broker.port)
    mqttc.loop_start()
    try:
        _connect_and_grant_aliases(fake_broker, 2)

        topic_a = "some/really/long/topic/name/that/benefits/alpha"
        topic_b = "some/really/long/topic/name/that/benefits/beta"
        resolver = AliasResolver()

        mqttc.publish(topic_a, "warm-up", qos=0)
        publish = read_publish(fake_broker)
        assert resolver.resolve(publish) == topic_a.encode()

        # QoS 1 publish that the broker never acknowledges.
        info = mqttc.publish(topic_b, "important", qos=1)
        publish = read_publish(fake_broker)
        assert publish.qos == 1
        assert not publish.dup
        mid = publish.mid
        assert resolver.resolve(publish) == topic_b.encode()

        # Drop the connection without sending PUBACK; the client
        # reconnects automatically (connect_async + loop_start).
        fake_broker.drop_connection()
        _connect_and_grant_aliases(fake_broker, 2)

        # Aliases are scoped to a connection: the broker starts over.
        resolver = AliasResolver()
        publish = read_publish(fake_broker)
        assert publish.qos == 1
        assert publish.dup  # retransmission
        assert publish.mid == mid
        # The full topic must be sent again together with a (new) alias.
        assert publish.topic == topic_b.encode()
        assert publish.alias is not None
        assert resolver.resolve(publish) == topic_b.encode()

        # Acknowledge now; the client must complete the publish.
        puback_packet = paho_test.gen_puback(mid, proto_ver=5)
        fake_broker.send_packet(puback_packet)
        info.wait_for_publish(5)
        assert info.is_published()

        # A new publish on the new connection re-registers its alias.
        mqttc.publish(topic_a, "after-reconnect", qos=0)
        publish = read_publish(fake_broker)
        assert publish.topic == topic_a.encode()
        assert resolver.resolve(publish) == topic_a.encode()
    finally:
        mqttc.loop_stop()


def test_manual_alias_takes_precedence(fake_broker: FakeBroker):
    """An application-provided TopicAlias goes on the wire unchanged and
    deterministically replaces a conflicting automatic binding."""
    mqttc = client.Client(
        CallbackAPIVersion.VERSION2,
        "auto-topic-alias-manual",
        protocol=MQTTProtocolVersion.MQTTv5,
    )
    mqttc.enable_topic_alias()
    mqttc.connect_async("localhost", fake_broker.port)
    mqttc.loop_start()
    try:
        _connect_and_grant_aliases(fake_broker, 2)

        topic_a = "some/really/long/topic/name/that/benefits/alpha"
        topic_b = "some/really/long/topic/name/that/benefits/beta"
        topic_c = "some/really/long/topic/name/that/benefits/gamma"
        resolver = AliasResolver()

        mqttc.publish(topic_a, "a1", qos=0)
        publish = read_publish(fake_broker)
        assert (publish.topic, publish.alias) == (topic_a.encode(), 1)
        assert resolver.resolve(publish) == topic_a.encode()

        mqttc.publish(topic_b, "b1", qos=0)
        publish = read_publish(fake_broker)
        assert (publish.topic, publish.alias) == (topic_b.encode(), 2)
        assert resolver.resolve(publish) == topic_b.encode()

        # The application manually binds alias 1 to topic C: the manual
        # value goes on the wire unchanged and wins over the automatic
        # binding of alias 1 to topic A.
        properties = Properties(PacketTypes.PUBLISH)
        properties.TopicAlias = 1
        mqttc.publish(topic_c, "c1", qos=0, properties=properties)
        publish = read_publish(fake_broker)
        assert (publish.topic, publish.alias) == (topic_c.encode(), 1)
        assert resolver.resolve(publish) == topic_c.encode()

        # Publishing A again: alias 1 is now manually bound to C, so A is
        # deterministically re-registered on the only evictable alias (2),
        # with the full topic on the wire.
        mqttc.publish(topic_a, "a2", qos=0)
        publish = read_publish(fake_broker)
        assert (publish.topic, publish.alias) == (topic_a.encode(), 2)
        assert resolver.resolve(publish) == topic_a.encode()

        # The manual binding is reused as-is: publishing C without
        # properties may omit the topic since alias 1 still maps to C.
        mqttc.publish(topic_c, "c2", qos=0)
        publish = read_publish(fake_broker)
        assert (publish.topic, publish.alias) == (b"", 1)
        assert resolver.resolve(publish) == topic_c.encode()
    finally:
        mqttc.loop_stop()


def test_no_quota_sends_full_topic(fake_broker: FakeBroker):
    """Without a granted quota (CONNACK without TopicAliasMaximum) the
    client sends full topics and no TopicAlias property, even when the
    automatic mode is enabled."""
    mqttc = client.Client(
        CallbackAPIVersion.VERSION2,
        "auto-topic-alias-no-quota",
        protocol=MQTTProtocolVersion.MQTTv5,
    )
    mqttc.enable_topic_alias()
    mqttc.connect_async("localhost", fake_broker.port)
    mqttc.loop_start()
    try:
        _connect_and_grant_aliases(fake_broker, 0)
        assert mqttc.topic_alias_maximum == 0

        topic = "some/really/long/topic/name/that/benefits/alpha"
        for index in range(3):
            mqttc.publish(topic, f"payload-{index}", qos=0)
            publish = read_publish(fake_broker)
            assert publish.topic == topic.encode()
            assert publish.alias is None
            assert publish.payload == f"payload-{index}".encode()
    finally:
        mqttc.loop_stop()


def test_disabled_sends_full_topic(fake_broker: FakeBroker):
    """With a quota granted but the automatic mode never enabled, the
    client keeps sending full topics without TopicAlias properties."""
    mqttc = client.Client(
        CallbackAPIVersion.VERSION2,
        "auto-topic-alias-disabled",
        protocol=MQTTProtocolVersion.MQTTv5,
    )
    mqttc.connect_async("localhost", fake_broker.port)
    mqttc.loop_start()
    try:
        _connect_and_grant_aliases(fake_broker, 2)
        assert mqttc.topic_alias_maximum == 2

        topic = "some/really/long/topic/name/that/benefits/alpha"
        for index in range(3):
            mqttc.publish(topic, f"payload-{index}", qos=0)
            publish = read_publish(fake_broker)
            assert publish.topic == topic.encode()
            assert publish.alias is None
    finally:
        mqttc.loop_stop()
