# Tests for MQTT v5.0 Topic Alias management.
#
# These tests drive the client through a fake socket and inject raw
# packets, so no network access or broker is needed.

from paho.mqtt.client import Client
from paho.mqtt.enums import CallbackAPIVersion, MQTTErrorCode, MQTTProtocolVersion
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

import tests.mqtt5_props as mqtt5_props
import tests.paho_test as paho_test

TEMP_TOPIC = "sensor/temperature"
HUM_TOPIC = "sensor/humidity"


class FakeSocket:
    def __init__(self):
        self.sent = bytearray()
        self.closed = False

    def send(self, data):
        self.sent += data
        return len(data)

    def close(self):
        self.closed = True

    def setblocking(self, flag):
        pass


def split_packet(packet):
    # Split a raw MQTT packet into (command byte, remaining length, body).
    command = packet[0]
    multiplier = 1
    remaining_length = 0
    pos = 1
    while True:
        digit = packet[pos]
        pos += 1
        remaining_length += (digit & 127) * multiplier
        if digit & 128 == 0:
            break
        multiplier *= 128
    return command, remaining_length, packet[pos:]


def feed_packet(mqttc, packet):
    # Simulate the reception of a raw packet by the client.
    command, remaining_length, body = split_packet(packet)
    mqttc._in_packet = {
        "command": command,
        "have_remaining": 0,
        "remaining_count": [],
        "remaining_mult": 1,
        "remaining_length": remaining_length,
        "packet": bytearray(body),
        "to_process": len(body),
        "pos": 0,
    }
    return mqttc._packet_handle()


def make_client(monkeypatch, auto_topic_alias=False, topic_alias_maximum=10):
    mqttc = Client(
        CallbackAPIVersion.VERSION2,
        "topic-alias-test",
        protocol=MQTTProtocolVersion.MQTTv5,
    )
    mqttc.auto_topic_alias = auto_topic_alias
    sock = FakeSocket()
    monkeypatch.setattr(mqttc, "_create_socket", lambda: sock)
    connect_properties = Properties(PacketTypes.CONNECT)
    connect_properties.TopicAliasMaximum = topic_alias_maximum
    mqttc.connect_async("localhost", 1883, properties=connect_properties)
    mqttc.reconnect()
    assert sock.sent  # CONNECT was sent
    sock.sent.clear()
    return mqttc, sock


def do_connack(mqttc, sock, topic_alias_maximum=10):
    connack = paho_test.gen_connack(
        rc=0,
        proto_ver=5,
        properties=mqtt5_props.gen_uint16_prop(
            mqtt5_props.PROP_TOPIC_ALIAS_MAXIMUM, topic_alias_maximum),
        property_helper=False,
    )
    rc = feed_packet(mqttc, connack)
    assert rc == MQTTErrorCode.MQTT_ERR_SUCCESS


def alias_prop(alias):
    return mqtt5_props.gen_uint16_prop(mqtt5_props.PROP_TOPIC_ALIAS, alias)


def test_outbound_auto_alias_assign_then_reference(monkeypatch):
    mqttc, sock = make_client(monkeypatch)
    do_connack(mqttc, sock)

    # First publish to a topic: full topic + alias to establish the mapping.
    mqttc.publish(TEMP_TOPIC, b"22.5", qos=0, auto_topic_alias=True)
    assert bytes(sock.sent) == paho_test.gen_publish(
        TEMP_TOPIC, qos=0, payload=b"22.5", proto_ver=5, properties=alias_prop(1))
    sock.sent.clear()

    # Second publish to the same topic: empty topic referencing the alias.
    mqttc.publish(TEMP_TOPIC, b"23.0", qos=0, auto_topic_alias=True)
    assert bytes(sock.sent) == paho_test.gen_publish(
        "", qos=0, payload=b"23.0", proto_ver=5, properties=alias_prop(1))
    sock.sent.clear()

    # A different topic gets the next alias, sent with its full topic.
    mqttc.publish(HUM_TOPIC, b"60", qos=0, auto_topic_alias=True)
    assert bytes(sock.sent) == paho_test.gen_publish(
        HUM_TOPIC, qos=0, payload=b"60", proto_ver=5, properties=alias_prop(2))


def test_outbound_auto_alias_client_wide_default(monkeypatch):
    mqttc, sock = make_client(monkeypatch, auto_topic_alias=True)
    do_connack(mqttc, sock)

    mqttc.publish(TEMP_TOPIC, b"22.5", qos=0)
    assert bytes(sock.sent) == paho_test.gen_publish(
        TEMP_TOPIC, qos=0, payload=b"22.5", proto_ver=5, properties=alias_prop(1))
    sock.sent.clear()

    # Per-call opt-out of the client-wide default.
    mqttc.publish(TEMP_TOPIC, b"23.0", qos=0, auto_topic_alias=False)
    assert bytes(sock.sent) == paho_test.gen_publish(
        TEMP_TOPIC, qos=0, payload=b"23.0", proto_ver=5)


def test_outbound_alias_disabled_by_default(monkeypatch):
    mqttc, sock = make_client(monkeypatch)
    do_connack(mqttc, sock)

    for _ in range(2):
        mqttc.publish(TEMP_TOPIC, b"22.5", qos=0)
        assert bytes(sock.sent) == paho_test.gen_publish(
            TEMP_TOPIC, qos=0, payload=b"22.5", proto_ver=5)
        sock.sent.clear()


def test_outbound_alias_respects_server_maximum(monkeypatch):
    mqttc, sock = make_client(monkeypatch, auto_topic_alias=True)
    do_connack(mqttc, sock, topic_alias_maximum=1)

    mqttc.publish(TEMP_TOPIC, b"22.5", qos=0)
    assert bytes(sock.sent) == paho_test.gen_publish(
        TEMP_TOPIC, qos=0, payload=b"22.5", proto_ver=5, properties=alias_prop(1))
    sock.sent.clear()

    # No alias left: full topic without any Topic Alias property.
    mqttc.publish(HUM_TOPIC, b"60", qos=0)
    assert bytes(sock.sent) == paho_test.gen_publish(
        HUM_TOPIC, qos=0, payload=b"60", proto_ver=5)


def test_outbound_alias_not_used_when_server_disallows(monkeypatch):
    mqttc, sock = make_client(monkeypatch, auto_topic_alias=True)
    do_connack(mqttc, sock, topic_alias_maximum=0)

    mqttc.publish(TEMP_TOPIC, b"22.5", qos=0)
    assert bytes(sock.sent) == paho_test.gen_publish(
        TEMP_TOPIC, qos=0, payload=b"22.5", proto_ver=5)


def test_outbound_manual_alias_takes_precedence(monkeypatch):
    mqttc, sock = make_client(monkeypatch, auto_topic_alias=True)
    do_connack(mqttc, sock)

    properties = Properties(PacketTypes.PUBLISH)
    properties.TopicAlias = 7
    mqttc.publish(TEMP_TOPIC, b"22.5", qos=0, properties=properties)
    assert bytes(sock.sent) == paho_test.gen_publish(
        TEMP_TOPIC, qos=0, payload=b"22.5", proto_ver=5, properties=alias_prop(7))
    sock.sent.clear()

    # The manual alias must not consume an automatically allocated alias.
    mqttc.publish(HUM_TOPIC, b"60", qos=0)
    assert bytes(sock.sent) == paho_test.gen_publish(
        HUM_TOPIC, qos=0, payload=b"60", proto_ver=5, properties=alias_prop(1))


def test_outbound_alias_does_not_mutate_message_properties(monkeypatch):
    mqttc, sock = make_client(monkeypatch, auto_topic_alias=True)
    do_connack(mqttc, sock)

    properties = Properties(PacketTypes.PUBLISH)
    properties.MessageExpiryInterval = 60
    info = mqttc.publish(TEMP_TOPIC, b"22.5", qos=1, properties=properties)
    assert bytes(sock.sent) == paho_test.gen_publish(
        TEMP_TOPIC, qos=1, mid=info.mid, payload=b"22.5", proto_ver=5,
        properties=mqtt5_props.gen_uint32_prop(
            mqtt5_props.PROP_MESSAGE_EXPIRY_INTERVAL, 60) + alias_prop(1))
    # The properties stored for retransmission must not carry the alias of
    # this connection.
    message = mqttc._out_messages[info.mid]
    assert not hasattr(message.properties, "TopicAlias")
    assert message.topic == TEMP_TOPIC


def test_inbound_alias_resolution_and_rebind(monkeypatch):
    mqttc, sock = make_client(monkeypatch, topic_alias_maximum=4)
    do_connack(mqttc, sock)

    received = []
    mqttc.on_message = lambda client, userdata, message: received.append(message)

    # First use of alias 1: full topic establishes the mapping.
    feed_packet(mqttc, paho_test.gen_publish(
        TEMP_TOPIC, qos=0, payload=b"22.5", proto_ver=5, properties=alias_prop(1)))
    # Then the alias alone references it.
    feed_packet(mqttc, paho_test.gen_publish(
        "", qos=0, payload=b"23.0", proto_ver=5, properties=alias_prop(1)))
    # Rebind alias 1 to another topic.
    feed_packet(mqttc, paho_test.gen_publish(
        HUM_TOPIC, qos=0, payload=b"60", proto_ver=5, properties=alias_prop(1)))
    feed_packet(mqttc, paho_test.gen_publish(
        "", qos=0, payload=b"61", proto_ver=5, properties=alias_prop(1)))

    # The callback always sees the resolved, real topic.
    assert [m.topic for m in received] == [TEMP_TOPIC, TEMP_TOPIC, HUM_TOPIC, HUM_TOPIC]
    assert [bytes(m.payload) for m in received] == [b"22.5", b"23.0", b"60", b"61"]


def test_inbound_alias_undefined_reference(monkeypatch):
    mqttc, sock = make_client(monkeypatch, topic_alias_maximum=4)
    do_connack(mqttc, sock)

    received = []
    mqttc.on_message = lambda client, userdata, message: received.append(message)

    rc = feed_packet(mqttc, paho_test.gen_publish(
        "", qos=0, payload=b"23.0", proto_ver=5, properties=alias_prop(2)))

    assert rc == MQTTErrorCode.MQTT_ERR_PROTOCOL
    assert received == []
    # The client rejects the packet with a DISCONNECT (Topic alias invalid).
    assert bytes(sock.sent) == paho_test.gen_disconnect(reason_code=148, proto_ver=5)


def test_inbound_alias_above_negotiated_maximum(monkeypatch):
    mqttc, sock = make_client(monkeypatch, topic_alias_maximum=1)
    do_connack(mqttc, sock)

    received = []
    mqttc.on_message = lambda client, userdata, message: received.append(message)

    rc = feed_packet(mqttc, paho_test.gen_publish(
        TEMP_TOPIC, qos=0, payload=b"22.5", proto_ver=5, properties=alias_prop(2)))

    assert rc == MQTTErrorCode.MQTT_ERR_PROTOCOL
    assert received == []
    assert bytes(sock.sent) == paho_test.gen_disconnect(reason_code=148, proto_ver=5)


def test_inbound_empty_topic_without_alias_is_protocol_error(monkeypatch):
    mqttc, sock = make_client(monkeypatch)
    do_connack(mqttc, sock)

    rc = feed_packet(mqttc, paho_test.gen_publish(
        "", qos=0, payload=b"23.0", proto_ver=5))

    assert rc == MQTTErrorCode.MQTT_ERR_PROTOCOL


def test_reconnect_retransmit_uses_full_topic(monkeypatch):
    mqttc, sock = make_client(monkeypatch, auto_topic_alias=True)
    do_connack(mqttc, sock)

    # QoS 1 publish on connection 1: alias 1 is bound to the temperature
    # topic. It stays unacknowledged.
    info = mqttc.publish(TEMP_TOPIC, b"22.5", qos=1)
    assert bytes(sock.sent) == paho_test.gen_publish(
        TEMP_TOPIC, qos=1, mid=info.mid, payload=b"22.5", proto_ver=5,
        properties=alias_prop(1))

    # Reconnect: the alias mappings of the old connection are dropped.
    mqttc.reconnect()
    assert mqttc._out_topic_aliases == {}
    assert mqttc._in_topic_aliases == {}
    sock.sent.clear()
    do_connack(mqttc, sock)

    # The unacknowledged message is retransmitted with its full topic, it
    # must not reference an alias of the previous connection.
    assert bytes(sock.sent) == paho_test.gen_publish(
        TEMP_TOPIC, qos=1, mid=info.mid, payload=b"22.5", proto_ver=5,
        dup=True, properties=alias_prop(1))
    sock.sent.clear()

    # New publishes allocate fresh aliases on the new connection.
    mqttc.publish(HUM_TOPIC, b"60", qos=0)
    assert bytes(sock.sent) == paho_test.gen_publish(
        HUM_TOPIC, qos=0, payload=b"60", proto_ver=5, properties=alias_prop(2))


def test_reconnect_inbound_alias_rebind(monkeypatch):
    mqttc, sock = make_client(monkeypatch, topic_alias_maximum=4)
    do_connack(mqttc, sock)

    received = []
    mqttc.on_message = lambda client, userdata, message: received.append(message)

    # Connection 1: alias 1 points to the temperature topic.
    feed_packet(mqttc, paho_test.gen_publish(
        TEMP_TOPIC, qos=0, payload=b"22.5", proto_ver=5, properties=alias_prop(1)))
    feed_packet(mqttc, paho_test.gen_publish(
        "", qos=0, payload=b"23.0", proto_ver=5, properties=alias_prop(1)))

    # Reconnect: alias 1 is now bound to the humidity topic by the server.
    mqttc.reconnect()
    sock.sent.clear()
    do_connack(mqttc, sock)

    feed_packet(mqttc, paho_test.gen_publish(
        HUM_TOPIC, qos=0, payload=b"60", proto_ver=5, properties=alias_prop(1)))
    feed_packet(mqttc, paho_test.gen_publish(
        "", qos=0, payload=b"61", proto_ver=5, properties=alias_prop(1)))

    # Callbacks always see the topic that is valid on the current
    # connection, never a stale mapping from the previous one.
    assert [m.topic for m in received] == [TEMP_TOPIC, TEMP_TOPIC, HUM_TOPIC, HUM_TOPIC]
