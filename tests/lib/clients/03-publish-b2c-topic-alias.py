import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties
from tests.paho_test import get_test_server_port, wait_for_keyboard_interrupt

received = []
filtered = []


def on_message(mqttc, obj, msg):
    # on_message only fires when no topic-filtered callback matches.
    received.append((msg.topic, bytes(msg.payload)))


def on_topic_two(mqttc, obj, msg):
    # Topic-filtered callbacks must see the resolved topic, not the
    # (empty) topic sent on the wire.
    filtered.append((msg.topic, bytes(msg.payload)))
    received.append((msg.topic, bytes(msg.payload)))


# Allow the server to use topic aliases 1-2 towards us.
connect_properties = Properties(PacketTypes.CONNECT)
connect_properties.TopicAliasMaximum = 2

mqttc = mqtt.Client(
    mqtt.CallbackAPIVersion.VERSION1,
    "publish-topic-alias-test",
    protocol=mqtt.MQTTv5,
)
mqttc.on_message = on_message
mqttc.message_callback_add("alias/topic/two", on_topic_two)


def loop_until_disconnected():
    while mqttc.is_connected():
        mqttc.loop()


with wait_for_keyboard_interrupt():
    mqttc.connect("localhost", get_test_server_port(), properties=connect_properties)

    # Connection 1: full topic + alias registration, alias-only reference,
    # legal rebind of the alias, alias-only reference of the rebound alias.
    while len(received) < 4:
        mqttc.loop()
    loop_until_disconnected()

    # Connection 2: the session is resumed (session present flag set), but
    # the alias table belongs to the network connection and must be empty.
    # The server sends an alias-only PUBLISH for the previously known alias,
    # which is a protocol error and must not be delivered.
    mqttc.reconnect()
    loop_until_disconnected()

    # Connection 3: alias above the declared TopicAliasMaximum is a
    # protocol error and must not be delivered or acknowledged.
    mqttc.reconnect()
    loop_until_disconnected()

    # Connection 4: alias 0 is a protocol error and must not be delivered.
    mqttc.reconnect()
    loop_until_disconnected()

    assert received == [
        ("alias/topic/one", b"first"),
        ("alias/topic/one", b"second"),
        ("alias/topic/two", b"third"),
        ("alias/topic/two", b"fourth"),
    ], f"Unexpected messages: {received}"
    assert filtered == [
        ("alias/topic/two", b"third"),
        ("alias/topic/two", b"fourth"),
    ], f"Unexpected filtered messages: {filtered}"
