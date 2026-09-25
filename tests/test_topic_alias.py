"""Tests for MQTT v5 topic alias management.

Topic aliases are scoped to a single connection: the client and the
server each maintain their own mapping, negotiated with the
TopicAliasMaximum property of CONNECT / CONNACK. The first use of an
alias must carry the full topic, later uses may refer to it with an
empty topic. On reconnect, both mappings are forgotten and messages
re-sent are sent with the full topic of the new connection.
"""

import socket
import unittest

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

import tests.paho_test as paho_test
from tests import mqtt5_props

TOPIC_ALIAS = mqtt5_props.PROP_TOPIC_ALIAS
TOPIC_ALIAS_MAXIMUM = mqtt5_props.PROP_TOPIC_ALIAS_MAXIMUM


def alias_prop(alias):
    return mqtt5_props.gen_uint16_prop(TOPIC_ALIAS, alias)


def alias_max_prop(maximum):
    return mqtt5_props.gen_uint16_prop(TOPIC_ALIAS_MAXIMUM, maximum)


def connack_packet(alias_maximum=None):
    properties = None
    if alias_maximum is not None:
        properties = alias_max_prop(alias_maximum)
    return paho_test.gen_connack(
        rc=0, proto_ver=5, properties=properties, property_helper=False)


def connect_properties(alias_maximum):
    properties = Properties(PacketTypes.CONNECT)
    properties.TopicAliasMaximum = alias_maximum
    return properties


class FakeBroker:
    """Minimal fake broker: a socketpair end that the test drives directly.

    The paho client is connected to it by replacing Client._create_socket
    with a function returning one end of a socketpair; the test then uses
    the other end to send and inspect raw MQTT packets.
    """

    def __init__(self):
        self.peer = None

    def attach(self, client):
        client._sockpairR, client._sockpairW = socket.socketpair()
        client._create_socket = self._create_socket

    def _create_socket(self):
        sock, peer = socket.socketpair()
        sock.setblocking(False)
        peer.settimeout(10)
        self.peer = peer
        return sock


class TestTopicAlias(unittest.TestCase):

    def _new_client(self, connect_props=None, alias_maximum=10):
        client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION1,
            "topic-alias-test",
            protocol=mqtt.MQTTv5,
        )
        broker = FakeBroker()
        broker.attach(client)
        client.connect("localhost", 1234, properties=connect_props)

        if connect_props is not None and hasattr(connect_props, "TopicAliasMaximum"):
            connect_props_bytes = alias_max_prop(connect_props.TopicAliasMaximum)
        else:
            connect_props_bytes = None
        paho_test.expect_packet(
            broker.peer, "connect",
            paho_test.gen_connect(
                "topic-alias-test", keepalive=60, proto_ver=5,
                properties=connect_props_bytes))
        broker.peer.send(connack_packet(alias_maximum))
        client.loop(0.5)
        self.assertTrue(client.is_connected())
        return client, broker

    def test_auto_alias_assign_and_reuse(self):
        client, broker = self._new_client()

        # first publish establishes the alias with the full topic
        client.publish("long/topic/name", "payload1", qos=0)
        paho_test.expect_packet(
            broker.peer, "publish 1",
            paho_test.gen_publish(
                "long/topic/name", qos=0, payload="payload1", proto_ver=5,
                properties=alias_prop(1)))

        # second publish to the same topic uses the alias only
        client.publish("long/topic/name", "payload2", qos=0)
        paho_test.expect_packet(
            broker.peer, "publish 2",
            paho_test.gen_publish(
                "", qos=0, payload="payload2", proto_ver=5,
                properties=alias_prop(1)))

        # a new topic gets the next free alias
        client.publish("other/topic", "payload3", qos=0)
        paho_test.expect_packet(
            broker.peer, "publish 3",
            paho_test.gen_publish(
                "other/topic", qos=0, payload="payload3", proto_ver=5,
                properties=alias_prop(2)))

        # the properties object owned by the caller is not modified
        properties = Properties(PacketTypes.PUBLISH)
        client.publish("third/topic", "payload4", qos=0, properties=properties)
        paho_test.expect_packet(
            broker.peer, "publish 4",
            paho_test.gen_publish(
                "third/topic", qos=0, payload="payload4", proto_ver=5,
                properties=alias_prop(3)))
        self.assertFalse(hasattr(properties, "TopicAlias"))

        client.disconnect()
        paho_test.expect_packet(
            broker.peer, "disconnect", paho_test.gen_disconnect())

    def test_no_alias_when_not_negotiated(self):
        # CONNACK without TopicAliasMaximum: aliases must not be used
        client, broker = self._new_client(alias_maximum=None)

        client.publish("some/topic", "payload1", qos=0)
        paho_test.expect_packet(
            broker.peer, "publish 1",
            paho_test.gen_publish(
                "some/topic", qos=0, payload="payload1", proto_ver=5))

        client.publish("some/topic", "payload2", qos=0)
        paho_test.expect_packet(
            broker.peer, "publish 2",
            paho_test.gen_publish(
                "some/topic", qos=0, payload="payload2", proto_ver=5))

        client.disconnect()
        paho_test.expect_packet(
            broker.peer, "disconnect", paho_test.gen_disconnect())

    def test_manual_alias(self):
        client, broker = self._new_client()

        properties = Properties(PacketTypes.PUBLISH)
        properties.TopicAlias = 1

        # first use of the alias carries the full topic
        client.publish("manual/topic", "payload1", qos=0, properties=properties)
        paho_test.expect_packet(
            broker.peer, "publish 1",
            paho_test.gen_publish(
                "manual/topic", qos=0, payload="payload1", proto_ver=5,
                properties=alias_prop(1)))

        # then an empty topic refers to the alias
        client.publish("", "payload2", qos=0, properties=properties)
        paho_test.expect_packet(
            broker.peer, "publish 2",
            paho_test.gen_publish(
                "", qos=0, payload="payload2", proto_ver=5,
                properties=alias_prop(1)))

        # the alias can be re-bound to another topic
        client.publish("rebound/topic", "payload3", qos=0, properties=properties)
        paho_test.expect_packet(
            broker.peer, "publish 3",
            paho_test.gen_publish(
                "rebound/topic", qos=0, payload="payload3", proto_ver=5,
                properties=alias_prop(1)))

        client.publish("", "payload4", qos=0, properties=properties)
        paho_test.expect_packet(
            broker.peer, "publish 4",
            paho_test.gen_publish(
                "", qos=0, payload="payload4", proto_ver=5,
                properties=alias_prop(1)))

        client.disconnect()
        paho_test.expect_packet(
            broker.peer, "disconnect", paho_test.gen_disconnect())

    def test_undefined_alias_rejected(self):
        client, broker = self._new_client(alias_maximum=2)

        properties = Properties(PacketTypes.PUBLISH)
        properties.TopicAlias = 1

        # an empty topic requires an alias established on this connection
        self.assertRaises(
            ValueError, client.publish, "", "payload", qos=0,
            properties=properties)
        # an empty topic without any alias is invalid
        self.assertRaises(ValueError, client.publish, "", "payload", qos=0)
        # an alias above the negotiated maximum is invalid
        properties.TopicAlias = 3
        self.assertRaises(
            ValueError, client.publish, "some/topic", "payload", qos=0,
            properties=properties)

        # the client must not have sent anything
        paho_test.expect_no_packet(broker.peer, delay=0.5)

    def test_receive_alias_resolved(self):
        client, broker = self._new_client(connect_props=connect_properties(5))

        messages = []
        client.on_message = lambda client, userdata, message: messages.append(message)

        # the server establishes alias 1 with a full topic
        broker.peer.send(paho_test.gen_publish(
            "server/temp", qos=0, payload="t1", proto_ver=5,
            properties=alias_prop(1)))
        client.loop(0.5)
        self.assertEqual(messages[-1].topic, "server/temp")

        # an empty topic refers to the alias; the callback sees the real topic
        broker.peer.send(paho_test.gen_publish(
            "", qos=0, payload="t2", proto_ver=5, properties=alias_prop(1)))
        client.loop(0.5)
        self.assertEqual(messages[-1].topic, "server/temp")

        # the alias can be re-bound to another topic
        broker.peer.send(paho_test.gen_publish(
            "server/humidity", qos=0, payload="h1", proto_ver=5,
            properties=alias_prop(1)))
        client.loop(0.5)
        self.assertEqual(messages[-1].topic, "server/humidity")

        broker.peer.send(paho_test.gen_publish(
            "", qos=0, payload="h2", proto_ver=5, properties=alias_prop(1)))
        client.loop(0.5)
        self.assertEqual(messages[-1].topic, "server/humidity")

        client.disconnect()
        paho_test.expect_packet(
            broker.peer, "disconnect", paho_test.gen_disconnect())

    def test_receive_alias_out_of_range(self):
        client, broker = self._new_client(connect_props=connect_properties(1))

        # alias 2 is above the maximum this client allowed
        broker.peer.send(paho_test.gen_publish(
            "some/topic", qos=0, payload="x", proto_ver=5,
            properties=alias_prop(2)))
        rc = client.loop(0.5)
        self.assertNotEqual(rc, mqtt.MQTT_ERR_SUCCESS)
        # the client disconnects with "Topic alias invalid" (0x94)
        paho_test.expect_packet(
            broker.peer, "disconnect",
            paho_test.gen_disconnect(reason_code=148, proto_ver=5))

    def test_receive_alias_not_allowed(self):
        client, broker = self._new_client()

        # the client did not advertise a TopicAliasMaximum, so the server
        # must not use aliases at all
        broker.peer.send(paho_test.gen_publish(
            "some/topic", qos=0, payload="x", proto_ver=5,
            properties=alias_prop(1)))
        rc = client.loop(0.5)
        self.assertNotEqual(rc, mqtt.MQTT_ERR_SUCCESS)
        paho_test.expect_packet(
            broker.peer, "disconnect",
            paho_test.gen_disconnect(reason_code=148, proto_ver=5))

    def test_receive_alias_undefined_reference(self):
        client, broker = self._new_client(connect_props=connect_properties(5))

        # an empty topic referring to an unknown alias is a protocol error
        broker.peer.send(paho_test.gen_publish(
            "", qos=0, payload="x", proto_ver=5, properties=alias_prop(1)))
        rc = client.loop(0.5)
        self.assertNotEqual(rc, mqtt.MQTT_ERR_SUCCESS)
        paho_test.expect_packet(
            broker.peer, "disconnect",
            paho_test.gen_disconnect(reason_code=148, proto_ver=5))

    def test_reconnect_clears_aliases(self):
        messages = []

        client, broker = self._new_client(connect_props=connect_properties(5))
        client.on_message = lambda client, userdata, message: messages.append(message)

        # QoS 1 publish: alias 1 is assigned to the temperature topic
        client.publish("temp/topic", "temp payload", qos=1)
        paho_test.expect_packet(
            broker.peer, "publish temp",
            paho_test.gen_publish(
                "temp/topic", qos=1, mid=1, payload="temp payload",
                proto_ver=5, properties=alias_prop(1)))
        # the broker does not PUBACK: the message stays in-flight

        # the server binds alias 1 to a temperature topic on its side
        broker.peer.send(paho_test.gen_publish(
            "server/temp", qos=0, payload="t1", proto_ver=5,
            properties=alias_prop(1)))
        client.loop(0.5)
        self.assertEqual(messages[-1].topic, "server/temp")

        # reconnect: the client closes the old connection and the broker
        # accepts the new one
        client.reconnect()
        paho_test.expect_packet(
            broker.peer, "connect 2",
            paho_test.gen_connect(
                "topic-alias-test", keepalive=60, proto_ver=5,
                clean_session=False, properties=alias_max_prop(5)))
        broker.peer.send(connack_packet(alias_maximum=10))
        client.loop(0.5)

        # the QoS 1 message is re-sent with the full topic of the current
        # connection, not with an alias of the old connection
        paho_test.expect_packet(
            broker.peer, "re-sent publish",
            paho_test.gen_publish(
                "temp/topic", qos=1, mid=1, dup=True, payload="temp payload",
                proto_ver=5, properties=alias_prop(1)))

        # the server can re-bind alias 1 to a different topic; callbacks
        # see the topic of the current connection
        broker.peer.send(paho_test.gen_publish(
            "server/humidity", qos=0, payload="h1", proto_ver=5,
            properties=alias_prop(1)))
        client.loop(0.5)
        self.assertEqual(messages[-1].topic, "server/humidity")

        broker.peer.send(paho_test.gen_publish(
            "", qos=0, payload="h2", proto_ver=5, properties=alias_prop(1)))
        client.loop(0.5)
        self.assertEqual(messages[-1].topic, "server/humidity")

        client.disconnect()
        paho_test.expect_packet(
            broker.peer, "disconnect", paho_test.gen_disconnect())


if __name__ == "__main__":
    unittest.main()

