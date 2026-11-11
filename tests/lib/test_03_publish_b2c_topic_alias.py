# Test whether a client resolves MQTTv5 topic aliases on inbound PUBLISH
# packets, and rejects invalid ones without delivering them.

# Connection 1: the server registers alias 1 with a full topic, then sends
# an alias-only PUBLISH (empty topic), then legally rebinds alias 1 to a new
# topic and references it again. All four messages must be delivered with
# the resolved topic.
# Connection 2: the session is resumed (session present flag set) but the
# alias table must not survive the reconnect, so an alias-only PUBLISH is a
# protocol error and the client must close the connection.
# Connection 3: an alias above the TopicAliasMaximum declared by the client
# is a protocol error; the client must close the connection without sending
# a PUBACK.
# Connection 4: alias 0 is a protocol error; the client must close the
# connection.

from tests import mqtt5_props, paho_test

alias_maximum_props = mqtt5_props.gen_uint16_prop(mqtt5_props.PROP_TOPIC_ALIAS_MAXIMUM, 2)

connect_packet = paho_test.gen_connect(
    "publish-topic-alias-test", keepalive=60, proto_ver=5, properties=alias_maximum_props)
# Reconnects don't set the clean start flag (MQTT_CLEAN_START_FIRST_ONLY).
reconnect_packet = paho_test.gen_connect(
    "publish-topic-alias-test", keepalive=60, clean_session=False, proto_ver=5,
    properties=alias_maximum_props)

connack_packet = paho_test.gen_connack(rc=0, proto_ver=5)
connack_session_present = paho_test.gen_connack(flags=1, rc=0, proto_ver=5)


def alias_prop(alias):
    return mqtt5_props.gen_uint16_prop(mqtt5_props.PROP_TOPIC_ALIAS, alias)


publish_full_1 = paho_test.gen_publish(
    "alias/topic/one", qos=0, payload="first", proto_ver=5, properties=alias_prop(1))
publish_alias_1 = paho_test.gen_publish(
    "", qos=0, payload="second", proto_ver=5, properties=alias_prop(1))
publish_rebind_1 = paho_test.gen_publish(
    "alias/topic/two", qos=0, payload="third", proto_ver=5, properties=alias_prop(1))
publish_alias_1_rebound = paho_test.gen_publish(
    "", qos=0, payload="fourth", proto_ver=5, properties=alias_prop(1))

# Alias 1 was bound on connection 1, but must be unknown on connection 2.
publish_unknown_alias = paho_test.gen_publish(
    "", qos=0, payload="bad", proto_ver=5, properties=alias_prop(1))
# The client declared TopicAliasMaximum=2, so alias 3 is invalid.
publish_alias_over_max = paho_test.gen_publish(
    "alias/topic/three", qos=1, mid=7, payload="bad", proto_ver=5, properties=alias_prop(3))
# Alias 0 is never valid.
publish_alias_zero = paho_test.gen_publish(
    "alias/topic/zero", qos=0, payload="bad", proto_ver=5, properties=alias_prop(0))


def test_03_publish_b2c_topic_alias(server_socket, start_client):
    start_client("03-publish-b2c-topic-alias.py")

    # Connection 1: register, reference, rebind, reference again.
    (conn, _address) = server_socket.accept()
    conn.settimeout(10)

    paho_test.expect_packet(conn, "connect", connect_packet)
    conn.send(connack_packet)
    conn.send(publish_full_1 + publish_alias_1 + publish_rebind_1 + publish_alias_1_rebound)
    conn.close()

    # Connection 2: session resumed, but the alias table must be empty.
    (conn, _address) = server_socket.accept()
    conn.settimeout(10)

    paho_test.expect_packet(conn, "reconnect", reconnect_packet)
    conn.send(connack_session_present)
    conn.send(publish_unknown_alias)
    # Protocol error: the client must close the connection without
    # delivering the message.
    assert conn.recv(1024) == b""
    conn.close()

    # Connection 3: alias above the declared TopicAliasMaximum.
    (conn, _address) = server_socket.accept()
    conn.settimeout(10)

    paho_test.expect_packet(conn, "reconnect", reconnect_packet)
    conn.send(connack_session_present)
    conn.send(publish_alias_over_max)
    # Protocol error: no PUBACK for the invalid QoS 1 PUBLISH, the client
    # must close the connection.
    assert conn.recv(1024) == b""
    conn.close()

    # Connection 4: alias 0.
    (conn, _address) = server_socket.accept()
    conn.settimeout(10)

    paho_test.expect_packet(conn, "reconnect", reconnect_packet)
    conn.send(connack_session_present)
    conn.send(publish_alias_zero)
    # Protocol error: the client must close the connection.
    assert conn.recv(1024) == b""
    conn.close()
