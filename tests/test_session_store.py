# Tests for the optional durable session store (paho.mqtt.sessionstore).
#
# A "process restart" is simulated by throwing away the whole Client
# object (its loop is stopped and the broker end of the socket is closed
# without DISCONNECT) and creating a brand new Client that points at the
# same store directory.  Only the durable store survives, exactly like a
# real process crash.
#
# No network ports are bound: the client socket is an in-process
# socketpair(), the fake broker speaks raw MQTT packets on the other end.

import socket
import threading
import time

import paho.mqtt.client as mqtt
import pytest
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties
from paho.mqtt.sessionstore import (
    FileSessionStore,
    InboundStage,
    OutboundStage,
    StoredInboundMessage,
    StoredOutboundMessage,
)

import tests.mqtt5_props as mqtt5_props
import tests.paho_test as paho_test

CLIENT_ID = "session-store-test"

try:
    _probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    _probe.bind(("127.0.0.1", 0))
    _probe.close()
except OSError:
    # Environments where even loopback TCP is restricted: paho's wakeup
    # socketpair falls back to the real (AF_UNIX) socketpair().
    mqtt._socketpair_compat = socket.socketpair


class SocketpairClient(mqtt.Client):
    """MQTT client connected over an in-process socketpair."""

    def __init__(self, *args, **kwargs):
        self.broker_sock = None
        super().__init__(*args, **kwargs)

    def _create_socket(self):
        client_end, broker_end = socket.socketpair()
        self.broker_sock = broker_end
        return client_end


class Broker:
    """Scriptable fake broker speaking raw MQTT on the client socket."""

    def __init__(self):
        self.conn = None

    def attach(self, sock):
        self.conn = sock
        self.conn.settimeout(10)

    def expect(self, name, packet):
        paho_test.expect_packet(self.conn, name, packet)

    def send(self, packet):
        self.conn.sendall(packet)

    def drop(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None


def make_client(store_dir, **kwargs):
    return SocketpairClient(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=CLIENT_ID,
        clean_session=False,
        reconnect_on_failure=False,
        session_store=FileSessionStore(store_dir),
        **kwargs,
    )


def start_client(mqttc, broker, session_present):
    mqttc.connect("localhost", 1883)
    broker.attach(mqttc.broker_sock)
    mqttc.loop_start()
    broker.expect(
        "connect",
        paho_test.gen_connect(CLIENT_ID, keepalive=60, clean_session=False),
    )
    broker.send(paho_test.gen_connack(flags=1 if session_present else 0, rc=0))


def kill_client(mqttc, broker):
    # Simulate a process crash: the network dies without DISCONNECT and
    # the whole client object is abandoned.
    broker.drop()
    mqttc.loop_stop()


def store_records(store_dir):
    return FileSessionStore(store_dir).load()


def wait_for_store(store_dir, out_count=None, in_count=None, timeout=5.0):
    deadline = time.monotonic() + timeout
    while True:
        outbound, inbound = store_records(store_dir)
        if (out_count is None or len(outbound) == out_count) and (
            in_count is None or len(inbound) == in_count
        ):
            return outbound, inbound
        if time.monotonic() > deadline:
            raise AssertionError(
                f"store did not reach out={out_count} in={in_count}: "
                f"out={outbound} in={inbound}"
            )
        time.sleep(0.02)


@pytest.fixture
def broker():
    broker = Broker()
    yield broker
    broker.drop()


class TestFileSessionStore:
    def test_roundtrip(self, tmp_path):
        store = FileSessionStore(tmp_path / "session")
        assert store.load() == ([], [])

        out = StoredOutboundMessage(
            mid=7,
            topic=b"a/topic",
            payload=b"\x00binary payload\xff",
            qos=2,
            retain=True,
            stage=OutboundStage.WAIT_PUBREC,
            properties=b"\x01\x02",
        )
        store.save_outbound(out)
        inbound = StoredInboundMessage(
            mid=9, topic=b"in/topic", payload=b"inbound", retain=False, dup=True
        )
        store.save_inbound(inbound)

        loaded_out, loaded_in = store.load()
        assert loaded_out == [out]
        assert loaded_in == [inbound]
        assert loaded_in[0].stage is InboundStage.WAIT_PUBREL

    def test_stage_update_and_delete(self, tmp_path):
        store = FileSessionStore(tmp_path / "session")
        record = StoredOutboundMessage(
            mid=1, topic=b"t", payload=b"p", qos=2, retain=False,
            stage=OutboundStage.WAIT_PUBREC,
        )
        store.save_outbound(record)
        record.stage = OutboundStage.WAIT_PUBCOMP
        store.save_outbound(record)
        outbound, _ = store.load()
        assert [r.stage for r in outbound] == [OutboundStage.WAIT_PUBCOMP]

        store.delete_outbound(1)
        store.delete_outbound(1)  # no-op
        store.delete_inbound(1)  # no-op
        assert store.load() == ([], [])

    def test_clear(self, tmp_path):
        store = FileSessionStore(tmp_path / "session")
        store.save_outbound(StoredOutboundMessage(
            mid=1, topic=b"t", payload=b"p", qos=1, retain=False,
            stage=OutboundStage.WAIT_PUBACK,
        ))
        store.save_inbound(StoredInboundMessage(mid=2, topic=b"t", payload=b"p"))
        store.clear()
        assert store.load() == ([], [])

    def test_crash_mid_write_leaves_no_temp_files(self, tmp_path):
        store_dir = tmp_path / "session"
        store = FileSessionStore(store_dir)
        store.save_outbound(StoredOutboundMessage(
            mid=3, topic=b"t", payload=b"p", qos=1, retain=False,
            stage=OutboundStage.WAIT_PUBACK,
        ))
        # A torn write from a crashed process: ignored and cleaned up.
        (store_dir / "out-00004.json.1234.tmp").write_text('{"mid": 4')
        outbound, _ = store.load()
        assert [r.mid for r in outbound] == [3]
        assert not list(store_dir.glob("*.tmp"))


class TestOutboundResume:
    def test_qos1_restart_waiting_puback(self, tmp_path, broker):
        store_dir = tmp_path / "session"

        mqttc = make_client(store_dir)
        start_client(mqttc, broker, session_present=False)
        mqttc.publish("out/topic", "first", qos=1)
        broker.expect("publish", paho_test.gen_publish(
            "out/topic", qos=1, mid=1, payload="first"))
        # Crash after PUBLISH was sent, before PUBACK.
        outbound, _ = wait_for_store(store_dir, out_count=1)
        assert outbound[0].stage is OutboundStage.WAIT_PUBACK
        kill_client(mqttc, broker)

        mqttc = make_client(store_dir)
        start_client(mqttc, broker, session_present=True)
        # The exchange is resumed: same packet id, DUP set.
        broker.expect("republished publish", paho_test.gen_publish(
            "out/topic", qos=1, mid=1, payload="first", dup=True))
        broker.send(paho_test.gen_puback(1))
        wait_for_store(store_dir, out_count=0)

        # The packet id of the completed exchange is free again, and the
        # next message gets a fresh, unused one.
        mqttc.publish("out/topic", "second", qos=1)
        broker.expect("second publish", paho_test.gen_publish(
            "out/topic", qos=1, mid=2, payload="second"))
        broker.send(paho_test.gen_puback(2))
        wait_for_store(store_dir, out_count=0)
        kill_client(mqttc, broker)

    def test_qos2_restart_waiting_pubrec(self, tmp_path, broker):
        store_dir = tmp_path / "session"

        mqttc = make_client(store_dir)
        start_client(mqttc, broker, session_present=False)
        mqttc.publish("out/topic", "payload", qos=2)
        broker.expect("publish", paho_test.gen_publish(
            "out/topic", qos=2, mid=1, payload="payload"))
        outbound, _ = wait_for_store(store_dir, out_count=1)
        assert outbound[0].stage is OutboundStage.WAIT_PUBREC
        kill_client(mqttc, broker)

        mqttc = make_client(store_dir)
        start_client(mqttc, broker, session_present=True)
        broker.expect("republished publish", paho_test.gen_publish(
            "out/topic", qos=2, mid=1, payload="payload", dup=True))
        broker.send(paho_test.gen_pubrec(1))
        broker.expect("pubrel", paho_test.gen_pubrel(1))
        outbound, _ = wait_for_store(store_dir, out_count=1)
        assert outbound[0].stage is OutboundStage.WAIT_PUBCOMP
        broker.send(paho_test.gen_pubcomp(1))
        wait_for_store(store_dir, out_count=0)
        kill_client(mqttc, broker)

    def test_qos2_restart_waiting_pubcomp(self, tmp_path, broker):
        store_dir = tmp_path / "session"

        mqttc = make_client(store_dir)
        start_client(mqttc, broker, session_present=False)
        mqttc.publish("out/topic", "payload", qos=2)
        broker.expect("publish", paho_test.gen_publish(
            "out/topic", qos=2, mid=1, payload="payload"))
        broker.send(paho_test.gen_pubrec(1))
        broker.expect("pubrel", paho_test.gen_pubrel(1))
        # Crash after PUBREL was sent, before PUBCOMP.
        kill_client(mqttc, broker)

        mqttc = make_client(store_dir)
        start_client(mqttc, broker, session_present=True)
        # The exchange must resume with PUBREL - the PUBLISH must NOT be
        # sent again, or the broker would process the message twice.
        broker.expect("resent pubrel", paho_test.gen_pubrel(1))
        broker.send(paho_test.gen_pubcomp(1))
        wait_for_store(store_dir, out_count=0)
        kill_client(mqttc, broker)

    def test_queued_message_survives_restart(self, tmp_path, broker):
        store_dir = tmp_path / "session"

        mqttc = make_client(store_dir)
        mqttc.max_inflight_messages_set(1)
        start_client(mqttc, broker, session_present=False)
        mqttc.publish("out/topic", "one", qos=1)
        mqttc.publish("out/topic", "two", qos=1)
        broker.expect("first publish", paho_test.gen_publish(
            "out/topic", qos=1, mid=1, payload="one"))
        outbound, _ = wait_for_store(store_dir, out_count=2)
        stages = {r.mid: r.stage for r in outbound}
        assert stages == {1: OutboundStage.WAIT_PUBACK, 2: OutboundStage.QUEUED}
        kill_client(mqttc, broker)

        mqttc = make_client(store_dir)
        mqttc.max_inflight_messages_set(1)
        start_client(mqttc, broker, session_present=True)
        broker.expect("republished first", paho_test.gen_publish(
            "out/topic", qos=1, mid=1, payload="one", dup=True))
        broker.send(paho_test.gen_puback(1))
        # Never sent before the crash: goes out as a fresh exchange.
        broker.expect("second publish", paho_test.gen_publish(
            "out/topic", qos=1, mid=2, payload="two"))
        broker.send(paho_test.gen_puback(2))
        wait_for_store(store_dir, out_count=0)
        kill_client(mqttc, broker)


class TestInboundResume:
    def test_qos2_restart_waiting_pubrel(self, tmp_path, broker):
        store_dir = tmp_path / "session"
        received = []

        mqttc = make_client(store_dir)
        start_client(mqttc, broker, session_present=False)
        broker.send(paho_test.gen_publish(
            "in/topic", qos=2, mid=7, payload="inbound-data"))
        broker.expect("pubrec", paho_test.gen_pubrec(7))
        # Crash after PUBREC was sent, before PUBREL arrived.
        _, inbound = wait_for_store(store_dir, in_count=1)
        assert inbound[0].stage is InboundStage.WAIT_PUBREL
        assert inbound[0].payload == b"inbound-data"
        kill_client(mqttc, broker)

        delivered = threading.Event()

        def on_message(mqttc, userdata, message):
            received.append((message.topic, message.payload))
            delivered.set()

        mqttc = make_client(store_dir)
        mqttc.on_message = on_message
        start_client(mqttc, broker, session_present=True)
        # The broker re-sends PUBREL; the stored message is delivered and
        # the exchange completes.
        broker.send(paho_test.gen_pubrel(7))
        broker.expect("pubcomp", paho_test.gen_pubcomp(7))
        assert delivered.wait(5)
        assert received == [("in/topic", b"inbound-data")]
        wait_for_store(store_dir, in_count=0)
        kill_client(mqttc, broker)

    def test_completed_inbound_leaves_no_record(self, tmp_path, broker):
        store_dir = tmp_path / "session"
        delivered = threading.Event()

        mqttc = make_client(store_dir)
        mqttc.on_message = lambda mqttc, userdata, message: delivered.set()
        start_client(mqttc, broker, session_present=False)
        broker.send(paho_test.gen_publish(
            "in/topic", qos=2, mid=7, payload="data"))
        broker.expect("pubrec", paho_test.gen_pubrec(7))
        broker.send(paho_test.gen_pubrel(7))
        broker.expect("pubcomp", paho_test.gen_pubcomp(7))
        assert delivered.wait(5)
        wait_for_store(store_dir, in_count=0)
        kill_client(mqttc, broker)


class TestFreshSession:
    def test_session_not_present_restarts_exchanges(self, tmp_path, broker):
        store_dir = tmp_path / "session"
        received = []
        delivered = threading.Event()

        def on_message(mqttc, userdata, message):
            received.append(message.payload)
            delivered.set()

        mqttc = make_client(store_dir)
        mqttc.on_message = on_message
        start_client(mqttc, broker, session_present=False)
        mqttc.publish("out/topic", "payload", qos=1)
        broker.expect("publish", paho_test.gen_publish(
            "out/topic", qos=1, mid=1, payload="payload"))
        broker.send(paho_test.gen_publish(
            "in/topic", qos=2, mid=9, payload="inbound"))
        broker.expect("pubrec", paho_test.gen_pubrec(9))
        wait_for_store(store_dir, out_count=1, in_count=1)
        kill_client(mqttc, broker)

        mqttc = make_client(store_dir)
        mqttc.on_message = on_message
        # The broker lost the session (Session Present = 0): the outbound
        # exchange is re-established as a NEW exchange (DUP=0), not
        # blindly continued.
        start_client(mqttc, broker, session_present=False)
        broker.expect("fresh publish", paho_test.gen_publish(
            "out/topic", qos=1, mid=1, payload="payload"))
        broker.send(paho_test.gen_puback(1))
        # The stale inbound record was dropped: if the broker redelivers
        # the QoS 2 message as a new exchange it is processed normally.
        wait_for_store(store_dir, out_count=0, in_count=0)
        broker.send(paho_test.gen_publish(
            "in/topic", qos=2, mid=9, payload="inbound"))
        broker.expect("pubrec", paho_test.gen_pubrec(9))
        broker.send(paho_test.gen_pubrel(9))
        broker.expect("pubcomp", paho_test.gen_pubcomp(9))
        assert delivered.wait(5)
        assert received == [b"inbound"]
        wait_for_store(store_dir, out_count=0, in_count=0)
        kill_client(mqttc, broker)


class TestV5Session:
    def test_qos2_restart_preserves_properties(self, tmp_path, broker):
        store_dir = tmp_path / "session"
        user_prop = mqtt5_props.gen_string_pair_prop(
            mqtt5_props.PROP_USER_PROPERTY, "key", "value")

        def make_v5_client():
            return SocketpairClient(
                mqtt.CallbackAPIVersion.VERSION2,
                client_id=CLIENT_ID,
                protocol=mqtt.MQTTv5,
                reconnect_on_failure=False,
                session_store=FileSessionStore(store_dir),
            )

        def start_v5(mqttc, session_present):
            mqttc.connect("localhost", 1883, clean_start=False)
            broker.attach(mqttc.broker_sock)
            mqttc.loop_start()
            broker.expect("connect", paho_test.gen_connect(
                CLIENT_ID, keepalive=60, clean_session=False,
                proto_ver=5, properties=None))
            broker.send(paho_test.gen_connack(
                flags=1 if session_present else 0, rc=0, proto_ver=5))

        props = Properties(PacketTypes.PUBLISH)
        props.UserProperty = ("key", "value")

        mqttc = make_v5_client()
        start_v5(mqttc, session_present=False)
        mqttc.publish("out/topic", "v5-payload", qos=2, properties=props)
        broker.expect("publish", paho_test.gen_publish(
            "out/topic", qos=2, mid=1, payload="v5-payload",
            proto_ver=5, properties=user_prop))
        kill_client(mqttc, broker)

        mqttc = make_v5_client()
        start_v5(mqttc, session_present=True)
        # Resumed with DUP=1 and the original v5 properties intact.
        broker.expect("republished publish", paho_test.gen_publish(
            "out/topic", qos=2, mid=1, payload="v5-payload", dup=True,
            proto_ver=5, properties=user_prop))
        broker.send(paho_test.gen_pubrec(1, proto_ver=5))
        broker.expect("pubrel", paho_test.gen_pubrel(1, proto_ver=5))
        broker.send(paho_test.gen_pubcomp(1, proto_ver=5))
        wait_for_store(store_dir, out_count=0)
        kill_client(mqttc, broker)
