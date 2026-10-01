"""Tests for the persistent session store (paho.mqtt.sessionstore).

Each QoS 1/2 protocol stage is exercised with a simulated process restart:
the first client is dropped mid-exchange (its socket is closed without a
DISCONNECT, modelling a crash) and a brand new Client is created against
the same store directory, exactly as a restarted process would.

The broker is faked in-process with a socketpair: the client's
`_create_socket` is swapped for one end and the test drives the other.
"""

import socket

import paho.mqtt.client as mqtt
from paho.mqtt.enums import MessageState
from paho.mqtt.sessionstore import (
    DIRECTION_IN,
    DIRECTION_OUT,
    FileSessionStore,
    PendingMessage,
)

import tests.paho_test as paho_test

CLIENT_ID = "session-store-test"


class Broker:
    """The broker end of an in-process socketpair."""

    def __init__(self):
        self.client_end, self.sock = socket.socketpair()
        self.sock.settimeout(5)

    def expect(self, name, packet):
        paho_test.expect_packet(self.sock, name, packet)

    def send(self, packet):
        self.sock.sendall(packet)

    def close(self):
        self.sock.close()


def start_client(store_dir, clean_session=False, session_present=False):
    """Create a client, connect it to a fake broker, do the CONNACK exchange."""
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        CLIENT_ID,
        clean_session=clean_session,
        session_store=FileSessionStore(store_dir),
    )
    broker = Broker()
    client._create_socket = lambda: broker.client_end
    client.connect("localhost", 1883)
    broker.expect(
        "connect",
        paho_test.gen_connect(CLIENT_ID, keepalive=60, clean_session=clean_session),
    )
    broker.send(paho_test.gen_connack(flags=1 if session_present else 0, rc=0))
    client.loop_read()
    return client, broker


def crash(client, broker):
    """Simulate an abrupt process exit: the socket dies, memory is lost."""
    if client._sock is not None:
        client._sock.close()
    broker.close()


def pending_records(store_dir):
    return FileSessionStore(store_dir).load_all()


class TestFileSessionStore:
    def test_roundtrip(self, tmp_path):
        store = FileSessionStore(tmp_path / "store")
        store.open("cid")
        record = PendingMessage(
            direction=DIRECTION_OUT,
            mid=42,
            topic=b"a/b",
            payload=b"payload",
            qos=2,
            retain=True,
            dup=True,
            state=MessageState.MQTT_MS_WAIT_FOR_PUBCOMP,
            timestamp=1.25,
            properties=b"\x00",
        )
        store.put(record)
        store.set_last_mid(42)

        # A fresh instance sees what the previous "process" wrote.
        reloaded = FileSessionStore(tmp_path / "store")
        reloaded.open("cid")
        assert reloaded.load_all() == [record]
        assert reloaded.get_last_mid() == 42

        reloaded.remove(DIRECTION_OUT, 42)
        assert reloaded.load_all() == []
        reloaded.remove(DIRECTION_OUT, 42)  # missing record is ignored

        reloaded.clear()
        assert reloaded.get_last_mid() == 0

    def test_corrupt_record_is_skipped(self, tmp_path):
        store_dir = tmp_path / "store"
        store = FileSessionStore(store_dir)
        store.open("cid")
        store.put(
            PendingMessage(
                direction=DIRECTION_IN,
                mid=1,
                topic=b"t",
                payload=b"p",
                qos=2,
                retain=False,
                dup=False,
                state=MessageState.MQTT_MS_WAIT_FOR_PUBREL,
                timestamp=0.0,
            )
        )
        (store_dir / "out-9.json").write_text("{not json")
        records = store.load_all()
        assert len(records) == 1
        assert records[0].mid == 1


class TestOutgoingQos1:
    def test_restart_before_puback_resends_dup_publish(self, tmp_path):
        store_dir = tmp_path / "store"
        client, broker = start_client(store_dir)

        info = client.publish("pub/qos1/test", "message", 1)
        broker.expect(
            "publish",
            paho_test.gen_publish("pub/qos1/test", qos=1, mid=info.mid, payload="message"),
        )
        # Crash after PUBLISH went out, before PUBACK arrived.
        crash(client, broker)

        client2, broker2 = start_client(store_dir, session_present=True)
        published = []
        client2.on_publish = lambda *args: published.append(args)
        # Same packet id, DUP set: the exchange is resumed, not restarted.
        broker2.expect(
            "re-sent publish",
            paho_test.gen_publish(
                "pub/qos1/test", qos=1, mid=info.mid, payload="message", dup=True
            ),
        )
        broker2.send(paho_test.gen_puback(info.mid))
        client2.loop_read()

        assert len(published) == 1
        assert pending_records(store_dir) == []


class TestOutgoingQos2:
    def test_restart_before_pubrec_resends_dup_publish(self, tmp_path):
        store_dir = tmp_path / "store"
        client, broker = start_client(store_dir)

        info = client.publish("pub/qos2/test", "message", 2)
        broker.expect(
            "publish",
            paho_test.gen_publish("pub/qos2/test", qos=2, mid=info.mid, payload="message"),
        )
        # Crash after PUBLISH went out, before PUBREC arrived.
        crash(client, broker)

        client2, broker2 = start_client(store_dir, session_present=True)
        broker2.expect(
            "re-sent publish",
            paho_test.gen_publish(
                "pub/qos2/test", qos=2, mid=info.mid, payload="message", dup=True
            ),
        )
        broker2.send(paho_test.gen_pubrec(info.mid))
        client2.loop_read()
        broker2.expect("pubrel", paho_test.gen_pubrel(info.mid))
        broker2.send(paho_test.gen_pubcomp(info.mid))
        client2.loop_read()

        assert pending_records(store_dir) == []

    def test_restart_before_pubcomp_resends_pubrel_only(self, tmp_path):
        store_dir = tmp_path / "store"
        client, broker = start_client(store_dir)

        info = client.publish("pub/qos2/test", "message", 2)
        broker.expect(
            "publish",
            paho_test.gen_publish("pub/qos2/test", qos=2, mid=info.mid, payload="message"),
        )
        broker.send(paho_test.gen_pubrec(info.mid))
        client.loop_read()
        broker.expect("pubrel", paho_test.gen_pubrel(info.mid))
        # Crash after PUBREL went out, before PUBCOMP arrived.
        crash(client, broker)

        client2, broker2 = start_client(store_dir, session_present=True)
        # The broker still has the session: resume with PUBREL, and do
        # NOT blindly re-send PUBLISH.
        broker2.expect("re-sent pubrel", paho_test.gen_pubrel(info.mid))
        broker2.send(paho_test.gen_pubcomp(info.mid))
        client2.loop_read()

        assert pending_records(store_dir) == []

    def test_restart_with_broker_session_lost_restarts_exchange(self, tmp_path):
        store_dir = tmp_path / "store"
        client, broker = start_client(store_dir)

        info = client.publish("pub/qos2/test", "message", 2)
        broker.expect(
            "publish",
            paho_test.gen_publish("pub/qos2/test", qos=2, mid=info.mid, payload="message"),
        )
        broker.send(paho_test.gen_pubrec(info.mid))
        client.loop_read()
        broker.expect("pubrel", paho_test.gen_pubrel(info.mid))
        crash(client, broker)

        # Session Present is 0: the broker lost the session, so the
        # half-finished QoS 2 exchange is unknown to it. The client must
        # restart from PUBLISH (DUP=1), not send a dangling PUBREL.
        client2, broker2 = start_client(store_dir, session_present=False)
        broker2.expect(
            "restarted publish",
            paho_test.gen_publish(
                "pub/qos2/test", qos=2, mid=info.mid, payload="message", dup=True
            ),
        )
        broker2.send(paho_test.gen_pubrec(info.mid))
        client2.loop_read()
        broker2.expect("pubrel", paho_test.gen_pubrel(info.mid))
        broker2.send(paho_test.gen_pubcomp(info.mid))
        client2.loop_read()

        assert pending_records(store_dir) == []


class TestIncomingQos2:
    def test_restart_before_pubrel_redelivers_once(self, tmp_path):
        store_dir = tmp_path / "store"
        received = []
        client, broker = start_client(store_dir)
        client.on_message = lambda *args: received.append(args[2])

        broker.send(paho_test.gen_publish("sub/qos2/test", qos=2, mid=7, payload="message"))
        client.loop_read()
        broker.expect("pubrec", paho_test.gen_pubrec(7))
        # Crash after PUBREC went out, before PUBREL arrived.
        crash(client, broker)

        client2, broker2 = start_client(store_dir, session_present=True)
        client2.on_message = lambda *args: received.append(args[2])

        # The broker re-sends PUBREL; the restored record lets the
        # client complete the exchange and deliver exactly once.
        broker2.send(paho_test.gen_pubrel(7))
        client2.loop_read()
        broker2.expect("pubcomp", paho_test.gen_pubcomp(7))

        assert len(received) == 1
        assert received[0].payload == b"message"
        assert received[0].mid == 7
        assert pending_records(store_dir) == []

    def test_restart_with_clean_session_drops_inbound_exchange(self, tmp_path):
        store_dir = tmp_path / "store"
        client, broker = start_client(store_dir, clean_session=True)

        broker.send(paho_test.gen_publish("sub/qos2/test", qos=2, mid=7, payload="message"))
        client.loop_read()
        broker.expect("pubrec", paho_test.gen_pubrec(7))
        crash(client, broker)

        # Clean session: neither side keeps the exchange, the persisted
        # inbound record must be discarded.
        _client2, _broker2 = start_client(store_dir, clean_session=True)
        assert pending_records(store_dir) == []


class TestPacketIdReuse:
    def test_mid_not_reused_after_restart(self, tmp_path):
        store_dir = tmp_path / "store"
        client, broker = start_client(store_dir)

        first = client.publish("pub/qos1/test", "one", 1)
        broker.expect(
            "publish",
            paho_test.gen_publish("pub/qos1/test", qos=1, mid=first.mid, payload="one"),
        )
        # Crash while the first exchange is still in flight.
        crash(client, broker)

        client2, broker2 = start_client(store_dir, session_present=True)
        broker2.expect(
            "re-sent publish",
            paho_test.gen_publish(
                "pub/qos1/test", qos=1, mid=first.mid, payload="one", dup=True
            ),
        )
        # A new publish must get a fresh packet id: the old exchange is
        # still incomplete, so its id cannot be reused.
        second = client2.publish("pub/qos1/test", "two", 1)
        assert second.mid != first.mid
        assert second.mid > first.mid
        broker2.expect(
            "second publish",
            paho_test.gen_publish("pub/qos1/test", qos=1, mid=second.mid, payload="two"),
        )

