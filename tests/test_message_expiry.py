"""Tests for local handling of the MQTT v5 Message Expiry Interval.

No real network is used: the client socket factory is replaced by a vendor
of socketpairs, the other end of which acts as a fake broker. A fake
monotonic clock is injected into the client, so expiry behaviour is tested
without any real time passing.
"""

import socket
import threading

import pytest

import paho.mqtt.client as client
import tests.mqtt5_props as mqtt5_props
import tests.paho_test as paho_test
from paho.mqtt.enums import CallbackAPIVersion, MQTTErrorCode, MQTTProtocolVersion
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

CLIENT_ID = "expiry-test-client"


class FakeClock:
    def __init__(self, now: float = 1000.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class SocketpairBroker:
    """Fake broker connected to the client through a socketpair.

    A new socketpair is created every time the client (re)connects, so
    multiple connection cycles can be exercised without any network.
    """

    def __init__(self) -> None:
        self.conn: socket.socket | None = None

    def create_connection(self) -> socket.socket:
        client_end, broker_end = socket.socketpair()
        broker_end.settimeout(5)
        self.conn = broker_end
        return client_end

    def close(self) -> None:
        if self.conn is not None:
            self.conn.close()
            self.conn = None


@pytest.fixture
def broker():
    fake = SocketpairBroker()
    yield fake
    fake.close()


def make_client(monkeypatch, fake_broker, clock, local_message_expiry=True):
    mqttc = client.Client(
        CallbackAPIVersion.VERSION2,
        CLIENT_ID,
        protocol=MQTTProtocolVersion.MQTTv5,
        local_message_expiry=local_message_expiry,
    )
    mqttc._clock = clock
    monkeypatch.setattr(
        client.Client,
        "_create_socket_connection",
        lambda self: fake_broker.create_connection(),
    )
    return mqttc


def expiry_properties(interval: int) -> Properties:
    properties = Properties(PacketTypes.PUBLISH)
    properties.MessageExpiryInterval = interval
    return properties


def expected_publish(mid, interval, qos=1, dup=False,
                     topic=b"test/expire", payload=b"payload"):
    properties = b""
    if interval is not None:
        properties = mqtt5_props.gen_uint32_prop(
            mqtt5_props.PROP_MESSAGE_EXPIRY_INTERVAL, interval)
    return paho_test.gen_publish(
        topic, qos=qos, payload=payload, mid=mid, dup=dup,
        proto_ver=5, properties=properties)


def connect(mqttc, fake_broker) -> None:
    mqttc.connect_async("localhost", 1883)
    mqttc.reconnect()
    assert fake_broker.conn.recv(1000)  # CONNECT packet
    fake_broker.conn.sendall(paho_test.gen_connack(rc=0, proto_ver=5))
    mqttc.loop_read()  # CONNACK: queued messages are (re)sent from here


def reconnect(mqttc, fake_broker) -> None:
    mqttc.reconnect()
    assert fake_broker.conn.recv(1000)  # CONNECT packet
    fake_broker.conn.sendall(paho_test.gen_connack(rc=0, proto_ver=5))
    mqttc.loop_read()


class TestLocalMessageExpiry:
    def test_remaining_seconds_sent_after_waiting_for_connection(self, monkeypatch, broker):
        clock = FakeClock()
        mqttc = make_client(monkeypatch, broker, clock)
        properties = expiry_properties(10)

        info = mqttc.publish("test/expire", "payload", qos=1, properties=properties)
        assert info.rc == MQTTErrorCode.MQTT_ERR_NO_CONN

        clock.advance(4)
        connect(mqttc, broker)

        paho_test.expect_packet(
            broker.conn, "publish", expected_publish(info.mid, 6))
        # The user-provided properties object is not mutated.
        assert properties.MessageExpiryInterval == 10

        broker.conn.sendall(paho_test.gen_puback(info.mid, proto_ver=5))
        mqttc.loop_read()
        assert info.is_published()
        assert info.rc == MQTTErrorCode.MQTT_ERR_SUCCESS

    @pytest.mark.parametrize("delay,expired", [(5.0, True), (4.5, False)])
    def test_expiry_boundary_at_deadline(self, monkeypatch, broker, delay, expired):
        clock = FakeClock()
        mqttc = make_client(monkeypatch, broker, clock)

        info = mqttc.publish("test/expire", "payload", qos=1,
                             properties=expiry_properties(5))
        clock.advance(delay)
        connect(mqttc, broker)

        if expired:
            # Exactly at the deadline: nothing goes on the wire.
            paho_test.expect_no_packet(broker.conn, delay=0.2)
            assert info.rc == MQTTErrorCode.MQTT_ERR_MESSAGE_EXPIRED
            with pytest.raises(RuntimeError, match=r"[Ee]xpired"):
                info.wait_for_publish()
            with pytest.raises(RuntimeError):
                info.is_published()
        else:
            # Just before the deadline: remaining time is rounded up to 1s.
            paho_test.expect_packet(
                broker.conn, "publish", expected_publish(info.mid, 1))
            broker.conn.sendall(paho_test.gen_puback(info.mid, proto_ver=5))
            mqttc.loop_read()
            assert info.is_published()

    def test_no_expiry_property_unaffected(self, monkeypatch, broker):
        clock = FakeClock()
        mqttc = make_client(monkeypatch, broker, clock)

        info = mqttc.publish("test/expire", "payload", qos=1)
        clock.advance(1000)
        connect(mqttc, broker)

        paho_test.expect_packet(
            broker.conn, "publish", expected_publish(info.mid, None))

    def test_zero_interval_expires_at_publish_time(self, monkeypatch, broker):
        clock = FakeClock()
        mqttc = make_client(monkeypatch, broker, clock)
        connect(mqttc, broker)

        info = mqttc.publish("test/expire", "payload", qos=1,
                             properties=expiry_properties(0))

        paho_test.expect_no_packet(broker.conn, delay=0.2)
        assert info.rc == MQTTErrorCode.MQTT_ERR_MESSAGE_EXPIRED
        with pytest.raises(RuntimeError, match=r"[Ee]xpired"):
            info.wait_for_publish()

    def test_disabled_feature_keeps_full_interval(self, monkeypatch, broker):
        clock = FakeClock()
        mqttc = make_client(monkeypatch, broker, clock,
                            local_message_expiry=False)

        info = mqttc.publish("test/expire", "payload", qos=1,
                             properties=expiry_properties(10))
        clock.advance(1000)
        connect(mqttc, broker)

        paho_test.expect_packet(
            broker.conn, "publish", expected_publish(info.mid, 10))

    def test_queued_by_inflight_limit_gets_remaining_interval(self, monkeypatch, broker):
        clock = FakeClock()
        mqttc = make_client(monkeypatch, broker, clock)
        mqttc.max_inflight_messages_set(1)
        connect(mqttc, broker)

        first = mqttc.publish("test/one", "one", qos=1)
        paho_test.expect_packet(
            broker.conn, "publish",
            expected_publish(first.mid, None, topic=b"test/one", payload=b"one"))

        second = mqttc.publish("test/two", "two", qos=1,
                               properties=expiry_properties(10))
        assert second.rc == MQTTErrorCode.MQTT_ERR_SUCCESS
        # Held back by the inflight limit: nothing sent yet.
        paho_test.expect_no_packet(broker.conn, delay=0.2)

        clock.advance(3)
        broker.conn.sendall(paho_test.gen_puback(first.mid, proto_ver=5))
        mqttc.loop_read()  # frees an inflight slot -> second is sent

        paho_test.expect_packet(
            broker.conn, "publish",
            expected_publish(second.mid, 7, topic=b"test/two", payload=b"two"))

    def test_expired_while_throttled_by_inflight_limit(self, monkeypatch, broker):
        clock = FakeClock()
        mqttc = make_client(monkeypatch, broker, clock)
        mqttc.max_inflight_messages_set(1)
        connect(mqttc, broker)

        first = mqttc.publish("test/one", "one", qos=1)
        paho_test.expect_packet(
            broker.conn, "publish",
            expected_publish(first.mid, None, topic=b"test/one", payload=b"one"))

        second = mqttc.publish("test/two", "two", qos=1,
                               properties=expiry_properties(5))

        clock.advance(10)
        broker.conn.sendall(paho_test.gen_puback(first.mid, proto_ver=5))
        mqttc.loop_read()  # frees an inflight slot -> second expires instead

        paho_test.expect_no_packet(broker.conn, delay=0.2)
        assert second.rc == MQTTErrorCode.MQTT_ERR_MESSAGE_EXPIRED
        with pytest.raises(RuntimeError, match=r"[Ee]xpired"):
            second.is_published()

    def test_wait_for_publish_wakes_on_local_expiry(self, monkeypatch, broker):
        clock = FakeClock()
        mqttc = make_client(monkeypatch, broker, clock)
        mqttc.max_inflight_messages_set(1)
        connect(mqttc, broker)

        first = mqttc.publish("test/one", "one", qos=1)
        paho_test.expect_packet(
            broker.conn, "publish",
            expected_publish(first.mid, None, topic=b"test/one", payload=b"one"))

        second = mqttc.publish("test/two", "two", qos=1,
                               properties=expiry_properties(5))
        assert second.rc == MQTTErrorCode.MQTT_ERR_SUCCESS

        failures = []
        finished = threading.Event()

        def waiter():
            try:
                second.wait_for_publish()  # no timeout: must not wait forever
            except Exception as exc:
                failures.append(exc)
            finally:
                finished.set()

        thread = threading.Thread(target=waiter)
        thread.start()
        try:
            clock.advance(10)
            broker.conn.sendall(paho_test.gen_puback(first.mid, proto_ver=5))
            mqttc.loop_read()
            assert finished.wait(timeout=5)
            assert len(failures) == 1
            assert second.rc == MQTTErrorCode.MQTT_ERR_MESSAGE_EXPIRED
        finally:
            thread.join(timeout=5)

    def test_reconnects_keep_decreasing_remaining_interval(self, monkeypatch, broker):
        clock = FakeClock()
        mqttc = make_client(monkeypatch, broker, clock)

        info = mqttc.publish("test/expire", "payload", qos=1,
                             properties=expiry_properties(30))

        connect(mqttc, broker)
        paho_test.expect_packet(
            broker.conn, "publish", expected_publish(info.mid, 30, dup=False))

        # Connection lost before PUBACK: the message enters (and later leaves)
        # the outgoing queue again, but its deadline does not reset.
        broker.close()
        clock.advance(10)
        reconnect(mqttc, broker)
        paho_test.expect_packet(
            broker.conn, "publish", expected_publish(info.mid, 20, dup=True))

        broker.close()
        clock.advance(15)
        reconnect(mqttc, broker)
        paho_test.expect_packet(
            broker.conn, "publish", expected_publish(info.mid, 5, dup=True))

        broker.conn.sendall(paho_test.gen_puback(info.mid, proto_ver=5))
        mqttc.loop_read()
        assert info.is_published()
        assert info.rc == MQTTErrorCode.MQTT_ERR_SUCCESS

    def test_qos2_handshake_completes_even_if_expired(self, monkeypatch, broker):
        clock = FakeClock()
        mqttc = make_client(monkeypatch, broker, clock)
        connect(mqttc, broker)

        info = mqttc.publish("test/expire", "payload", qos=2,
                             properties=expiry_properties(5))
        paho_test.expect_packet(
            broker.conn, "publish", expected_publish(info.mid, 5, qos=2))

        clock.advance(10)  # expires while the QoS 2 exchange is in progress

        broker.conn.sendall(paho_test.gen_pubrec(info.mid, proto_ver=5))
        mqttc.loop_read()
        paho_test.expect_packet(
            broker.conn, "pubrel",
            paho_test.gen_pubrel(info.mid, proto_ver=5))

        broker.conn.sendall(paho_test.gen_pubcomp(info.mid, proto_ver=5))
        mqttc.loop_read()
        assert info.is_published()
        assert info.rc == MQTTErrorCode.MQTT_ERR_SUCCESS
        assert info.mid not in mqttc._out_messages

    def test_reconnect_in_handshake_after_expiry_completes_with_zero(self, monkeypatch, broker):
        clock = FakeClock()
        mqttc = make_client(monkeypatch, broker, clock)

        info = mqttc.publish("test/expire", "payload", qos=1,
                             properties=expiry_properties(5))
        connect(mqttc, broker)
        paho_test.expect_packet(
            broker.conn, "publish", expected_publish(info.mid, 5))

        broker.close()
        clock.advance(10)
        reconnect(mqttc, broker)
        # The dup resend is not deleted: it goes out with 0 remaining seconds
        # rather than regaining a full interval or vanishing mid-handshake.
        paho_test.expect_packet(
            broker.conn, "publish", expected_publish(info.mid, 0, dup=True))

        broker.conn.sendall(paho_test.gen_puback(info.mid, proto_ver=5))
        mqttc.loop_read()
        assert info.is_published()

    def test_caller_distinguishes_success_rejection_and_expiry(self, monkeypatch, broker):
        clock = FakeClock()
        mqttc = make_client(monkeypatch, broker, clock)
        mqttc.max_inflight_messages_set(1)
        connect(mqttc, broker)

        # 1. Normal completion.
        ok_info = mqttc.publish("test/ok", "ok", qos=1)
        paho_test.expect_packet(
            broker.conn, "publish",
            expected_publish(ok_info.mid, None, topic=b"test/ok", payload=b"ok"))
        broker.conn.sendall(paho_test.gen_puback(ok_info.mid, proto_ver=5))
        mqttc.loop_read()
        assert ok_info.wait_for_publish() is None
        assert ok_info.rc == MQTTErrorCode.MQTT_ERR_SUCCESS

        # 2. Peer rejection: the broker answers PUBACK with a failure reason.
        rejected_reasons = []

        def on_publish(_client, _userdata, _mid, reason, _properties):
            rejected_reasons.append(reason)

        mqttc.on_publish = on_publish
        rejected_info = mqttc.publish("test/rejected", "no", qos=1)
        paho_test.expect_packet(
            broker.conn, "publish",
            expected_publish(rejected_info.mid, None,
                             topic=b"test/rejected", payload=b"no"))
        broker.conn.sendall(paho_test.gen_puback(
            rejected_info.mid, proto_ver=5, reason_code=0x87))
        mqttc.loop_read()
        assert rejected_info.wait_for_publish() is None
        assert rejected_info.rc != MQTTErrorCode.MQTT_ERR_MESSAGE_EXPIRED
        assert len(rejected_reasons) == 1
        assert rejected_reasons[0].is_failure

        # 3. Local expiry: the message never reaches the broker.
        holder = mqttc.publish("test/hold", "hold", qos=1)
        paho_test.expect_packet(
            broker.conn, "publish",
            expected_publish(holder.mid, None, topic=b"test/hold", payload=b"hold"))
        expired_info = mqttc.publish("test/late", "late", qos=1,
                                     properties=expiry_properties(5))
        clock.advance(6)
        broker.conn.sendall(paho_test.gen_puback(holder.mid, proto_ver=5))
        mqttc.loop_read()
        paho_test.expect_no_packet(broker.conn, delay=0.2)
        assert expired_info.rc == MQTTErrorCode.MQTT_ERR_MESSAGE_EXPIRED
        with pytest.raises(RuntimeError, match=r"[Ee]xpired"):
            expired_info.wait_for_publish()
