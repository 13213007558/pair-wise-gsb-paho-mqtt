"""
Deterministic tests for MQTT v5.0 enhanced authentication (AUTH packet).

The fake broker drives a two-round challenge/response exchange; the
"cryptography" is done by the application callback (on_auth), the client
library only transports Authentication Data and enforces the protocol
phases.
"""

import hashlib
import struct
import threading
import time

import paho.mqtt.client as client
from paho.mqtt.enums import (
    CallbackAPIVersion,
    MQTTErrorCode,
    MQTTProtocolVersion,
    _AuthState,
)
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties
from paho.mqtt.reasoncodes import ReasonCode

import tests.mqtt5_props as mqtt5_props
import tests.paho_test as paho_test

# Import test fixture
from tests.testsupport.broker import FakeBroker, fake_broker  # noqa: F401

AUTH_METHOD = "TEST-CHALLENGE"
SECRET = b"shared-secret"

REASON_CONTINUE = 24   # Continue authentication
REASON_REAUTH = 25     # Re-authenticate
REASON_SUCCESS = 0     # Success


def gen_binary_prop(identifier, data):
    return struct.pack(f"!BH{len(data)}s", identifier, len(data), data)


def auth_method_prop(method=AUTH_METHOD):
    return mqtt5_props.gen_string_prop(mqtt5_props.PROP_AUTHENTICATION_METHOD, method)


def auth_data_prop(data):
    return gen_binary_prop(mqtt5_props.PROP_AUTHENTICATION_DATA, data)


def auth_props(method=AUTH_METHOD, data=None):
    props = auth_method_prop(method)
    if data is not None:
        props += auth_data_prop(data)
    return props


def connect_properties():
    props = Properties(PacketTypes.CONNECT)
    props.AuthenticationMethod = AUTH_METHOD
    return props


def app_proof(challenge: bytes) -> bytes:
    # The password algorithm is the application's business; the client
    # just transports the resulting Authentication Data.
    return hashlib.sha256(challenge + SECRET).digest()


def wait_for_auth_state(mqttc, state, timeout=5):
    deadline = time.time() + timeout
    while mqttc._auth_state != state:
        assert time.time() < deadline, f"auth state did not become {state}"


class AuthCallbacks:
    """Application side of the exchange: answers challenges and records
    what the client asked for."""

    def __init__(self):
        self.seen = []
        self.connected = threading.Event()
        self.disconnected = threading.Event()
        self.disconnect_reason = None
        self.disconnect_flags = None
        self.published = threading.Event()
        self.messages = []

    def on_auth(self, mqttc, userdata, reason_code, properties):
        self.seen.append((str(reason_code), getattr(properties, "AuthenticationData", None)))
        if str(reason_code) == "Re-authenticate":
            return app_proof(b"reauth-init")
        return app_proof(properties.AuthenticationData)

    def on_connect(self, mqttc, userdata, flags, reason_code, properties):
        if reason_code == 0:
            self.connected.set()

    def on_disconnect(self, mqttc, userdata, disconnect_flags, reason_code, properties):
        self.disconnect_flags = disconnect_flags
        self.disconnect_reason = reason_code
        self.disconnected.set()

    def on_publish(self, mqttc, userdata, mid, reason_code, properties):
        self.published.set()

    def on_message(self, mqttc, userdata, message):
        self.messages.append(message)

    def register(self, mqttc):
        mqttc.on_auth = self.on_auth
        mqttc.on_connect = self.on_connect
        mqttc.on_disconnect = self.on_disconnect
        mqttc.on_publish = self.on_publish
        mqttc.on_message = self.on_message


def make_client(client_id, callbacks):
    mqttc = client.Client(
        CallbackAPIVersion.VERSION2,
        client_id,
        protocol=MQTTProtocolVersion.MQTTv5,
    )
    callbacks.register(mqttc)
    return mqttc


class TestInitialAuth:
    """Broker continues the authentication after CONNECT with AUTH packets."""

    def test_two_round_challenge(self, fake_broker: FakeBroker):
        cb = AuthCallbacks()
        mqttc = make_client("auth-initial", cb)

        mqttc.connect_async("localhost", fake_broker.port, properties=connect_properties())
        mqttc.loop_start()
        try:
            fake_broker.start()

            connect_packet = paho_test.gen_connect(
                "auth-initial", keepalive=60, proto_ver=5,
                properties=auth_props())
            fake_broker.expect_packet("connect", connect_packet)

            # The CONNECT/CONNACK phase constraints still apply while the
            # initial exchange is running.
            assert not mqttc.is_connected()
            assert mqttc.reauthenticate() == MQTTErrorCode.MQTT_ERR_NO_CONN

            # First challenge round.
            fake_broker.send_packet(paho_test.gen_auth(
                REASON_CONTINUE, properties=auth_props(data=b"challenge-1")))
            fake_broker.expect_packet("auth response 1", paho_test.gen_auth(
                REASON_CONTINUE, properties=auth_props(data=app_proof(b"challenge-1"))))

            # Second challenge round.
            fake_broker.send_packet(paho_test.gen_auth(
                REASON_CONTINUE, properties=auth_props(data=b"challenge-2")))
            fake_broker.expect_packet("auth response 2", paho_test.gen_auth(
                REASON_CONTINUE, properties=auth_props(data=app_proof(b"challenge-2"))))

            # Broker accepts: CONNACK ends the initial exchange.
            fake_broker.send_packet(paho_test.gen_connack(rc=0, proto_ver=5))
            assert cb.connected.wait(5)
            assert mqttc.is_connected()

            assert cb.seen == [
                ("Continue authentication", b"challenge-1"),
                ("Continue authentication", b"challenge-2"),
            ]

            mqttc.disconnect()
            fake_broker.expect_packet("disconnect", paho_test.gen_disconnect(proto_ver=5))
        finally:
            mqttc.loop_stop()

        packet_in = fake_broker.receive_packet(1)
        assert not packet_in  # Check connection is closed

    def test_auth_with_external_event_loop(self, fake_broker: FakeBroker):
        """Same exchange, but the application drives loop_read/loop_write
        itself: the client must queue the AUTH response and let the
        application write it, instead of running a nested loop."""
        cb = AuthCallbacks()
        mqttc = make_client("auth-external-loop", cb)

        registered_write = []
        mqttc.on_socket_register_write = lambda c, u, s: registered_write.append(s)

        mqttc.connect("localhost", fake_broker.port, properties=connect_properties())
        fake_broker.start()

        # CONNECT is queued, the external loop writes it.
        assert mqttc.want_write()
        mqttc.loop_write()
        connect_packet = paho_test.gen_connect(
            "auth-external-loop", keepalive=60, proto_ver=5,
            properties=auth_props())
        fake_broker.expect_packet("connect", connect_packet)

        fake_broker.send_packet(paho_test.gen_auth(
            REASON_CONTINUE, properties=auth_props(data=b"challenge-1")))
        mqttc.loop_read()

        # The response must have been queued, not written from inside the
        # read handling: the external event loop stays in charge.
        assert mqttc.want_write()
        mqttc.loop_write()
        fake_broker.expect_packet("auth response 1", paho_test.gen_auth(
            REASON_CONTINUE, properties=auth_props(data=app_proof(b"challenge-1"))))

        fake_broker.send_packet(paho_test.gen_connack(rc=0, proto_ver=5))
        mqttc.loop_read()
        assert cb.connected.wait(5)

        mqttc.disconnect()
        mqttc.loop_write()
        fake_broker.expect_packet("disconnect", paho_test.gen_disconnect(proto_ver=5))


class TestReauthenticate:
    """Application-initiated re-authentication on an established connection,
    interleaved with normal traffic."""

    def _connect(self, mqttc, cb, fake_broker, client_id):
        mqttc.connect_async("localhost", fake_broker.port, properties=connect_properties())
        mqttc.loop_start()
        fake_broker.start()
        connect_packet = paho_test.gen_connect(
            client_id, keepalive=60, proto_ver=5, properties=auth_props())
        fake_broker.expect_packet("connect", connect_packet)
        fake_broker.send_packet(paho_test.gen_connack(rc=0, proto_ver=5))
        assert cb.connected.wait(5)

    def test_reauthenticate_interleaved_traffic(self, fake_broker: FakeBroker):
        cb = AuthCallbacks()
        mqttc = make_client("auth-reauth", cb)

        try:
            self._connect(mqttc, cb, fake_broker, "auth-reauth")

            # Normal QoS 1 traffic before re-authentication starts.
            mqttc.publish("reauth/topic", "payload-1", qos=1)
            fake_broker.expect_packet("publish 1", paho_test.gen_publish(
                "reauth/topic", 1, payload=b"payload-1", mid=1, proto_ver=5))

            # Application initiates re-authentication.
            assert mqttc.reauthenticate() == MQTTErrorCode.MQTT_ERR_SUCCESS
            # A second attempt while the exchange runs is rejected.
            assert mqttc.reauthenticate() == MQTTErrorCode.MQTT_ERR_INVAL

            fake_broker.expect_packet("reauth request", paho_test.gen_auth(
                REASON_REAUTH, properties=auth_props(data=app_proof(b"reauth-init"))))

            # Broker challenges, client answers.
            fake_broker.send_packet(paho_test.gen_auth(
                REASON_CONTINUE, properties=auth_props(data=b"reauth-challenge")))
            fake_broker.expect_packet("reauth response", paho_test.gen_auth(
                REASON_CONTINUE, properties=auth_props(data=app_proof(b"reauth-challenge"))))

            # Interleaved: the pending QoS 1 exchange completes *during*
            # the re-authentication.
            fake_broker.send_packet(paho_test.gen_puback(1, proto_ver=5))
            assert cb.published.wait(5)

            # Interleaved: broker->client PUBLISH is delivered and PUBACKed
            # while the re-authentication is still in progress.
            fake_broker.send_packet(paho_test.gen_publish(
                "reauth/topic", 1, payload=b"from-broker", mid=7, proto_ver=5))
            fake_broker.expect_packet("puback 7", paho_test.gen_puback(7, proto_ver=5))
            deadline = time.time() + 5
            while not cb.messages:
                assert time.time() < deadline
                time.sleep(0.01)
            assert cb.messages[0].payload == b"from-broker"

            # Interleaved: another client publish keeps the packet id
            # sequence (mid 2, not reset by the AUTH exchange).
            mqttc.publish("reauth/topic", "payload-2", qos=1)
            fake_broker.expect_packet("publish 2", paho_test.gen_publish(
                "reauth/topic", 1, payload=b"payload-2", mid=2, proto_ver=5))
            fake_broker.send_packet(paho_test.gen_puback(2, proto_ver=5))

            # Broker completes the re-authentication.
            fake_broker.send_packet(paho_test.gen_auth(REASON_SUCCESS))
            wait_for_auth_state(mqttc, _AuthState.MQTT_AS_IDLE)

            # A new re-authentication is allowed again.
            assert mqttc.reauthenticate() == MQTTErrorCode.MQTT_ERR_SUCCESS
            fake_broker.expect_packet("reauth request 2", paho_test.gen_auth(
                REASON_REAUTH, properties=auth_props(data=app_proof(b"reauth-init"))))

            mqttc.disconnect()
            fake_broker.expect_packet("disconnect", paho_test.gen_disconnect(proto_ver=5))
        finally:
            mqttc.loop_stop()

        assert cb.seen == [
            ("Re-authenticate", None),
            ("Continue authentication", b"reauth-challenge"),
            ("Re-authenticate", None),
        ]

    def test_reauthenticate_preconditions(self, fake_broker: FakeBroker):
        # MQTT v3.1.1 has no AUTH packet.
        mqttc_v3 = client.Client(
            CallbackAPIVersion.VERSION2,
            "auth-pre-v3",
            protocol=MQTTProtocolVersion.MQTTv311,
        )
        assert mqttc_v3.reauthenticate() == MQTTErrorCode.MQTT_ERR_NOT_SUPPORTED

        # No Authentication Method was negotiated in CONNECT.
        mqttc_no_method = client.Client(
            CallbackAPIVersion.VERSION2,
            "auth-pre-no-method",
            protocol=MQTTProtocolVersion.MQTTv5,
        )
        mqttc_no_method.connect_async("localhost", fake_broker.port)
        assert mqttc_no_method.reauthenticate() == MQTTErrorCode.MQTT_ERR_NOT_SUPPORTED

        # Method negotiated but not connected yet.
        mqttc = make_client("auth-pre-not-connected", AuthCallbacks())
        mqttc.connect_async("localhost", fake_broker.port, properties=connect_properties())
        assert mqttc.reauthenticate() == MQTTErrorCode.MQTT_ERR_NO_CONN


class TestAuthFailures:
    """Failure modes must end in a clear connection terminal state."""

    def _start_initial_exchange(self, mqttc, fake_broker, client_id):
        mqttc.connect_async("localhost", fake_broker.port, properties=connect_properties())
        mqttc.loop_start()
        fake_broker.start()
        connect_packet = paho_test.gen_connect(
            client_id, keepalive=60, proto_ver=5, properties=auth_props())
        fake_broker.expect_packet("connect", connect_packet)

    def _assert_auth_failure_terminal_state(self, mqttc, cb, fake_broker):
        # The client closes the connection without sending further packets.
        packet_in = fake_broker.receive_packet(1)
        assert not packet_in

        assert cb.disconnected.wait(5)
        # The disconnection is client-side and reported as an auth failure.
        assert not cb.disconnect_flags.is_disconnect_packet_from_server
        assert cb.disconnect_reason == ReasonCode(
            PacketTypes.DISCONNECT, "Bad authentication method")
        wait_for_auth_state(mqttc, _AuthState.MQTT_AS_IDLE)

    def test_auth_method_mismatch(self, fake_broker: FakeBroker):
        cb = AuthCallbacks()
        mqttc = make_client("auth-bad-method", cb)

        try:
            self._start_initial_exchange(mqttc, fake_broker, "auth-bad-method")

            # The broker tries to switch the negotiated authentication
            # method mid-exchange.
            fake_broker.send_packet(paho_test.gen_auth(
                REASON_CONTINUE, properties=auth_props(method="OTHER-METHOD", data=b"x")))

            self._assert_auth_failure_terminal_state(mqttc, cb, fake_broker)
            assert cb.seen == []  # callback never invoked for the bad packet
        finally:
            mqttc.loop_stop()

    def test_auth_callback_exception(self, fake_broker: FakeBroker):
        cb = AuthCallbacks()
        mqttc = make_client("auth-cb-error", cb)

        def failing_on_auth(mqttc, userdata, reason_code, properties):
            raise RuntimeError("no credentials available")

        mqttc.on_auth = failing_on_auth

        try:
            self._start_initial_exchange(mqttc, fake_broker, "auth-cb-error")

            fake_broker.send_packet(paho_test.gen_auth(
                REASON_CONTINUE, properties=auth_props(data=b"challenge-1")))

            self._assert_auth_failure_terminal_state(mqttc, cb, fake_broker)
        finally:
            mqttc.loop_stop()

    def test_disconnect_during_auth(self, fake_broker: FakeBroker):
        cb = AuthCallbacks()
        mqttc = make_client("auth-disconnect", cb)

        try:
            self._start_initial_exchange(mqttc, fake_broker, "auth-disconnect")

            fake_broker.send_packet(paho_test.gen_auth(
                REASON_CONTINUE, properties=auth_props(data=b"challenge-1")))
            fake_broker.expect_packet("auth response 1", paho_test.gen_auth(
                REASON_CONTINUE, properties=auth_props(data=app_proof(b"challenge-1"))))

            # The broker aborts the exchange with a DISCONNECT.
            fake_broker.send_packet(paho_test.gen_disconnect(
                0x87, proto_ver=5,
                properties=mqtt5_props.gen_string_prop(
                    mqtt5_props.PROP_REASON_STRING, "auth failed")))

            assert cb.disconnected.wait(5)
            assert cb.disconnect_flags.is_disconnect_packet_from_server
            assert cb.disconnect_reason == ReasonCode(
                PacketTypes.DISCONNECT, "Not authorized")
            wait_for_auth_state(mqttc, _AuthState.MQTT_AS_IDLE)

            packet_in = fake_broker.receive_packet(1)
            assert not packet_in  # Check connection is closed
        finally:
            mqttc.loop_stop()

    def test_auth_without_negotiated_method(self, fake_broker: FakeBroker):
        cb = AuthCallbacks()
        mqttc = make_client("auth-no-method", cb)

        try:
            # Plain CONNECT without an Authentication Method.
            mqttc.connect_async("localhost", fake_broker.port)
            mqttc.loop_start()
            fake_broker.start()
            connect_packet = paho_test.gen_connect(
                "auth-no-method", keepalive=60, proto_ver=5, properties=None)
            fake_broker.expect_packet("connect", connect_packet)

            # The broker must not send AUTH packets at all.
            fake_broker.send_packet(paho_test.gen_auth(
                REASON_CONTINUE, properties=auth_props(data=b"challenge-1")))

            self._assert_auth_failure_terminal_state(mqttc, cb, fake_broker)
            assert cb.seen == []
        finally:
            mqttc.loop_stop()

    def test_unexpected_auth_success(self, fake_broker: FakeBroker):
        cb = AuthCallbacks()
        mqttc = make_client("auth-stray-success", cb)

        try:
            self._start_initial_exchange(mqttc, fake_broker, "auth-stray-success")

            # AUTH (Success) is only legal to complete a re-authentication.
            fake_broker.send_packet(paho_test.gen_auth(REASON_SUCCESS))

            self._assert_auth_failure_terminal_state(mqttc, cb, fake_broker)
            assert cb.seen == []
        finally:
            mqttc.loop_stop()
