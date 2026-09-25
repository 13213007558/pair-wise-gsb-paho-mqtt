import socket
import threading
import time

import paho.mqtt.client as client
import pytest
from paho.mqtt.enums import CallbackAPIVersion, MQTTProtocolVersion
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.reasoncodes import ReasonCode

import tests.mqtt5_props as mqtt5_props
import tests.paho_test as paho_test

AUTH_METHOD = "test-method"


def auth_props(method, data=None):
    """Build the Authentication Method (+ optional Authentication Data)
    properties. data=None means the property is absent; data=b'' means an
    explicitly empty property."""
    props = mqtt5_props.gen_string_prop(
        mqtt5_props.PROP_AUTHENTICATION_METHOD, method)
    if data is not None:
        props += mqtt5_props.gen_binary_prop(
            mqtt5_props.PROP_AUTHENTICATION_DATA, data)
    return props


class FakeAuthBroker:
    """A local fake broker connected to the client through a socketpair, so
    the tests do not need any network access. It can send packets (including
    segmented ones) and check the packets sent by the client."""

    def __init__(self):
        self.client_sock, self.sock = socket.socketpair()
        self.sock.settimeout(10)

    def attach(self, mqttc):
        # Make the client use its end of the socketpair instead of opening a
        # real network connection.
        mqttc._create_socket = lambda: self.client_sock

    def expect_packet(self, name, packet):
        paho_test.expect_packet(self.sock, name, packet)

    def send_packet(self, packet):
        return self.sock.send(packet)

    def send_segmented(self, packet, cuts, delay=0.1):
        """Send a packet in several segments to exercise the client packet
        reassembly logic."""
        prev = 0
        for cut in cuts:
            self.send_packet(packet[prev:cut])
            time.sleep(delay)
            prev = cut
        self.send_packet(packet[prev:])

    def expect_closed(self):
        assert self.sock.recv(1) == b""

    def finish(self):
        for sock in (self.client_sock, self.sock):
            try:
                sock.close()
            except OSError:
                pass


@pytest.fixture
def auth_broker():
    broker = FakeAuthBroker()
    yield broker
    broker.finish()


def wait_for(event, timeout=5):
    assert event.wait(timeout), "timed out waiting for client callback"


class TestEnhancedAuth:
    """Tests for MQTT v5.0 enhanced authentication (AUTH packet exchanges)."""

    def make_client(self, auth_broker, client_id, data=None,
                    reconnect_on_failure=True, keepalive=60):
        mqttc = client.Client(
            CallbackAPIVersion.VERSION2,
            client_id,
            protocol=MQTTProtocolVersion.MQTTv5,
            reconnect_on_failure=reconnect_on_failure,
        )
        mqttc.auth_method_set(AUTH_METHOD, data)
        auth_broker.attach(mqttc)
        mqttc.connect_async("localhost", 1883, keepalive=keepalive)
        return mqttc

    def expect_connect(self, auth_broker, client_id, keepalive=60, data=None):
        connect_packet = paho_test.gen_connect(
            client_id, keepalive=keepalive, proto_ver=5,
            properties=auth_props(AUTH_METHOD, data))
        auth_broker.expect_packet("connect", connect_packet)

    def test_two_round_auth_success(self, auth_broker):
        """Two rounds of AUTH Continue (sent segmented by the broker) complete
        with CONNACK; on_connect must not fire before authentication passed."""
        client_id = "auth-two-round-success"
        connected = threading.Event()
        auth_rounds = []

        mqttc = self.make_client(auth_broker, client_id, data=b"initial-data")

        def on_connect(mqttc, userdata, flags, reason_code, properties):
            assert reason_code == ReasonCode(PacketTypes.CONNACK, "Success")
            connected.set()

        def on_auth(mqttc, userdata, reason_code, properties):
            assert reason_code == ReasonCode(PacketTypes.AUTH, "Continue authentication")
            assert properties.AuthenticationMethod == AUTH_METHOD
            auth_rounds.append(properties.AuthenticationData)
            if properties.AuthenticationData == b"challenge-1":
                mqttc.authenticate(b"response-1")
            elif properties.AuthenticationData == b"challenge-2":
                # Empty data is valid and must be sent as an empty property
                mqttc.authenticate(b"")

        mqttc.on_connect = on_connect
        mqttc.on_auth = on_auth
        mqttc.loop_start()

        try:
            self.expect_connect(auth_broker, client_id, data=b"initial-data")

            # Round 1: split the AUTH packet into three segments
            auth1 = paho_test.gen_auth(
                reason_code=0x18,
                properties=auth_props(AUTH_METHOD, b"challenge-1"))
            auth_broker.send_segmented(auth1, cuts=(1, 2))

            auth_broker.expect_packet(
                "auth response 1",
                paho_test.gen_auth(
                    reason_code=0x18,
                    properties=auth_props(AUTH_METHOD, b"response-1")))

            # The connection is not established yet: no on_connect callback
            assert not connected.wait(0.2)
            assert not mqttc.is_connected()

            # Round 2: split inside the properties
            auth2 = paho_test.gen_auth(
                reason_code=0x18,
                properties=auth_props(AUTH_METHOD, b"challenge-2"))
            auth_broker.send_segmented(auth2, cuts=(3, 7))

            # The client answers with an explicit empty (b"") authentication
            # data property, which must not be confused with an absent one.
            auth_broker.expect_packet(
                "auth response 2",
                paho_test.gen_auth(
                    reason_code=0x18,
                    properties=auth_props(AUTH_METHOD, b"")))

            assert not connected.wait(0.2)
            assert not mqttc.is_connected()

            # The broker completes the exchange with CONNACK
            auth_broker.send_packet(paho_test.gen_connack(rc=0, proto_ver=5))

            wait_for(connected)
            assert mqttc.is_connected()
            assert auth_rounds == [b"challenge-1", b"challenge-2"]

            mqttc.disconnect()
            auth_broker.expect_packet("disconnect", paho_test.gen_disconnect())
        finally:
            mqttc.loop_stop()

    def test_auth_method_mismatch(self, auth_broker):
        """The broker must not change the authentication method: the client
        aborts with DISCONNECT 'Bad authentication method' and never calls
        on_connect."""
        client_id = "auth-method-mismatch"
        connected = threading.Event()
        disconnected = threading.Event()
        disconnect_reasons = []

        mqttc = self.make_client(
            auth_broker, client_id, reconnect_on_failure=False)
        mqttc.on_connect = lambda *args: connected.set()

        def on_disconnect(mqttc, userdata, flags, reason_code, properties):
            disconnect_reasons.append(reason_code)
            disconnected.set()

        mqttc.on_disconnect = on_disconnect
        mqttc.loop_start()

        try:
            # CONNECT carries the method but no Authentication Data property
            self.expect_connect(auth_broker, client_id)

            # The broker tries to switch to another authentication method
            auth_broker.send_packet(paho_test.gen_auth(
                reason_code=0x18,
                properties=auth_props("another-method", b"challenge")))

            auth_broker.expect_packet(
                "disconnect",
                paho_test.gen_disconnect(reason_code=0x8C, proto_ver=5))

            wait_for(disconnected)
            assert disconnect_reasons == [
                ReasonCode(PacketTypes.DISCONNECT, "Bad authentication method")]
            assert not connected.is_set()
            assert not mqttc.is_connected()
        finally:
            mqttc.loop_stop()

    def test_auth_callback_exception(self, auth_broker):
        """An exception in the on_auth callback aborts the exchange with
        DISCONNECT 'Implementation specific error' instead of crashing the
        network loop."""
        client_id = "auth-callback-error"
        connected = threading.Event()
        disconnected = threading.Event()
        disconnect_reasons = []

        mqttc = self.make_client(
            auth_broker, client_id, reconnect_on_failure=False)
        mqttc.on_connect = lambda *args: connected.set()

        def on_auth(mqttc, userdata, reason_code, properties):
            raise RuntimeError("boom")

        def on_disconnect(mqttc, userdata, flags, reason_code, properties):
            disconnect_reasons.append(reason_code)
            disconnected.set()

        mqttc.on_auth = on_auth
        mqttc.on_disconnect = on_disconnect
        mqttc.loop_start()

        try:
            self.expect_connect(auth_broker, client_id)

            auth_broker.send_packet(paho_test.gen_auth(
                reason_code=0x18,
                properties=auth_props(AUTH_METHOD, b"challenge")))

            auth_broker.expect_packet(
                "disconnect",
                paho_test.gen_disconnect(reason_code=0x83, proto_ver=5))

            wait_for(disconnected)
            assert disconnect_reasons == [
                ReasonCode(PacketTypes.DISCONNECT, "Implementation specific error")]
            assert not connected.is_set()
        finally:
            mqttc.loop_stop()

    def test_auth_success_before_connack_rejected(self, auth_broker):
        """An AUTH packet with reason Success is only legal to complete a
        re-authentication; before CONNACK it is a protocol error."""
        client_id = "auth-early-success"
        disconnected = threading.Event()
        disconnect_reasons = []

        mqttc = self.make_client(
            auth_broker, client_id, reconnect_on_failure=False)

        def on_disconnect(mqttc, userdata, flags, reason_code, properties):
            disconnect_reasons.append(reason_code)
            disconnected.set()

        mqttc.on_disconnect = on_disconnect
        mqttc.loop_start()

        try:
            self.expect_connect(auth_broker, client_id)

            # Success is not a valid way to complete the initial exchange
            auth_broker.send_packet(paho_test.gen_auth(
                reason_code=0x00, properties=auth_props(AUTH_METHOD)))

            auth_broker.expect_packet(
                "disconnect",
                paho_test.gen_disconnect(reason_code=0x82, proto_ver=5))

            wait_for(disconnected)
            assert disconnect_reasons == [
                ReasonCode(PacketTypes.DISCONNECT, "Protocol error")]
        finally:
            mqttc.loop_stop()

    def test_re_authenticate(self, auth_broker):
        """Re-authentication on an established connection: client-initiated and
        server-initiated, with PUBLISH/ACK and keepalive traffic continuing
        during the exchange."""
        client_id = "auth-re-authenticate"
        connected = threading.Event()
        reauth_done = threading.Event()
        server_reauth_done = threading.Event()
        messages = []
        disconnects = []

        mqttc = self.make_client(auth_broker, client_id, keepalive=2)

        def on_connect(mqttc, userdata, flags, reason_code, properties):
            assert reason_code == ReasonCode(PacketTypes.CONNACK, "Success")
            connected.set()
            # Start a client-initiated re-authentication
            assert mqttc.re_authenticate(b"reauth-1") == 0

        def on_auth(mqttc, userdata, reason_code, properties):
            if reason_code == ReasonCode(PacketTypes.AUTH, "Continue authentication"):
                assert properties.AuthenticationMethod == AUTH_METHOD
                mqttc.authenticate(b"reauth-2")
            elif reason_code == ReasonCode(PacketTypes.AUTH, "Re-authenticate"):
                assert properties.AuthenticationMethod == AUTH_METHOD
                # Server-initiated re-authentication
                mqttc.authenticate(b"server-reauth")
            elif reason_code == ReasonCode(PacketTypes.AUTH, "Success"):
                if reauth_done.is_set():
                    server_reauth_done.set()
                else:
                    reauth_done.set()

        def on_message(mqttc, userdata, message):
            messages.append(message)

        def on_disconnect(mqttc, userdata, flags, reason_code, properties):
            disconnects.append(reason_code)

        mqttc.on_connect = on_connect
        mqttc.on_auth = on_auth
        mqttc.on_message = on_message
        mqttc.on_disconnect = on_disconnect
        mqttc.loop_start()

        try:
            self.expect_connect(auth_broker, client_id, keepalive=2)
            auth_broker.send_packet(paho_test.gen_connack(rc=0, proto_ver=5))

            wait_for(connected)

            # Client-initiated re-authentication starts with Re-authenticate
            auth_broker.expect_packet(
                "re-authenticate",
                paho_test.gen_auth(
                    reason_code=0x19,
                    properties=auth_props(AUTH_METHOD, b"reauth-1")))

            # Normal traffic keeps flowing during the exchange: the broker
            # publishes a QoS 1 message and expects a PUBACK.
            auth_broker.send_packet(paho_test.gen_publish(
                "test/reauth", 1, payload="hello", mid=7, proto_ver=5))
            auth_broker.expect_packet(
                "puback", paho_test.gen_puback(7, proto_ver=5))

            # The broker continues the exchange
            auth_broker.send_packet(paho_test.gen_auth(
                reason_code=0x18,
                properties=auth_props(AUTH_METHOD, b"server-challenge")))
            auth_broker.expect_packet(
                "auth continue",
                paho_test.gen_auth(
                    reason_code=0x18,
                    properties=auth_props(AUTH_METHOD, b"reauth-2")))

            # Keepalive is still honoured while re-authenticating
            auth_broker.expect_packet("pingreq", paho_test.gen_pingreq())
            auth_broker.send_packet(paho_test.gen_pingresp())

            # The broker completes the re-authentication
            auth_broker.send_packet(paho_test.gen_auth(reason_code=0x00))
            wait_for(reauth_done)
            assert mqttc.is_connected()

            # Server-initiated re-authentication
            auth_broker.send_packet(paho_test.gen_auth(
                reason_code=0x19, properties=auth_props(AUTH_METHOD)))
            auth_broker.expect_packet(
                "auth continue (server)",
                paho_test.gen_auth(
                    reason_code=0x18,
                    properties=auth_props(AUTH_METHOD, b"server-reauth")))
            auth_broker.send_packet(paho_test.gen_auth(reason_code=0x00))
            wait_for(server_reauth_done)

            assert mqttc.is_connected()
            assert len(messages) == 1
            assert messages[0].payload == b"hello"
            # No unexpected disconnection happened during the exchanges
            assert disconnects == []

            mqttc.disconnect()
            auth_broker.expect_packet("disconnect", paho_test.gen_disconnect())
        finally:
            mqttc.loop_stop()

    def test_auth_failure_disconnect_reason(self, auth_broker):
        """If the broker rejects the authentication with a DISCONNECT, the
        reason code is delivered to the application through on_disconnect."""
        client_id = "auth-failure-reason"
        connected = threading.Event()
        disconnected = threading.Event()
        disconnect_reasons = []

        mqttc = self.make_client(
            auth_broker, client_id, reconnect_on_failure=False)
        mqttc.on_connect = lambda *args: connected.set()

        def on_disconnect(mqttc, userdata, flags, reason_code, properties):
            disconnect_reasons.append(reason_code)
            disconnected.set()

        mqttc.on_disconnect = on_disconnect
        mqttc.loop_start()

        try:
            self.expect_connect(auth_broker, client_id)

            # The broker refuses the authentication attempt
            auth_broker.send_packet(
                paho_test.gen_disconnect(reason_code=0x87, proto_ver=5))

            wait_for(disconnected)
            assert disconnect_reasons == [
                ReasonCode(PacketTypes.DISCONNECT, "Not authorized")]
            assert not connected.is_set()
        finally:
            mqttc.loop_stop()

    def test_re_authenticate_requires_connection(self, auth_broker):
        """re_authenticate() is only valid on a connected client that used
        enhanced authentication, and enhanced authentication needs MQTT v5."""
        mqttc = client.Client(
            CallbackAPIVersion.VERSION2,
            "auth-reauth-no-conn",
            protocol=MQTTProtocolVersion.MQTTv5,
        )
        mqttc.auth_method_set(AUTH_METHOD)

        # Not connected yet
        assert mqttc.re_authenticate() != 0

        # authenticate() without an ongoing exchange is an error
        assert mqttc.authenticate() != 0

        # Enhanced authentication requires MQTT v5
        mqttc3 = client.Client(
            CallbackAPIVersion.VERSION2,
            "auth-v3",
            protocol=MQTTProtocolVersion.MQTTv311,
        )
        with pytest.raises(ValueError):
            mqttc3.auth_method_set(AUTH_METHOD)
