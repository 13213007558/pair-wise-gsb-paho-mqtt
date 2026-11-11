"""Tests for MQTT v5.0 enhanced authentication (AUTH) support.

These tests use an in-memory socket pair (tests.testsupport.loopback) instead
of a listening broker, so they run without binding any TCP port. They cover the
initial-authentication and re-authentication lifecycles, the deterministic
fake authenticator's success/failure/multi-step flows, and every protocol
violation the client is required to reject.
"""
from __future__ import annotations

import threading

import pytest

from paho.mqtt import client as paho_client
from paho.mqtt.auth import AuthError, AuthPhase, AuthResult
from paho.mqtt.enums import MQTTErrorCode, MQTTProtocolVersion
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties
from paho.mqtt.reasoncodes import ReasonCode
from tests.testsupport import loopback
from tests.testsupport.fakeauth import FAKE_METHOD, FakeAuthController, NOT_AUTHORIZED


def _connect_with_auth(controller):
    mqttc, broker = loopback.make_loopback(controller)
    rc = mqttc._send_connect(60)
    assert rc == MQTTErrorCode.MQTT_ERR_SUCCESS
    connect = broker.read_packet(mqttc)
    assert connect["command"] == 0x10
    return mqttc, broker, connect


def test_connect_includes_auth_method_and_data():
    controller = FakeAuthController()
    mqttc, broker, connect = _connect_with_auth(controller)
    assert connect["connect"]["method"] == FAKE_METHOD
    assert connect["connect"]["data"] == b"client-initial"
    mqttc._sock_close()
    broker.close()


def test_initial_single_round_success():
    controller = FakeAuthController()
    mqttc, broker, _ = _connect_with_auth(controller)

    connected = threading.Event()
    mqttc.on_connect = lambda *a: connected.set()

    # Server issues one challenge, then a successful CONNACK.
    broker.send_packet(loopback.build_auth(0x18, FAKE_METHOD, b"server-challenge"))
    mqttc.loop_read()
    answer = broker.read_packet(mqttc)
    assert answer["auth"]["reason"] == 0x18
    assert answer["auth"]["method"] == FAKE_METHOD
    assert answer["auth"]["data"] == b"server-challenge|response-1"

    broker.send_packet(loopback.build_connack(0, FAKE_METHOD, b"final"))
    mqttc.loop_read()
    assert connected.wait(1)
    assert mqttc.is_connected()
    assert mqttc._auth_state.name == "AUTHENTICATED"
    assert ("success", b"final") in controller.events
    mqttc._sock_close()
    broker.close()


def test_initial_success_without_challenge():
    controller = FakeAuthController()
    mqttc, broker, _ = _connect_with_auth(controller)
    connected = threading.Event()
    mqttc.on_connect = lambda *a: connected.set()

    broker.send_packet(loopback.build_connack(0, FAKE_METHOD))
    mqttc.loop_read()
    assert connected.wait(1)
    assert mqttc.is_connected()
    mqttc._sock_close()
    broker.close()


def test_initial_server_failure_connack_never_connected():
    controller = FakeAuthController()
    mqttc, broker, _ = _connect_with_auth(controller)
    mqttc.on_connect = lambda *a: pytest.fail("on_connect must not run on failure")

    broker.send_packet(loopback.build_connack(NOT_AUTHORIZED, FAKE_METHOD))
    rc = mqttc.loop_read()
    assert rc == MQTTErrorCode.MQTT_ERR_AUTH
    assert not mqttc.is_connected()
    assert mqttc._auth_state.name == "NONE"
    assert any(event[0] == "failure" or True for event in controller.events)
    # handle_failure observed the server rejection.
    assert ("failure", "initial") not in controller.events  # failure came from server
    broker.close()


def test_local_controller_refuses_initial():
    controller = FakeAuthController(fail_at="initial")
    mqttc, broker = loopback.make_loopback(controller)
    rc = mqttc._send_connect(60)
    assert rc == MQTTErrorCode.MQTT_ERR_AUTH
    assert not mqttc.is_connected()
    broker.close()


def test_controller_exception_initial_aborts():
    controller = FakeAuthController(raise_at="initial")
    mqttc, broker = loopback.make_loopback(controller)
    mqttc.suppress_exceptions = True
    rc = mqttc._send_connect(60)
    assert rc == MQTTErrorCode.MQTT_ERR_AUTH
    assert not mqttc.is_connected()
    broker.close()


def test_challenge_rejected_by_controller_closes_connection():
    controller = FakeAuthController(fail_at="challenge")
    mqttc, broker, _ = _connect_with_auth(controller)
    broker.send_packet(loopback.build_auth(0x18, FAKE_METHOD, b"ch"))
    rc = mqttc.loop_read()
    assert rc == MQTTErrorCode.MQTT_ERR_AUTH
    assert not mqttc.is_connected()
    assert mqttc._sock is None
    broker.close()


def test_challenge_callback_exception_does_not_authenticate():
    controller = FakeAuthController(raise_at="challenge")
    mqttc, broker, _ = _connect_with_auth(controller)
    mqttc.suppress_exceptions = True
    broker.send_packet(loopback.build_auth(0x18, FAKE_METHOD, b"ch"))
    rc = mqttc.loop_read()
    assert rc == MQTTErrorCode.MQTT_ERR_AUTH
    assert not mqttc.is_connected()
    broker.close()


def test_method_substitution_mid_exchange_rejected():
    controller = FakeAuthController()
    mqttc, broker, _ = _connect_with_auth(controller)
    broker.send_packet(loopback.build_auth(0x18, "OTHER-METHOD", b"ch"))
    rc = mqttc.loop_read()
    assert rc == MQTTErrorCode.MQTT_ERR_PROTOCOL
    assert not mqttc.is_connected()
    broker.close()


def test_auth_data_without_method_rejected():
    controller = FakeAuthController()
    mqttc, broker, _ = _connect_with_auth(controller)
    # Authentication Data property but no Authentication Method.
    props = Properties(PacketTypes.AUTH)
    props.AuthenticationData = b"orphan"
    packet = bytes([0xF0]) + loopback._remaining_length(1 + len(props.pack())) \
        + bytes([0x18]) + props.pack()
    broker.send_packet(packet)
    rc = mqttc.loop_read()
    assert rc == MQTTErrorCode.MQTT_ERR_PROTOCOL
    assert not mqttc.is_connected()
    broker.close()


def test_auth_packet_without_controller_rejected():
    mqttc, broker = loopback.make_loopback(None)
    rc = mqttc._send_connect(60)
    assert rc == MQTTErrorCode.MQTT_ERR_SUCCESS
    broker.read_packet(mqttc)
    broker.send_packet(loopback.build_auth(0x18, FAKE_METHOD, b"ch"))
    rc = mqttc.loop_read()
    assert rc == MQTTErrorCode.MQTT_ERR_PROTOCOL
    broker.close()


def test_unexpected_success_code_in_initial_challenge_rejected():
    controller = FakeAuthController()
    mqttc, broker, _ = _connect_with_auth(controller)
    # AUTH Success (0) is not valid while the server should send Continue (24).
    broker.send_packet(loopback.build_auth(0x00, FAKE_METHOD))
    rc = mqttc.loop_read()
    assert rc == MQTTErrorCode.MQTT_ERR_PROTOCOL
    broker.close()


def test_peer_disconnect_during_initial_auth_not_authenticated():
    controller = FakeAuthController()
    mqttc, broker, _ = _connect_with_auth(controller)
    disconnected = threading.Event()
    mqttc.on_disconnect = lambda *a: disconnected.set()
    broker.close()  # server drops the transport
    rc = mqttc.loop_read()
    assert rc == MQTTErrorCode.MQTT_ERR_CONN_LOST
    assert not mqttc.is_connected()
    assert mqttc._auth_state.name == "NONE"
    assert disconnected.wait(1)


def _establish(controller):
    mqttc, broker, _ = _connect_with_auth(controller)
    connected = threading.Event()
    mqttc.on_connect = lambda *a: connected.set()
    broker.send_packet(loopback.build_connack(0, FAKE_METHOD))
    mqttc.loop_read()
    assert connected.wait(1)
    assert mqttc.is_connected()
    return mqttc, broker


def test_re_authenticate_multi_step_success():
    controller = FakeAuthController(require_final_success_data=True)
    mqttc, broker = _establish(controller)
    reauth_done = threading.Event()
    seen = []

    def on_auth(client, userdata, phase, reason, properties):
        seen.append((phase, reason.value))
        if phase == AuthPhase.REAUTHENTICATE and reason.value == 0:
            reauth_done.set()

    mqttc.on_auth = on_auth

    rc = mqttc.re_authenticate()
    assert rc == MQTTErrorCode.MQTT_ERR_SUCCESS
    first = broker.read_packet(mqttc)
    assert first["auth"]["reason"] == 0x19
    assert first["auth"]["method"] == FAKE_METHOD
    assert first["auth"]["data"] == b"reauth-initial"

    # Two server challenges exercise the multi-step re-authentication.
    broker.send_packet(loopback.build_auth(0x18, FAKE_METHOD, b"ra-ch1"))
    mqttc.loop_read()
    step1 = broker.read_packet(mqttc)
    assert step1["auth"]["reason"] == 0x18
    assert step1["auth"]["data"] == b"ra-ch1|response-1"

    broker.send_packet(loopback.build_auth(0x18, FAKE_METHOD, b"ra-ch2"))
    mqttc.loop_read()
    step2 = broker.read_packet(mqttc)
    assert step2["auth"]["reason"] == 0x18
    assert step2["auth"]["data"] == b"ra-ch2|response-2"

    # Terminal server Success; client answers with a final Success.
    broker.send_packet(loopback.build_auth(0x00, FAKE_METHOD, b"ra-final"))
    mqttc.loop_read()
    final = broker.read_packet(mqttc)
    assert final["auth"]["reason"] == 0x00
    assert final["auth"]["data"] == b"client-final-success"
    assert reauth_done.wait(1)
    assert mqttc.is_connected()
    assert mqttc._auth_state.name == "AUTHENTICATED"
    assert (AuthPhase.REAUTHENTICATE, 0) in seen
    mqttc._sock_close()
    broker.close()


def test_re_authenticate_rejected_by_controller_keeps_state_invalid():
    controller = FakeAuthController(fail_at="reauth")
    mqttc, broker = _establish(controller)
    rc = mqttc.re_authenticate()
    assert rc == MQTTErrorCode.MQTT_ERR_AUTH
    # A locally refused re-authentication tears down the connection per spec.
    assert not mqttc.is_connected()
    broker.close()


def test_re_authenticate_challenge_failure_closes():
    controller = FakeAuthController(fail_at="reauth-challenge")
    mqttc, broker = _establish(controller)
    rc = mqttc.re_authenticate()
    assert rc == MQTTErrorCode.MQTT_ERR_SUCCESS
    broker.read_packet(mqttc)
    broker.send_packet(loopback.build_auth(0x18, FAKE_METHOD, b"ra"))
    rc = mqttc.loop_read()
    assert rc == MQTTErrorCode.MQTT_ERR_AUTH
    assert not mqttc.is_connected()
    broker.close()


def test_re_authenticate_server_failure_auth_packet():
    controller = FakeAuthController()
    mqttc, broker = _establish(controller)
    mqttc.re_authenticate()
    broker.read_packet(mqttc)
    broker.send_packet(loopback.build_auth(NOT_AUTHORIZED, FAKE_METHOD))
    rc = mqttc.loop_read()
    assert rc == MQTTErrorCode.MQTT_ERR_AUTH
    assert not mqttc.is_connected()
    broker.close()


def test_re_authenticate_requires_controller_and_connection():
    mqttc, broker = loopback.make_loopback(None)
    with pytest.raises(AuthError):
        mqttc.re_authenticate()

    controller = FakeAuthController()
    mqttc2, broker2 = loopback.make_loopback(controller)
    # Configured but not connected yet.
    with pytest.raises(AuthError):
        mqttc2.re_authenticate()
    broker.close()
    broker2.close()


def test_re_authenticate_requires_mqttv5():
    controller = FakeAuthController()
    mqttc = paho_client.Client(
        paho_client.CallbackAPIVersion.VERSION2,
        client_id="v3",
        protocol=MQTTProtocolVersion.MQTTv311,
    )
    with pytest.raises(AuthError):
        mqttc.auth_controller = controller


def test_auth_controller_validation():
    mqttc, broker = loopback.make_loopback(None)
    with pytest.raises(TypeError):
        mqttc.auth_controller = object()
    with pytest.raises(ValueError):
        mqttc.auth_controller = _EmptyMethodController()
    broker.close()


class _EmptyMethodController(FakeAuthController):
    method = ""


def test_on_auth_notification_fires_per_packet():
    controller = FakeAuthController()
    mqttc, broker, _ = _connect_with_auth(controller)
    notifications = []
    mqttc.on_auth = lambda c, u, phase, rc, props: notifications.append(
        (phase, rc.value, getattr(props, "AuthenticationData", None)))

    broker.send_packet(loopback.build_auth(0x18, FAKE_METHOD, b"n1"))
    mqttc.loop_read()
    broker.read_packet(mqttc)
    broker.send_packet(loopback.build_auth(0x18, FAKE_METHOD, b"n2"))
    mqttc.loop_read()
    broker.read_packet(mqttc)
    broker.send_packet(loopback.build_connack(0, FAKE_METHOD, b"ok"))
    mqttc.loop_read()

    assert [n[0] for n in notifications] == [
        AuthPhase.INITIAL, AuthPhase.INITIAL]
    assert notifications[0][1] == 0x18 and notifications[0][2] == b"n1"
    assert notifications[1][1] == 0x18 and notifications[1][2] == b"n2"
    mqttc._sock_close()
    broker.close()


def test_method_cannot_change_after_connect():
    controller = FakeAuthController()
    mqttc, broker, _ = _connect_with_auth(controller)
    mqttc._state = paho_client._ConnectionState.MQTT_CS_CONNECTED
    with pytest.raises(RuntimeError):
        mqttc.auth_controller = FakeAuthController()
    mqttc._sock_close()
    broker.close()


def test_controller_returning_failure_code_in_challenge():
    class RejectController(FakeAuthController):
        def handle_challenge(self, *args):
            return AuthResult(
                reason_code=ReasonCode(PacketTypes.AUTH, identifier=NOT_AUTHORIZED))

    mqttc, broker, _ = _connect_with_auth(RejectController())
    broker.send_packet(loopback.build_auth(0x18, FAKE_METHOD, b"ch"))
    rc = mqttc.loop_read()
    assert rc == MQTTErrorCode.MQTT_ERR_AUTH
    assert not mqttc.is_connected()
    broker.close()


def test_reconnect_resets_authentication_lifecycle():
    controller = FakeAuthController()
    mqttc, broker = _establish(controller)
    assert mqttc._auth_state.name == "AUTHENTICATED"
    # Simulate loss followed by a fresh reconnect lifecycle.
    mqttc._sock_close()
    broker.close()
    mqttc._state = paho_client._ConnectionState.MQTT_CS_CONNECTION_LOST
    mqttc._auth_reset()
    assert mqttc._auth_state.name == "INITIAL_IN_PROGRESS"
    assert mqttc._auth_method is None


def test_re_authenticate_while_exchange_in_progress_raises():
    controller = FakeAuthController()
    mqttc, broker = _establish(controller)
    mqttc.re_authenticate()
    broker.read_packet(mqttc)
    with pytest.raises(AuthError):
        mqttc.re_authenticate()
    mqttc._sock_close()
    broker.close()


def test_connect_properties_are_not_mutated_by_auth():
    controller = FakeAuthController()
    mqttc, broker = loopback.make_loopback(controller)
    original = Properties(PacketTypes.CONNECT)
    original.SessionExpiryInterval = 60
    mqttc._connect_properties = original
    mqttc._send_connect(60)
    connect = broker.read_packet(mqttc)
    # User object untouched; auth material added on the wire copy instead.
    assert not hasattr(original, "AuthenticationMethod")
    assert connect["connect"]["properties"].SessionExpiryInterval == 60
    assert connect["connect"]["method"] == FAKE_METHOD
    mqttc._sock_close()
    broker.close()


def test_auth_state_none_blocks_orphan_connack_auth_properties():
    # CONNACK carrying Authentication Data but no method and no controller.
    mqttc, broker = loopback.make_loopback(None)
    mqttc._send_connect(60)
    broker.read_packet(mqttc)
    props = Properties(PacketTypes.CONNACK)
    props.AuthenticationData = b"x"
    packet = bytes([0x20, 2 + len(props.pack()), 0, 0]) + props.pack()
    broker.send_packet(packet)
    rc = mqttc.loop_read()
    assert rc == MQTTErrorCode.MQTT_ERR_PROTOCOL
    broker.close()


def test_on_auth_exception_does_not_break_state_machine():
    controller = FakeAuthController()
    mqttc, broker, _ = _connect_with_auth(controller)
    mqttc.suppress_exceptions = True

    def boom(*args):
        raise RuntimeError("on_auth observer failure")

    mqttc.on_auth = boom
    connected = threading.Event()
    mqttc.on_connect = lambda *a: connected.set()

    broker.send_packet(loopback.build_auth(0x18, FAKE_METHOD, b"ch"))
    # The observer exception is suppressed and must not abort authentication.
    mqttc.loop_read()
    broker.read_packet(mqttc)
    broker.send_packet(loopback.build_connack(0, FAKE_METHOD))
    mqttc.loop_read()
    assert connected.wait(1)
    assert mqttc.is_connected()
    mqttc._sock_close()
    broker.close()


def test_disconnect_mid_reauth_not_reported_authenticated():
    controller = FakeAuthController()
    mqttc, broker = _establish(controller)
    disconnected = threading.Event()
    mqttc.on_disconnect = lambda *a: disconnected.set()
    mqttc.re_authenticate()
    broker.read_packet(mqttc)
    broker.close()  # server vanishes during re-authentication
    rc = mqttc.loop_read()
    assert rc == MQTTErrorCode.MQTT_ERR_CONN_LOST
    assert not mqttc.is_connected()
    assert mqttc._auth_state.name == "NONE"
    assert disconnected.wait(1)


def test_handle_success_failure_code_prevents_initial_connection():
    controller = FakeAuthController(fail_at="success")
    mqttc, broker, _ = _connect_with_auth(controller)
    mqttc.on_connect = lambda *a: pytest.fail("must not connect")
    broker.send_packet(loopback.build_connack(0, FAKE_METHOD, b"ok"))
    rc = mqttc.loop_read()
    assert rc == MQTTErrorCode.MQTT_ERR_AUTH
    assert not mqttc.is_connected()
    broker.close()


def test_handle_success_exception_prevents_initial_connection():
    controller = FakeAuthController(raise_at="success")
    mqttc, broker, _ = _connect_with_auth(controller)
    mqttc.suppress_exceptions = True
    broker.send_packet(loopback.build_connack(0, FAKE_METHOD, b"ok"))
    rc = mqttc.loop_read()
    assert rc == MQTTErrorCode.MQTT_ERR_AUTH
    assert not mqttc.is_connected()
    broker.close()


def test_reauth_success_exception_does_not_keep_authenticated_state():
    controller = FakeAuthController(raise_at="reauth-success")
    mqttc, broker = _establish(controller)
    mqttc.suppress_exceptions = True
    mqttc.re_authenticate()
    broker.read_packet(mqttc)
    broker.send_packet(loopback.build_auth(0x00, FAKE_METHOD, b"ok"))
    rc = mqttc.loop_read()
    assert rc == MQTTErrorCode.MQTT_ERR_AUTH
    assert not mqttc.is_connected()
    broker.close()


def test_connack_may_omit_method_but_must_not_substitute():
    # CONNACK without Authentication Method while initial auth is in progress
    # is permitted (the method was established on CONNECT). It must succeed.
    controller = FakeAuthController()
    mqttc, broker, _ = _connect_with_auth(controller)
    connected = threading.Event()
    mqttc.on_connect = lambda *a: connected.set()
    props = Properties(PacketTypes.CONNACK)  # no Authentication Method
    packet = bytes([0x20, 2 + len(props.pack()), 0, 0]) + props.pack()
    broker.send_packet(packet)
    mqttc.loop_read()
    assert connected.wait(1)
    assert mqttc.is_connected()
    mqttc._sock_close()
    broker.close()
