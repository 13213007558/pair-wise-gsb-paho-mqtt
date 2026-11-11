#!/usr/bin/python3
# -*- coding: utf-8 -*-

# Copyright (c) 2024 Roger Light <roger@atchoo.org>
#
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Eclipse Distribution License v1.0
# which accompanies this distribution.
#
# The Eclipse Distribution License is available at
#   http://www.eclipse.org/org/documents/edl-v10.php.

# MQTT v5.0 enhanced authentication example.
#
# The paho-mqtt library never implements an authentication algorithm or an
# identity service: it only carries the Authentication Method / Authentication
# Data properties and maintains the AUTH state machine. The application supplies
# an AuthController with the actual (here: deterministic fake) algorithm.
#
# This example is fully self contained and does not open a network socket. A
# fake peer connected through a socket pair plays the broker side and drives the
# same packet parsing code path as a real connection. It demonstrates:
#   1. a successful initial authentication,
#   2. an authentication rejected by the local controller,
#   3. a multi-step re-authentication on an established connection.

import socket

import context  # Ensures paho is in PYTHONPATH

from paho.mqtt.auth import AuthController, AuthPhase, AuthResult
from paho.mqtt.client import Client, CallbackAPIVersion, MQTTv5, MQTTErrorCode
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties
from paho.mqtt.reasoncodes import ReasonCode

METHOD = "EXAMPLE-FAKE-AUTH"


class FakeAuthenticator(AuthController):
    """Deterministic challenge/response authenticator (not a real identity
    service). Each challenge is answered by echoing the received data with a
    step counter, so the exchange is easy to follow on the wire."""

    method = METHOD

    def __init__(self):
        self.step = 0

    def initial_authentication(self, client, userdata):
        print("  controller: initial_authentication -> send initial data")
        return AuthResult(data=b"client-hello")

    def handle_challenge(self, client, userdata, phase, data, properties):
        self.step += 1
        print(f"  controller: handle_challenge({phase.value}, {data!r})")
        answer = b"answer-%d" % self.step
        if data is not None:
            answer = data + b"|" + answer
        return AuthResult(data=answer)

    def handle_success(self, client, userdata, phase, data, properties):
        print(f"  controller: handle_success({phase.value}, {data!r})")
        if phase == AuthPhase.REAUTHENTICATE:
            # Optionally attach data to the final client Success AUTH packet.
            return AuthResult(data=b"client-final-success")
        return None


class RejectingAuthenticator(FakeAuthenticator):
    """Refuses the very first challenge, demonstrating failure handling."""

    def handle_challenge(self, client, userdata, phase, data, properties):
        print("  controller: rejecting the challenge")
        return AuthResult(
            reason_code=ReasonCode(PacketTypes.AUTH, identifier=135))


def frame(command, body):
    def remaining(length):
        out = b""
        while True:
            byte = length % 128
            length //= 128
            if length:
                byte |= 128
            out += bytes([byte])
            if not length:
                return out
    return bytes([command]) + remaining(len(body)) + body


def auth_packet(reason, data=None):
    props = Properties(PacketTypes.AUTH)
    props.AuthenticationMethod = METHOD
    if data is not None:
        props.AuthenticationData = data
    return frame(0xF0, bytes([reason]) + props.pack())


def connack_packet(reason=0):
    props = Properties(PacketTypes.CONNACK)
    props.AuthenticationMethod = METHOD
    return frame(0x20, bytes([0, reason]) + props.pack())


def read_exact(sock, n, buffer):
    while len(buffer[0]) < n:
        buffer[0] += sock.recv(4096)
    out = buffer[0][:n]
    buffer[0] = buffer[0][n:]
    return out


def read_packet(sock, buffer):
    read_exact(sock, 1, buffer)
    remaining = 0
    multiplier = 1
    for _ in range(4):
        byte = read_exact(sock, 1, buffer)[0]
        remaining += (byte & 127) * multiplier
        multiplier *= 128
        if not byte & 128:
            break
    return read_exact(sock, remaining, buffer)


def make_client(controller):
    client_sock, broker_sock = socket.socketpair()
    client_sock.setblocking(False)
    broker_sock.setblocking(False)
    mqttc = Client(CallbackAPIVersion.VERSION2,
                   client_id="example", protocol=MQTTv5)
    mqttc.auth_controller = controller
    mqttc._sock = client_sock
    return mqttc, broker_sock


def demo_success():
    print("1) Successful initial authentication")
    mqttc, broker_sock = make_client(FakeAuthenticator())
    buffer = [b""]
    mqttc._send_connect(60)
    read_packet(broker_sock, buffer)  # CONNECT
    broker_sock.sendall(auth_packet(24, b"server-challenge"))
    mqttc.loop_read()
    answer = read_packet(broker_sock, buffer)
    print("  wire: client AUTH reason code =", answer[0],
          "(24 = Continue authentication)")
    broker_sock.sendall(connack_packet(0))
    mqttc.loop_read()
    print("  connected:", mqttc.is_connected())
    mqttc._sock_close()
    broker_sock.close()


def demo_failure():
    print("2) Rejected challenge never authenticates the connection")
    mqttc, broker_sock = make_client(RejectingAuthenticator())
    buffer = [b""]
    mqttc._send_connect(60)
    read_packet(broker_sock, buffer)  # CONNECT
    broker_sock.sendall(auth_packet(24, b"prove-yourself"))
    rc = mqttc.loop_read()
    print("  loop result:", MQTTErrorCode(rc).name,
          "| connected:", mqttc.is_connected())
    broker_sock.close()


def demo_reauthentication():
    print("3) Multi-step re-authentication on an established connection")
    authenticator = FakeAuthenticator()
    mqttc, broker_sock = make_client(authenticator)
    buffer = [b""]
    mqttc._send_connect(60)
    read_packet(broker_sock, buffer)
    broker_sock.sendall(connack_packet(0))
    mqttc.loop_read()
    print("  initially connected:", mqttc.is_connected())

    mqttc.re_authenticate()
    first = read_packet(broker_sock, buffer)
    # read_packet returns the packet body, whose first byte is the reason code.
    print("  wire: re-authenticate AUTH reason code =", first[0],
          "(25 = Re-authenticate)")

    for challenge in (b"ra-step-1", b"ra-step-2"):
        broker_sock.sendall(auth_packet(24, challenge))
        mqttc.loop_read()
        read_packet(broker_sock, buffer)  # client Continue response

    broker_sock.sendall(auth_packet(0, b"ra-ok"))  # server Success
    mqttc.loop_read()
    final = read_packet(broker_sock, buffer)
    print("  wire: final client AUTH reason code =", final[0],
          "(0 = Success) | connected:", mqttc.is_connected())
    mqttc._sock_close()
    broker_sock.close()


if __name__ == "__main__":
    demo_success()
    demo_failure()
    demo_reauthentication()
