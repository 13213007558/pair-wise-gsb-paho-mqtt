# Copyright (c) 2024 Roger Light and others
#
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Eclipse Public License v2.0
# and Eclipse Distribution License v1.0 which accompany this distribution.
#
# The Eclipse Public License is available at
#    http://www.eclipse.org/legal/epl-v20.html
# and the Eclipse Distribution License is available at
#   http://www.eclipse.org/org/documents/edl-v10.php.

# This example shows how to survive a *process* restart in the middle of
# QoS 1 / QoS 2 exchanges.  The broker keeps the session
# (clean_session=False), and the client keeps its side of the protocol
# state in a FileSessionStore on disk.
#
# Try it:
#   1. Start a local broker (e.g. mosquitto -p 1883).
#   2. Run: python client_session_store.py
#   3. While it publishes, kill it hard:  kill -9 <pid>
#   4. Run it again.  The interrupted QoS 1/2 exchanges resume where they
#      stopped (DUP PUBLISH, or PUBREL if PUBREC had already arrived)
#      instead of starting over or being lost.
#
# The store directory defaults to ./mqtt-session-store; delete it to
# start completely fresh.

import os
import time

import paho.mqtt.client as mqtt
from paho.mqtt.sessionstore import FileSessionStore

BROKER = "localhost"
STORE_DIR = "mqtt-session-store"
CLIENT_ID = "paho-session-store-demo"


def on_connect(mqttc, userdata, flags, reason_code, properties):
    print(f"connected: {reason_code}, session present: {flags.session_present}")
    # Session Present = 1 means the broker still has our session, so the
    # client resumes the persisted exchanges.  Otherwise they are
    # re-established as new exchanges automatically.


def on_publish(mqttc, userdata, mid, reason_code, properties):
    print(f"published mid={mid}")


def main():
    store = FileSessionStore(STORE_DIR)
    outbound, inbound = store.load()
    print(f"recovered from disk: {len(outbound)} outbound, {len(inbound)} inbound exchange(s)")

    mqttc = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=CLIENT_ID,
        clean_session=False,  # the broker must keep the session too
        session_store=store,  # and the client keeps its own side on disk
    )
    mqttc.on_connect = on_connect
    mqttc.on_publish = on_publish

    mqttc.connect(BROKER, 1883, keepalive=60)
    mqttc.loop_start()

    try:
        for count in range(1, 21):
            payload = f"message {count} (pid {os.getpid()})"
            mqttc.publish("paho/session-store/demo", payload, qos=2)
            print(f"sent: {payload}")
            time.sleep(1)  # slow down so you can kill -9 mid-exchange
    except KeyboardInterrupt:
        pass
    finally:
        mqttc.loop_stop()
        mqttc.disconnect()


if __name__ == "__main__":
    main()
