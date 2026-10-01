#!/usr/bin/python3
# -*- coding: utf-8 -*-

# This demonstrates the optional persistent session store: QoS 1/2
# protocol state (packet id, payload, exchange stage) is written to a
# local directory *before* each packet that advances the exchange is sent,
# so killing and restarting this process resumes unfinished exchanges
# instead of losing them.
#
# Try it:
#   1. Run against a broker, let it publish a few QoS 2 messages.
#   2. Kill the process (SIGKILL) while messages are in flight.
#   3. Start it again with the same --store-dir: in-flight exchanges
#      resume from their persisted stage (PUBLISH with DUP=1, or PUBREL
#      if PUBREC was already received) instead of starting over.

import argparse
import time

import context  # Ensures paho is in PYTHONPATH

import paho.mqtt.client as mqtt
from paho.mqtt.sessionstore import FileSessionStore


def on_connect(mqttc, obj, flags, reason_code, properties):
    print(f"connected: {reason_code} (session present: {flags.session_present})")


def on_publish(mqttc, obj, mid, reason_code, properties):
    print(f"published mid={mid}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=1883)
    parser.add_argument("--store-dir", default="./mqtt-session-store")
    parser.add_argument("--count", type=int, default=10)
    args = parser.parse_args()

    mqttc = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id="session-store-demo",
        # The broker must keep its side of the session as well.
        clean_session=False,
        # Persist our side of QoS 1/2 exchanges so they survive a restart.
        session_store=FileSessionStore(args.store_dir),
    )
    mqttc.on_connect = on_connect
    mqttc.on_publish = on_publish

    mqttc.connect(args.host, args.port)
    mqttc.loop_start()

    for i in range(args.count):
        info = mqttc.publish("session-store/demo", f"message {i}", qos=2)
        print(f"queued mid={info.mid}")
        time.sleep(1)

    mqttc.disconnect()
    mqttc.loop_stop()


if __name__ == "__main__":
    main()

