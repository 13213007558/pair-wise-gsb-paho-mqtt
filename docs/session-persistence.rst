Session persistence
===================

.. module:: paho.mqtt.sessionstore

The broker keeps its side of a persistent session (subscriptions,
in-flight QoS 1/2 exchanges) across connections when the client connects
with ``clean_session=False`` (MQTT v3) or ``clean_start=False`` (MQTT v5).
The client, however, used to keep its side only in memory: a *process*
restart lost every in-flight QoS 1/2 exchange and the packet-id counter.

Attaching a :class:`SessionStore` to :class:`paho.mqtt.client.Client`
closes that gap::

    from paho.mqtt.client import Client, CallbackAPIVersion
    from paho.mqtt.sessionstore import FileSessionStore

    client = Client(
        CallbackAPIVersion.VERSION2,
        client_id="my-client",
        clean_session=False,
        session_store=FileSessionStore("/var/lib/myapp/mqtt-session"),
    )

What is persisted
-----------------

For every in-flight QoS 1/2 exchange the client stores:

* the packet id (mid), topic, payload, QoS, retain and DUP flags,
* the MQTT v5 PUBLISH properties, if any,
* the protocol stage: waiting for PUBACK, PUBREC, PUBREL or PUBCOMP
  (including the queued/not-yet-sent stages),
* the packet-id counter, so an id is never reused while its previous
  exchange might still be incomplete.

Write ordering
--------------

The record is written *before* the packet that advances the exchange is
put on the wire (PUBLISH, PUBREL, PUBREC), and removed only once the
exchange is complete (PUBACK/PUBCOMP received, PUBCOMP sent). A crash
after any single step therefore always leaves a consistent record: the
exchange resumes from the last persisted stage, possibly repeating one
packet, never skipping one.

Recovery rules
--------------

On reconnect the client combines its persisted state with the CONNACK
result:

* **clean start requested** (``clean_session=True`` / ``clean_start=True``):
  the broker discarded the session. Outgoing messages are re-sent as
  fresh PUBLISH packets; persisted inbound QoS 2 records are dropped.
* **Session Present = 1**: the broker kept the session. Exchanges resume
  exactly where they stopped: QoS 1 and QoS 2-before-PUBREC re-send
  PUBLISH with DUP=1; QoS 2 waiting for PUBCOMP re-sends only PUBREL;
  inbound QoS 2 exchanges wait for the broker to re-send PUBREL.
* **Session Present = 0** (but a persistent session was requested): the
  broker lost the session. A QoS 2 exchange that was waiting for PUBCOMP
  is *restarted* from PUBLISH (DUP=1) instead of sending a PUBREL the
  broker would not understand; inbound QoS 2 records are dropped.

Delivery semantics
------------------

* Outgoing QoS 1: **at-least-once** (unchanged). Duplicates are possible
  whenever a PUBLISH or PUBACK is lost or a crash happens between the
  broker processing PUBLISH and the client persisting the PUBACK.
* Outgoing QoS 2: **exactly-once at the protocol level**, now also across
  process restarts, as long as the store and the broker session survive.
* Incoming QoS 2: the client resumes the exchange correctly, but see the
  crash window below.

What is NOT promised
--------------------

The store is not transactional with the application or the network, so
some crash windows remain and are called out explicitly:

* ``on_message`` runs *before* the completion of an inbound QoS 2
  exchange is persisted. If the process crashes after the callback ran
  but before the record is removed, the message is delivered to the
  application **again** after restart. Inbound delivery to the
  application is therefore **at-least-once**; applications needing
  exactly-once processing must deduplicate themselves (e.g. record the
  packet id atomically with their own state).
* ``on_publish`` may not have run for an exchange that completed at the
  protocol level just before a crash.
* If the *store itself* is lost (disk failure, deleted directory) while
  the broker still has the session, the client cannot resume: inbound
  QoS 2 messages already PUBRECed may be lost (the client answers the
  re-sent PUBREL with PUBCOMP, see issue #284), and the broker may hold
  packet ids the client no longer knows. Treat the store as part of the
  client state and back it up accordingly.

API
---

.. autoclass:: SessionStore
    :members:

.. autoclass:: FileSessionStore
    :members:

.. autoclass:: PendingMessage
    :members:

