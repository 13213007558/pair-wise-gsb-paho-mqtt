Session persistence
===================

By default the client keeps QoS 1 / QoS 2 protocol state in memory.  If
the *process* restarts, that state is lost even though the broker may
still hold the session (``clean_session=False`` / ``clean_start=False``).
Passing a ``session_store`` to the client makes the client-side state
durable as well::

    import paho.mqtt.client as mqtt
    from paho.mqtt.sessionstore import FileSessionStore

    mqttc = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id="my-client",
        clean_session=False,                    # broker keeps the session
        session_store=FileSessionStore("/var/lib/myapp/mqtt-session"),
    )

What is stored
--------------

Every incomplete QoS 1/QoS 2 exchange is persisted with its packet
identifier, topic, payload, MQTT v5 properties and its protocol stage:

* outbound: ``QUEUED`` (accepted but not yet sent), ``WAIT_PUBACK``
  (QoS 1), ``WAIT_PUBREC`` and ``WAIT_PUBCOMP`` (QoS 2);
* inbound (QoS 2 only): ``WAIT_PUBREL`` - the full message is stored so
  it can still be delivered to ``on_message`` when PUBREL arrives after
  a restart.

Write ordering
--------------

State is always persisted *before* the packet that depends on it is
sent, and a record is deleted only after the exchange completes:

* PUBLISH (QoS > 0): record written, then PUBLISH sent;
* PUBREC received: record updated to ``WAIT_PUBCOMP``, then PUBREL sent;
* PUBACK/PUBCOMP received: record deleted;
* inbound PUBLISH (QoS 2): record written, then PUBREC sent;
* PUBREL received: message delivered to the application, record deleted,
  then PUBCOMP sent (with ``manual_ack``, the record is deleted in
  ``ack()`` instead, after the application finished processing).

A crash at any point therefore leaves a consistent record: recovery
either resumes the exchange or repeats a step the broker has already
seen, which MQTT requires brokers to tolerate.

Recovery
--------

On the first connect of a new process the stored exchanges are loaded,
and their packet identifiers stay reserved - they are never handed out
to new messages while the old exchange is incomplete.  What happens next
depends on the CONNACK Session Present flag and the requested clean
start:

* Session Present = 1 (and no clean start requested): exchanges are
  *resumed*.  Outbound messages waiting for PUBACK/PUBREC are re-sent as
  PUBLISH with DUP=1; an exchange waiting for PUBCOMP resumes with
  PUBREL - the PUBLISH is *not* sent again.  Inbound QoS 2 exchanges
  wait for the broker to re-send PUBREL.
* Session Present = 0, or clean start requested: the broker discarded
  the session, so the old exchanges no longer exist on its side.
  Outbound messages are *re-established* as brand new exchanges
  (PUBLISH with DUP=0, never a stray PUBREL).  Stale inbound records are
  dropped; if the broker still has the QoS 2 message queued it is
  redelivered as a new PUBLISH and processed normally.

Delivery semantics - honestly
-----------------------------

The store makes the *protocol* state durable.  It cannot make your
application callback atomic with the network exchange:

* Outbound QoS 1: at-least-once is preserved across restarts.  A crash
  after the broker received PUBLISH but before PUBACK was processed
  causes a retransmission (DUP=1); the receiver may see the message
  twice.  This is normal QoS 1 behaviour.
* Outbound QoS 2: the exactly-once exchange is resumed at the right
  stage as long as the broker kept the session.  If the session is gone,
  the message is republished as a new exchange and may duplicate a
  message the broker had fully processed before losing the session.
* Inbound QoS 2: there is an unavoidable crash window *between* the
  ``on_message`` callback running and the state commit reaching disk.
  A crash inside that window causes the message to be delivered to the
  callback a second time after recovery.  Applications needing
  exactly-once *processing* must make the callback idempotent or attach
  it to their own transaction.  Using ``manual_ack`` and calling
  ``ack()`` only after processing shrinks the window to the ack itself
  but does not remove it.
* Inbound QoS 1 / QoS 0: unchanged - duplicates (QoS 1) and loss (QoS 0)
  remain possible exactly as without a store.

Implementations
---------------

``FileSessionStore(directory)`` is a small reference implementation
shipping with the library: one JSON file per record, written to a
temporary file, fsync'd and atomically renamed into place, with a
directory fsync after every change.  It is meant for demos, tests and
small workloads, and performs no cross-process locking - do not point
two running clients at the same directory.

To plug in another backend (SQLite, Redis, ...), subclass
``paho.mqtt.sessionstore.SessionStore`` and honour its contract:
``save_*``/``delete_*`` must be durable (on stable storage) before they
return, and a ``save_*`` must atomically replace the previous record.
See the module docstring of ``paho.mqtt.sessionstore`` for details.

API reference
-------------

.. automodule:: paho.mqtt.sessionstore
   :members:
   :undoc-members:
