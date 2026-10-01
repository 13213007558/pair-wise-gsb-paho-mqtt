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
"""
Optional durable storage for QoS 1 / QoS 2 session state.

When a process restarts, the broker may still hold the session
(``clean_session=False`` / ``clean_start=False``), but the client library
normally loses its in-memory QoS state.  By passing a `SessionStore`
implementation to ``Client(session_store=...)``, the client persists every
incomplete QoS 1/QoS 2 exchange (packet id, payload and protocol stage) so
that the exchanges can be resumed - or safely restarted - after a restart.

Delivery semantics
==================

The store makes the *protocol* state durable.  It does not, and cannot,
make application callbacks atomic with the network exchange:

* Outbound QoS 1: at-least-once delivery is preserved across restarts.
  If the process dies after the broker received PUBLISH but before PUBACK
  was processed, the message is republished with DUP=1 and may be
  delivered twice.  This is normal QoS 1 behaviour.
* Outbound QoS 2: the exactly-once exchange is resumed at the correct
  stage (PUBLISH/PUBREL) as long as the broker kept the session
  (CONNACK Session Present = 1).  If the session is gone, the message is
  republished as a *new* exchange, which may duplicate a message the
  broker had already fully processed before losing the session.
* Inbound QoS 2: the message is handed to ``on_message`` exactly once per
  process lifetime, but a crash *between* the ``on_message`` callback
  running and the state commit reaching disk causes the message to be
  delivered to the callback a second time after recovery.  Applications
  that need exactly-once *processing* must make their callback
  idempotent or participate in their own transaction; this window cannot
  be eliminated by the client library.
* Inbound QoS 1 / QoS 0: unchanged - duplicates (QoS 1) and loss (QoS 0)
  are possible exactly as without a store.

Write ordering
==============

State is always persisted *before* the packet that depends on it is sent
(PUBLISH, PUBREC, PUBREL), and a record is deleted only after the exchange
completes (PUBACK/PUBCOMP received, or PUBREL delivered to the
application).  A crash at any point therefore leaves a consistent record:
recovery either resumes the exchange or, at worst, repeats a step the
broker has already seen (which MQTT requires brokers to tolerate).
"""
from __future__ import annotations

import abc
import base64
import enum
import json
import os
from dataclasses import dataclass
from typing import Any


class OutboundStage(enum.Enum):
    """Protocol stage of an outbound (client -> broker) QoS 1/2 exchange."""

    #: Message accepted by the client but PUBLISH not (yet) sent, e.g. it
    #: was queued behind the inflight window or the connection was down.
    QUEUED = "queued"
    #: QoS 1: PUBLISH sent, waiting for PUBACK.
    WAIT_PUBACK = "wait_puback"
    #: QoS 2: PUBLISH sent, waiting for PUBREC.
    WAIT_PUBREC = "wait_pubrec"
    #: QoS 2: PUBREC received, PUBREL sent (or about to be), waiting for PUBCOMP.
    WAIT_PUBCOMP = "wait_pubcomp"


class InboundStage(enum.Enum):
    """Protocol stage of an inbound (broker -> client) QoS 2 exchange."""

    #: PUBLISH received, PUBREC sent, waiting for PUBREL.
    WAIT_PUBREL = "wait_pubrel"


@dataclass
class StoredOutboundMessage:
    """Durable record of one incomplete outbound QoS 1/2 exchange."""

    mid: int
    """Packet identifier.  It stays reserved until the exchange completes."""
    topic: bytes
    payload: bytes
    qos: int
    retain: bool
    stage: OutboundStage
    properties: bytes = b""
    """Packed MQTT v5 properties (opaque to the store, empty for v3)."""


@dataclass
class StoredInboundMessage:
    """Durable record of one incomplete inbound QoS 2 exchange.

    The full message is stored so it can still be delivered to the
    application when PUBREL arrives after a restart.
    """

    mid: int
    topic: bytes
    payload: bytes
    retain: bool = False
    dup: bool = False
    properties: bytes = b""
    """Packed MQTT v5 properties (opaque to the store, empty for v3)."""
    stage: InboundStage = InboundStage.WAIT_PUBREL


class SessionStore(abc.ABC):
    """Interface between `Client` and a durable session store.

    Implementations must honour two contracts:

    * Durability: when any ``save_*``/``delete_*`` method returns, the
      change must survive a process crash (e.g. written to stable
      storage, fsync'd).  The client relies on this for its
      persist-before-send ordering.
    * Atomicity per record: a ``save_*`` either fully replaces the record
      or leaves the previous one intact (e.g. write-temp-then-rename).

    Methods are always called while holding the client's internal locks,
    so implementations do not need their own locking for a single client.
    Sharing one store between concurrently running clients (or processes)
    is not supported.
    """

    @abc.abstractmethod
    def load(self) -> tuple[list[StoredOutboundMessage], list[StoredInboundMessage]]:
        """Return all persisted records, ``(outbound, inbound)``.

        Called once per process, before the first CONNECT is sent.
        Must return ``([], [])`` when nothing was stored.
        """

    @abc.abstractmethod
    def save_outbound(self, record: StoredOutboundMessage) -> None:
        """Insert or replace the outbound record for ``record.mid``."""

    @abc.abstractmethod
    def delete_outbound(self, mid: int) -> None:
        """Delete the outbound record for ``mid`` (no-op if absent)."""

    @abc.abstractmethod
    def save_inbound(self, record: StoredInboundMessage) -> None:
        """Insert or replace the inbound record for ``record.mid``."""

    @abc.abstractmethod
    def delete_inbound(self, mid: int) -> None:
        """Delete the inbound record for ``mid`` (no-op if absent)."""

    @abc.abstractmethod
    def clear(self) -> None:
        """Delete all records.  Utility for applications; the client never
        discards outbound messages on its own."""


def _b64(data: bytes) -> str:
    return base64.b64encode(bytes(data)).decode("ascii")


def _unb64(data: str) -> bytes:
    return base64.b64decode(data.encode("ascii"))


class FileSessionStore(SessionStore):
    """Small local `SessionStore` backed by one JSON file per record.

    Each record is written to a temporary file in the same directory,
    fsync'd and atomically renamed over the target, followed by a
    directory fsync, so a crash at any point leaves either the old or the
    new record - never a torn one.  Deletions are likewise followed by a
    directory fsync.

    This implementation is meant for demonstrations, tests and small
    workloads (one fsync pair per protocol step).  It performs no
    cross-process locking: do not point two running clients at the same
    directory.
    """

    def __init__(self, directory: str | os.PathLike[str]) -> None:
        self._dir = os.fspath(directory)
        os.makedirs(self._dir, exist_ok=True)

    @property
    def directory(self) -> str:
        return self._dir

    def _path(self, kind: str, mid: int) -> str:
        return os.path.join(self._dir, f"{kind}-{mid:05d}.json")

    def _fsync_dir(self) -> None:
        fd = os.open(self._dir, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def _write(self, path: str, doc: dict[str, Any]) -> None:
        tmp_path = f"{path}.{os.getpid()}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as tmp_file:
            json.dump(doc, tmp_file)
            tmp_file.flush()
            os.fsync(tmp_file.fileno())
        os.replace(tmp_path, path)
        self._fsync_dir()

    def _remove(self, path: str) -> None:
        try:
            os.unlink(path)
        except FileNotFoundError:
            return
        self._fsync_dir()

    def load(self) -> tuple[list[StoredOutboundMessage], list[StoredInboundMessage]]:
        outbound: list[StoredOutboundMessage] = []
        inbound: list[StoredInboundMessage] = []
        for name in sorted(os.listdir(self._dir)):
            path = os.path.join(self._dir, name)
            if name.endswith(".tmp"):
                # Leftover from a crash mid-write: the real record was
                # never renamed into place, so this is safe to drop.
                self._remove(path)
                continue
            if not name.startswith(("out-", "in-")):
                continue
            try:
                with open(path, encoding="utf-8") as record_file:
                    doc = json.load(record_file)
            except FileNotFoundError:
                # Deleted by the running client between listdir and open.
                continue
            if name.startswith("out-"):
                outbound.append(StoredOutboundMessage(
                    mid=doc["mid"],
                    topic=_unb64(doc["topic"]),
                    payload=_unb64(doc["payload"]),
                    qos=doc["qos"],
                    retain=doc["retain"],
                    stage=OutboundStage(doc["stage"]),
                    properties=_unb64(doc["properties"]),
                ))
            else:
                inbound.append(StoredInboundMessage(
                    mid=doc["mid"],
                    topic=_unb64(doc["topic"]),
                    payload=_unb64(doc["payload"]),
                    retain=doc["retain"],
                    dup=doc["dup"],
                    properties=_unb64(doc["properties"]),
                    stage=InboundStage(doc["stage"]),
                ))
        return outbound, inbound

    def save_outbound(self, record: StoredOutboundMessage) -> None:
        self._write(self._path("out", record.mid), {
            "mid": record.mid,
            "topic": _b64(record.topic),
            "payload": _b64(record.payload),
            "qos": record.qos,
            "retain": record.retain,
            "stage": record.stage.value,
            "properties": _b64(record.properties),
        })

    def delete_outbound(self, mid: int) -> None:
        self._remove(self._path("out", mid))

    def save_inbound(self, record: StoredInboundMessage) -> None:
        self._write(self._path("in", record.mid), {
            "mid": record.mid,
            "topic": _b64(record.topic),
            "payload": _b64(record.payload),
            "retain": record.retain,
            "dup": record.dup,
            "stage": record.stage.value,
            "properties": _b64(record.properties),
        })

    def delete_inbound(self, mid: int) -> None:
        self._remove(self._path("in", mid))

    def clear(self) -> None:
        for name in os.listdir(self._dir):
            if name.startswith(("out-", "in-")):
                self._remove(os.path.join(self._dir, name))
