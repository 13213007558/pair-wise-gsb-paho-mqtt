"""
Optional persistent session storage for QoS 1/2 message exchanges.

The broker keeps its side of a persistent session across connections, but
the paho client historically kept its QoS 1/2 protocol state only in
memory: if the *process* restarts, the client forgot which PUBLISH packets
were waiting for PUBACK/PUBREC/PUBREL/PUBCOMP and could neither resume the
exchanges nor know which packet identifiers were still in use.

This module defines a small pluggable interface, `SessionStore`, that the
client uses to persist every protocol-stage transition of an in-flight
QoS 1/2 exchange *before* it sends the packet that advances the exchange.
A demonstration implementation, `FileSessionStore`, keeps the state in a
local directory using atomic file renames.

Delivery semantics
==================

Attaching a store lets the client *resume interrupted protocol exchanges*
after a process restart, provided the broker still has the session
(CONNACK Session Present flag). Concretely:

* Outgoing QoS 1: at-least-once. A PUBLISH that was persisted but never
  PUBACKed is retransmitted with DUP=1. The broker (or the receiving
  application) may see a duplicate; that is inherent to QoS 1.
* Outgoing QoS 2: exactly-once at the *protocol* level. The exchange
  resumes from the persisted stage (PUBLISH, or PUBREL if PUBREC was
  already received), so the packet identifier is never reused for a
  different message while the old exchange is incomplete.
* Incoming QoS 2: the client remembers it already sent PUBREC and answers
  the broker's retransmitted PUBREL after restart.

What a store can NOT promise
----------------------------

Persistence is not transactional with the application or the network, so
there are crash windows that no client-side store can close:

* A crash *after* the broker processed a packet but *before* the client
  persisted the resulting state transition means the client re-does the
  previous step after restart (e.g. re-sends PUBLISH with DUP=1). This is
  safe, but may produce duplicates at the application level.
* For incoming messages, the application callback (``on_message``) runs
  before the client durably records that the exchange finished. A crash in
  between means the message is delivered to the application *again* after
  restart. Applications that need exactly-once processing must make their
  own processing atomic with their own durable state (e.g. record the
  packet id in the same database transaction as the work).
* Symmetrically, a crash after the application callback but before the
  final acknowledgement is persisted can leave the broker re-sending
  PUBREL; the client answers PUBCOMP without re-delivering only if the
  persisted record is still present.

In short: the store makes the *protocol state machine* crash-safe and
prevents packet-id reuse; it upgrades nothing to end-to-end exactly-once.
"""

from __future__ import annotations

import abc
import base64
import json
import logging
import os
import tempfile
import threading
from dataclasses import asdict, dataclass
from typing import Any

from .enums import MessageState

_logger = logging.getLogger(__name__)

#: Direction value for messages the client sends (waiting for PUBACK/PUBREC/PUBCOMP).
DIRECTION_OUT = "out"
#: Direction value for messages the client receives (QoS 2, waiting for PUBREL).
DIRECTION_IN = "in"


@dataclass
class PendingMessage:
    """A serializable snapshot of one in-flight QoS 1/2 exchange.

    ``state`` is a `paho.mqtt.enums.MessageState` value and identifies the
    protocol stage, e.g. waiting for PUBACK, PUBREC, PUBREL or PUBCOMP.
    ``properties`` is the MQTT v5 property blob of the PUBLISH packet,
    already encoded (``Properties.pack()``), or None.
    """

    direction: str
    mid: int
    topic: bytes
    payload: bytes
    qos: int
    retain: bool
    dup: bool
    state: MessageState
    timestamp: float
    properties: bytes | None = None


class SessionStore(abc.ABC):
    """Interface between `paho.mqtt.client.Client` and a durable store.

    Implementations must be thread-safe: the client calls these methods
    while holding its internal message locks, from both the application
    thread (``publish()``) and the network loop thread.

    Write-ordering contract the client relies on:

    * `put` is called *before* the packet that advances the exchange is
      sent, so a crash at any point leaves a record describing the stage
      the exchange had reached.
    * `remove` is called only once the exchange is complete (PUBACK/PUBCOMP
      received for outgoing, PUBCOMP sent for incoming).
    """

    @abc.abstractmethod
    def open(self, client_id: str) -> None:
        """Open the store for the given client id. Called once per client."""

    @abc.abstractmethod
    def close(self) -> None:
        """Release any resources. The store may be re-opened afterwards."""

    @abc.abstractmethod
    def clear(self) -> None:
        """Drop all persisted records and the packet-id counter."""

    @abc.abstractmethod
    def put(self, record: PendingMessage) -> None:
        """Insert or replace the record for (record.direction, record.mid)."""

    @abc.abstractmethod
    def remove(self, direction: str, mid: int) -> None:
        """Delete the record for (direction, mid). Missing records are ignored."""

    @abc.abstractmethod
    def load_all(self) -> list[PendingMessage]:
        """Return every persisted record (order does not matter)."""

    @abc.abstractmethod
    def set_last_mid(self, mid: int) -> None:
        """Persist the packet-id counter so ids are never reused after restart."""

    @abc.abstractmethod
    def get_last_mid(self) -> int:
        """Return the persisted packet-id counter, or 0 if none was stored."""


def _b64encode(data: bytes | bytearray | None) -> str | None:
    if data is None:
        return None
    return base64.b64encode(bytes(data)).decode("ascii")


def _b64decode(data: str | None) -> bytes | None:
    if data is None:
        return None
    return base64.b64decode(data.encode("ascii"))


class FileSessionStore(SessionStore):
    """Small local `SessionStore` backed by one JSON file per record.

    Each pending message is stored as ``<direction>-<mid>.json`` inside
    ``directory``; the packet-id counter lives in ``meta.json``. Writes go
    to a temporary file which is fsynced and then atomically renamed over
    the target, so a crash at any point leaves either the old or the new
    record, never a torn file. This is intended as a reference/demo
    implementation, not a high-throughput store.
    """

    META_FILENAME = "meta.json"

    def __init__(self, directory: str | os.PathLike[str]):
        self._directory = os.fspath(directory)
        self._lock = threading.Lock()
        self._client_id: str | None = None

    def open(self, client_id: str) -> None:
        with self._lock:
            self._client_id = client_id
            os.makedirs(self._directory, exist_ok=True)

    def close(self) -> None:
        # Nothing to release: every write is self-contained and fsynced.
        pass

    def clear(self) -> None:
        with self._lock:
            if not os.path.isdir(self._directory):
                return
            for name in os.listdir(self._directory):
                if name.endswith(".json"):
                    try:
                        os.unlink(os.path.join(self._directory, name))
                    except FileNotFoundError:
                        pass
            self._fsync_dir()

    def put(self, record: PendingMessage) -> None:
        data = asdict(record)
        data["state"] = int(record.state)
        data["topic"] = _b64encode(record.topic)
        data["payload"] = _b64encode(record.payload)
        data["properties"] = _b64encode(record.properties)
        with self._lock:
            self._write_json(self._record_path(record.direction, record.mid), data)

    def remove(self, direction: str, mid: int) -> None:
        with self._lock:
            try:
                os.unlink(self._record_path(direction, mid))
            except FileNotFoundError:
                pass
            self._fsync_dir()

    def load_all(self) -> list[PendingMessage]:
        records: list[PendingMessage] = []
        with self._lock:
            if not os.path.isdir(self._directory):
                return records
            for name in sorted(os.listdir(self._directory)):
                if name == self.META_FILENAME or not name.endswith(".json"):
                    continue
                path = os.path.join(self._directory, name)
                try:
                    with open(path, encoding="utf-8") as handle:
                        data: dict[str, Any] = json.load(handle)
                    records.append(
                        PendingMessage(
                            direction=data["direction"],
                            mid=int(data["mid"]),
                            topic=_b64decode(data["topic"]) or b"",
                            payload=_b64decode(data["payload"]) or b"",
                            qos=int(data["qos"]),
                            retain=bool(data["retain"]),
                            dup=bool(data["dup"]),
                            state=MessageState(int(data["state"])),
                            timestamp=float(data["timestamp"]),
                            properties=_b64decode(data.get("properties")),
                        )
                    )
                except (OSError, ValueError, KeyError, TypeError) as err:
                    # A record we cannot parse must not crash the client;
                    # skip it and keep the exchange semantics conservative
                    # (the broker will retry its side if it still has it).
                    _logger.warning("Ignoring unreadable session record %s: %s", path, err)
        return records

    def set_last_mid(self, mid: int) -> None:
        with self._lock:
            self._write_json(
                os.path.join(self._directory, self.META_FILENAME), {"last_mid": int(mid)}
            )

    def get_last_mid(self) -> int:
        with self._lock:
            try:
                with open(
                    os.path.join(self._directory, self.META_FILENAME), encoding="utf-8"
                ) as handle:
                    return int(json.load(handle)["last_mid"])
            except (OSError, ValueError, KeyError, TypeError):
                return 0

    def _record_path(self, direction: str, mid: int) -> str:
        return os.path.join(self._directory, f"{direction}-{mid}.json")

    def _write_json(self, path: str, data: dict[str, Any]) -> None:
        os.makedirs(self._directory, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(dir=self._directory, suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(data, handle)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_path, path)
        except BaseException:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise
        self._fsync_dir()

    def _fsync_dir(self) -> None:
        # Ensure the directory entry itself is durable; not all platforms
        # allow fsync on a directory, so failure is non-fatal.
        try:
            dir_fd = os.open(self._directory, os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(dir_fd)
        except OSError:
            pass
        finally:
            os.close(dir_fd)
