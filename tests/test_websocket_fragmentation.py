"""Compositional fragmentation tests for the WebSocket transport.

A single valid MQTT byte stream is split into WebSocket frames at many
different positions (level 1) and each resulting wire stream is additionally
chunked by socket.recv() at different boundaries (level 2).  Every
combination must deliver exactly the same callback messages and
acknowledgement packets.

A second group of tests injects illegal continuations, text frames, ping
frames and a mid-stream close, verifying that bytes belonging to control
frames never end up in a delivered MQTT message.

Everything runs against an in-memory scripted socket; no server, no network.
On a mismatch the equivalence test shrinks the failing case and reports the
shortest two-level fragmentation sequence that still fails.
"""

import itertools
import struct
from collections import deque, namedtuple
from unittest.mock import patch

import pytest

from paho.mqtt.client import (
    CallbackAPIVersion,
    Client,
    MQTTErrorCode,
    MQTTv311,
    _WebsocketWrapper,
)
from paho.mqtt.enums import _ConnectionState

OP_CONT = _WebsocketWrapper.OPCODE_CONTINUATION
OP_TEXT = _WebsocketWrapper.OPCODE_TEXT
OP_BINARY = _WebsocketWrapper.OPCODE_BINARY
OP_CLOSE = _WebsocketWrapper.OPCODE_CONNCLOSE
OP_PING = _WebsocketWrapper.OPCODE_PING
OP_PONG = _WebsocketWrapper.OPCODE_PONG

OPCODE_NAMES = {
    OP_CONT: "CONTINUATION",
    OP_TEXT: "TEXT",
    OP_BINARY: "BINARY",
    OP_CLOSE: "CLOSE",
    OP_PING: "PING",
    OP_PONG: "PONG",
}

# ---------------------------------------------------------------------------
# MQTT stream under test
# ---------------------------------------------------------------------------

CONNACK = b"\x20\x02\x00\x00"
PUB1_TOPIC = b"sensor/temp"
PUB1_PAYLOAD = b"hello-websocket-" * 12  # 192 bytes -> two-byte remaining length
PUB1_MID = 7
PUB2_TOPIC = b"sensor/humidity"
PUB2_PAYLOAD = b"dry"
PUBACK = b"\x40\x02" + struct.pack("!H", PUB1_MID)


def encode_remaining_length(length):
    out = bytearray()
    while True:
        digit = length % 128
        length //= 128
        if length:
            out.append(digit | 0x80)
        else:
            out.append(digit)
            return bytes(out)


def publish_packet(topic, payload, qos, mid=None):
    body = struct.pack("!H", len(topic)) + topic
    if mid is not None:
        body += struct.pack("!H", mid)
    body += payload
    return bytes([0x30 | (qos << 1)]) + encode_remaining_length(len(body)) + body


PUB1_PACKET = publish_packet(PUB1_TOPIC, PUB1_PAYLOAD, qos=1, mid=PUB1_MID)
PUB2_PACKET = publish_packet(PUB2_TOPIC, PUB2_PAYLOAD, qos=0)
MQTT_STREAM = CONNACK + PUB1_PACKET + PUB2_PACKET

# A complete, valid MQTT packet used as payload of control/text frames.  If
# any of those frames leaked into the MQTT layer, this exact message would
# appear in the on_message callback.
DECOY_PAYLOAD = b"INJECTED-CONTROL-BYTES"
DECOY_PACKET = publish_packet(b"evil/topic", DECOY_PAYLOAD, qos=0)
DECOY_MESSAGE = ("message", "evil/topic", DECOY_PAYLOAD, 0)

EXPECTED_EVENTS = [
    ("connect",),
    ("message", PUB1_TOPIC.decode(), PUB1_PAYLOAD, 1),
    ("message", PUB2_TOPIC.decode(), PUB2_PAYLOAD, 0),
]
EXPECTED_SENT = [(OP_BINARY, PUBACK)]

# recv() chunk-size patterns (level 2).  Sizes are cycled until the wire
# bytes are exhausted; a size larger than the remainder takes the remainder.
CHUNK_PATTERNS = {
    "whole": [1 << 30],
    "bytewise": [1],
    "pairs": [2],
    "mixed": [1, 2, 3, 5, 8],
}

# ---------------------------------------------------------------------------
# In-memory plumbing
# ---------------------------------------------------------------------------


def ws_frame(opcode, fin, payload):
    """Build one server->client (unmasked) WebSocket frame."""
    first = (0x80 if fin else 0x00) | opcode
    length = len(payload)
    if length < 126:
        header = struct.pack("!BB", first, length)
    elif length < 65536:
        header = struct.pack("!BBH", first, 126, length)
    else:
        header = struct.pack("!BBQ", first, 127, length)
    return header + payload


def expand_chunks(wire, pattern):
    chunks = []
    pos = 0
    index = 0
    while pos < len(wire):
        size = pattern[index % len(pattern)]
        chunks.append(wire[pos:pos + size])
        pos += size
        index += 1
    return chunks


class ChunkedSocket:
    """Scripted in-memory socket: recv() returns pre-defined chunks,
    send() records everything written by the client."""

    def __init__(self, chunks, eof="block"):
        self._chunks = deque(chunks)
        self._pending = b""
        self._eof = eof
        self.sent = bytearray()

    @property
    def drained(self):
        return not self._chunks and not self._pending

    def recv(self, count):
        if not self._pending:
            if not self._chunks:
                if self._eof == "close":
                    return b""
                raise BlockingIOError
            self._pending = self._chunks.popleft()
        data = self._pending[:count]
        self._pending = self._pending[count:]
        return data

    def send(self, data):
        self.sent.extend(data)
        return len(data)

    def close(self):
        pass

    def fileno(self):
        return -1


def make_wrapper(sock):
    """Create a _WebsocketWrapper around sock without doing the HTTP handshake."""
    with patch.object(_WebsocketWrapper, "_do_handshake", lambda self, extra_headers: None):
        wrapper = _WebsocketWrapper(sock, "broker.local", 1883, False, "/mqtt", None)
    wrapper.connected = True
    return wrapper


def decode_client_frames(data):
    """Parse client->server WebSocket frames (masked or not) into (opcode, payload)."""
    frames = []
    pos = 0
    while pos < len(data):
        first, second = data[pos], data[pos + 1]
        opcode = first & 0x0F
        masked = bool(second & 0x80)
        length = second & 0x7F
        pos += 2
        if length == 126:
            length, = struct.unpack("!H", data[pos:pos + 2])
            pos += 2
        elif length == 127:
            length, = struct.unpack("!Q", data[pos:pos + 8])
            pos += 8
        if masked:
            mask = data[pos:pos + 4]
            pos += 4
            payload = bytes(data[pos + i] ^ mask[i % 4] for i in range(length))
        else:
            payload = bytes(data[pos:pos + length])
        pos += length
        frames.append((opcode, payload))
    return frames


SessionResult = namedtuple("SessionResult", ["events", "sent", "last_rc"])


def run_session(frames, chunk_pattern, eof="block"):
    """Feed frames (list of (opcode, fin, payload)) through a real Client.

    chunk_pattern is the level-2 recv() chunking.  Returns the recorded
    on_connect/on_message events, the decoded frames the client sent back,
    and the last _packet_read() return code.
    """
    wire = b"".join(ws_frame(opcode, fin, payload) for opcode, fin, payload in frames)
    sock = ChunkedSocket(expand_chunks(wire, chunk_pattern), eof=eof)
    wrapper = make_wrapper(sock)

    mqttc = Client(
        CallbackAPIVersion.VERSION2,
        client_id="ws-frag-test",
        protocol=MQTTv311,
        transport="websockets",
    )
    events = []
    mqttc.on_connect = lambda client, userdata, flags, reason, properties: events.append(("connect",))
    mqttc.on_message = lambda client, userdata, message: events.append(
        ("message", message.topic, bytes(message.payload), message.qos))
    mqttc._sock = wrapper
    mqttc._state = _ConnectionState.MQTT_CS_CONNECTED

    last_rc = MQTTErrorCode.MQTT_ERR_SUCCESS
    for _ in range(100000):
        last_rc = mqttc._packet_read()
        if last_rc == MQTTErrorCode.MQTT_ERR_AGAIN:
            # With eof="close" keep reading so the TCP-level EOF surfaces as
            # MQTT_ERR_CONN_LOST instead of stopping at "no data right now".
            if sock.drained and eof == "block":
                break
        elif last_rc != MQTTErrorCode.MQTT_ERR_SUCCESS:
            break
    else:
        raise AssertionError("packet read loop did not settle")

    return SessionResult(events, decode_client_frames(bytes(sock.sent)), last_rc)


def frames_from_cuts(stream, cuts):
    """Split stream at the given offsets into a BINARY + CONTINUATION frame sequence."""
    bounds = [0, *sorted(cuts), len(stream)]
    frames = []
    for index in range(len(bounds) - 1):
        payload = stream[bounds[index]:bounds[index + 1]]
        opcode = OP_BINARY if index == 0 else OP_CONT
        fin = index == len(bounds) - 2
        frames.append((opcode, fin, payload))
    return frames


# ---------------------------------------------------------------------------
# Failure minimisation: shrink a failing (frames, chunk pattern) pair to the
# shortest two-level fragmentation sequence that still fails
# ---------------------------------------------------------------------------


def minimize_failure(frames, pattern, expected_events, expected_sent):
    def fails(trial_frames, trial_pattern):
        result = run_session(trial_frames, trial_pattern)
        return (result.events, result.sent) != (expected_events, expected_sent)

    # Level 1: merge adjacent data frames while the failure persists
    for _ in range(len(frames) + 1):
        for index in range(len(frames) - 1):
            first, second = frames[index], frames[index + 1]
            if first[0] not in (OP_BINARY, OP_CONT) or second[0] not in (OP_BINARY, OP_CONT):
                continue
            candidate = frames[:index] + [(first[0], second[1], first[2] + second[2])] + frames[index + 2:]
            if fails(candidate, pattern):
                frames = candidate
                break
        else:
            break

    # Level 2: merge adjacent recv() chunk sizes while the failure persists
    for _ in range(len(pattern)):
        for index in range(len(pattern) - 1):
            candidate = pattern[:index] + [pattern[index] + pattern[index + 1]] + pattern[index + 2:]
            if fails(frames, candidate):
                pattern = candidate
                break
        else:
            break

    return frames, pattern


def format_fragmentation(frames, pattern):
    lines = ["shortest failing two-level fragmentation sequence:", "  level 1 (websocket frames):"]
    for opcode, fin, payload in frames:
        shown = payload[:24].hex()
        if len(payload) > 24:
            shown += "..."
        lines.append(f"    {OPCODE_NAMES[opcode]} fin={int(fin)} len={len(payload)} payload={shown}")
    lines.append(f"  level 2 (socket.recv chunk sizes): {pattern}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestFragmentationEquivalence:
    """Every way of splitting the same MQTT stream must behave identically."""

    def test_all_splits_match(self):
        stream = MQTT_STREAM
        packet_ends = [len(CONNACK), len(CONNACK) + len(PUB1_PACKET), len(stream)]
        interesting = set(range(1, 9))
        for end in packet_ends:
            interesting.update(end + delta for delta in range(-2, 3))
        interesting.update(range(16, len(stream), 17))
        interesting = sorted(i for i in interesting if 0 < i < len(stream))

        # Each case: (frames, chunk pattern, expected sent frames)
        cases = []

        # One frame for the whole stream, under every recv() chunking
        for pattern in CHUNK_PATTERNS.values():
            cases.append((frames_from_cuts(stream, []), pattern, EXPECTED_SENT))

        # Every single cut position, under every recv() chunking
        for cut in range(1, len(stream)):
            for pattern in CHUNK_PATTERNS.values():
                cases.append((frames_from_cuts(stream, [cut]), pattern, EXPECTED_SENT))

        # Every pair of cut positions at interesting offsets
        for first, second in itertools.combinations(interesting, 2):
            cases.append((frames_from_cuts(stream, [first, second]), CHUNK_PATTERNS["whole"], EXPECTED_SENT))
            cases.append((frames_from_cuts(stream, [first, second]), CHUNK_PATTERNS["bytewise"], EXPECTED_SENT))

        # Every byte in its own frame
        for pattern in (CHUNK_PATTERNS["whole"], CHUNK_PATTERNS["bytewise"], CHUNK_PATTERNS["mixed"]):
            cases.append((frames_from_cuts(stream, list(range(1, len(stream)))), pattern, EXPECTED_SENT))

        # A ping (with a decoy MQTT packet as payload) inserted at every cut.
        # The PONG is sent as soon as the ping frame is consumed, so it
        # precedes the PUBACK unless the ping sits past the first PUBLISH.
        pub1_end = len(CONNACK) + len(PUB1_PACKET)
        for cut in range(1, len(stream)):
            for pattern in (CHUNK_PATTERNS["whole"], CHUNK_PATTERNS["bytewise"]):
                frames = frames_from_cuts(stream, [cut])
                frames.insert(1, (OP_PING, True, DECOY_PACKET))
                if cut < pub1_end:
                    expected_sent = [(OP_PONG, DECOY_PACKET), (OP_BINARY, PUBACK)]
                else:
                    expected_sent = [(OP_BINARY, PUBACK), (OP_PONG, DECOY_PACKET)]
                cases.append((frames, pattern, expected_sent))

        for frames, pattern, expected_sent in cases:
            result = run_session(frames, pattern)
            if (result.events, result.sent) != (EXPECTED_EVENTS, expected_sent):
                frames, pattern = minimize_failure(frames, pattern, EXPECTED_EVENTS, expected_sent)
                pytest.fail(
                    f"fragmented stream diverged:\n{format_fragmentation(frames, pattern)}\n"
                    f"  expected events: {EXPECTED_EVENTS}\n  got events: {result.events}\n"
                    f"  expected sent: {expected_sent}\n  got sent: {result.sent}"
                )


class TestInvalidFrames:
    """Control/text frame bytes must never be assembled into an MQTT message."""

    @pytest.mark.parametrize("pattern_name", ["whole", "bytewise"])
    def test_ping_interleaved_inside_fragmented_message(self, pattern_name):
        cut = len(CONNACK) + 60  # inside the first PUBLISH payload
        frames = [
            (OP_BINARY, False, MQTT_STREAM[:cut]),
            (OP_PING, True, DECOY_PACKET),
            (OP_CONT, True, MQTT_STREAM[cut:]),
        ]
        result = run_session(frames, CHUNK_PATTERNS[pattern_name])
        assert result.events == EXPECTED_EVENTS
        assert result.sent == [(OP_PONG, DECOY_PACKET), (OP_BINARY, PUBACK)]

    @pytest.mark.parametrize("opcode", [OP_TEXT, OP_PONG])
    @pytest.mark.parametrize("pattern_name", ["whole", "bytewise"])
    def test_non_binary_frame_inside_fragmented_message(self, opcode, pattern_name):
        cut = len(CONNACK) + 60
        frames = [
            (OP_BINARY, False, MQTT_STREAM[:cut]),
            (opcode, True, DECOY_PACKET),
            (OP_CONT, True, MQTT_STREAM[cut:]),
        ]
        result = run_session(frames, CHUNK_PATTERNS[pattern_name])
        assert result.events == EXPECTED_EVENTS
        assert result.sent == EXPECTED_SENT

    def test_lone_continuation_is_passed_through(self):
        # _recv_impl explicitly documents that it does not enforce proper
        # continuation sequencing: a stray continuation frame is treated as
        # binary data.  Pin down that (lenient) behaviour so a future fix is
        # noticed; control frames above are the ones that must never leak.
        frames = [
            (OP_CONT, True, DECOY_PACKET),
            (OP_BINARY, True, MQTT_STREAM),
        ]
        result = run_session(frames, CHUNK_PATTERNS["whole"])
        assert result.events == [DECOY_MESSAGE] + EXPECTED_EVENTS
        assert result.sent == EXPECTED_SENT

    @pytest.mark.parametrize("cut", [
        len(CONNACK) + 60,                 # mid PUBLISH: partial packet must not be delivered
        len(CONNACK) + len(PUB1_PACKET),   # exactly after the first PUBLISH
    ])
    @pytest.mark.parametrize("pattern_name", ["whole", "bytewise"])
    def test_close_mid_stream(self, cut, pattern_name):
        close_payload = struct.pack("!H", 1000) + DECOY_PACKET
        frames = [
            (OP_BINARY, False, MQTT_STREAM[:cut]),
            (OP_CLOSE, True, close_payload),
        ]
        result = run_session(frames, CHUNK_PATTERNS[pattern_name], eof="close")

        delivered = [event for event in EXPECTED_EVENTS if event[0] == "message"]
        if cut < len(CONNACK) + len(PUB1_PACKET):
            delivered = []
        else:
            delivered = delivered[:1]
        assert result.events == [("connect",)] + delivered
        expected_sent = [(OP_CLOSE, close_payload)]
        if delivered:
            expected_sent.insert(0, (OP_BINARY, PUBACK))
        assert result.sent == expected_sent
        assert result.last_rc == MQTTErrorCode.MQTT_ERR_CONN_LOST
        # The close-frame payload (which contains a decoy MQTT packet) must
        # never surface as a message.
        assert DECOY_MESSAGE not in result.events


class TestPartialFrameHeader:
    """socket.recv() may return less than a full frame header."""

    def test_half_of_short_header(self):
        # First recv() returns a single byte: half of the 2-byte frame header.
        result = run_session([(OP_BINARY, True, MQTT_STREAM)], [1, 1 << 30])
        assert result.events == EXPECTED_EVENTS
        assert result.sent == EXPECTED_SENT

    @pytest.mark.parametrize("first_chunks", [
        [1, 1, 1],      # byte 0, byte 1 (=126), then the 2-byte length one by one
        [3],            # stops in the middle of the 16-bit length field
        [2, 1],
    ])
    def test_split_16bit_extended_length(self, first_chunks):
        # PUB1 makes the frame payload > 125 bytes, so the frame uses the
        # 16-bit extended length (4-byte header).
        result = run_session([(OP_BINARY, True, MQTT_STREAM)], [*first_chunks, 1 << 30])
        assert result.events == EXPECTED_EVENTS
        assert result.sent == EXPECTED_SENT

    @pytest.mark.parametrize("first_chunks", [
        [1, 1, 1, 1, 1],   # split the 8-byte length field byte by byte
        [5],               # stops in the middle of the 64-bit length field
    ])
    def test_split_64bit_extended_length(self, first_chunks):
        big_payload = b"x" * 70000
        stream = CONNACK + publish_packet(b"big/topic", big_payload, qos=0)
        result = run_session([(OP_BINARY, True, stream)], [*first_chunks, 1 << 30])
        assert result.events == [("connect",), ("message", "big/topic", big_payload, 0)]
        assert result.sent == []
