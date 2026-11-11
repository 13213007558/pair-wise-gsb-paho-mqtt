"""验证 paho-mqtt QoS 2 + manual_ack=True 的投递与确认行为。

不连接真实 broker：用 socketpair 替换 socket.create_connection，
测试脚本直接驱动协议报文（CONNACK/PUBLISH/PUBREL/...），
记录每个方向上的完整报文序列。

运行: python3 qos2-manual-ack/run_experiments.py
"""
import queue
import socket
import struct
import sys
import threading
import time
import unittest.mock
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

import paho.mqtt.client as mqtt
import paho.mqtt.client as client_mod
from tests import paho_test

holder = [None]

PKT = {
    0x10: "CONNECT", 0x20: "CONNACK", 0x30: "PUBLISH", 0x40: "PUBACK",
    0x50: "PUBREC", 0x60: "PUBREL", 0x70: "PUBCOMP", 0x80: "SUBSCRIBE",
    0x90: "SUBACK", 0xC0: "PINGREQ", 0xD0: "PINGRESP", 0xE0: "DISCONNECT",
}


class Transcript:
    def __init__(self, title):
        self.title = title
        self.t0 = time.monotonic()
        self.lines = []

    def log(self, who, what):
        self.lines.append(f"[{time.monotonic() - self.t0:7.3f}s] {who:4s} {what}")

    def render(self):
        return f"===== {self.title} =====\n" + "\n".join(self.lines)


class Broker:
    """socketpair 的 broker 端：按 MQTT 帧读包、记录、断言。"""

    def __init__(self, sock, tr):
        self.sock = sock
        self.tr = tr
        self.sock.settimeout(10)

    def _read(self, n):
        buf = b""
        while len(buf) < n:
            chunk = self.sock.recv(n - len(buf))
            if not chunk:
                raise ConnectionError("client closed connection")
            buf += chunk
        return buf

    def recv_packet(self, timeout=10):
        self.sock.settimeout(timeout)
        b1 = self._read(1)[0]
        mult, rl = 1, 0
        while True:
            byte = self._read(1)[0]
            rl += (byte & 0x7F) * mult
            if not byte & 0x80:
                break
            mult *= 128
        body = self._read(rl) if rl else b""
        name = PKT.get(b1 & 0xF0, f"0x{b1:02x}")
        if (b1 & 0xF0) == 0x30:
            name += f"(dup={(b1 >> 3) & 1},qos={(b1 >> 1) & 3})"
        self.tr.log("C->B", f"{name} body={body.hex() or '-'}")
        return name.split("(")[0], body

    def expect(self, want, timeout=10):
        got, body = self.recv_packet(timeout)
        assert got == want, f"expected {want}, got {got}"
        return body

    def send(self, name, packet):
        self.tr.log("B->C", f"{name} hex={packet.hex()}")
        self.sock.sendall(packet)

    def assert_silence(self, dur, note=""):
        self.sock.settimeout(dur)
        try:
            data = self.sock.recv(1)
        except socket.timeout:
            suffix = "; " + note if note else ""
            self.tr.log(" ...", f"(静默 {dur}s: 客户端未发送任何报文{suffix})")
            return
        raise AssertionError(f"expected silence, got bytes {data.hex()}")

    def drop(self):
        self.tr.log("BROKER", "关闭连接（模拟网络断线）")
        self.sock.close()


class Harness:
    def __init__(self, tr, clean_session, manual_ack=True, on_message=None,
                 suppress_exceptions=None):
        holder[0] = self  # 在 loop_start 前注册, 避免重连路由竞态
        self.tr = tr
        self.broker_socks = queue.Queue()
        self.client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id="sec-events-01",
            clean_session=clean_session,
            protocol=mqtt.MQTTv311,
            manual_ack=manual_ack,
        )
        if on_message is not None:
            self.client.on_message = on_message
        if suppress_exceptions is not None:
            self.client.suppress_exceptions = suppress_exceptions
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.reconnect_delay_set(min_delay=1, max_delay=1)
        self.client.connect("fakebroker", 1883)
        self.client.loop_start()

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        sp = getattr(flags, "session_present", flags)
        self.tr.log("APP", f"on_connect session_present={sp} rc={reason_code}")

    def _on_disconnect(self, client, userdata, flags, reason_code, properties=None):
        self.tr.log("APP", f"on_disconnect rc={reason_code}")

    def broker(self, timeout=15):
        return Broker(self.broker_socks.get(timeout=timeout), self.tr)

    def stop(self):
        try:
            self.client.loop_stop()
        except Exception:
            pass


def handshake(h, b, session_present):
    body = b.expect("CONNECT")
    clean_flag = (body[7] & 0x02) >> 1 if len(body) > 7 else "?"
    h.tr.log(" ...", f"(CONNECT clean_session={clean_flag})")
    b.send("CONNACK(flags=%d)" % (1 if session_present else 0),
           paho_test.gen_connack(flags=1 if session_present else 0, rc=0))
    time.sleep(0.2)


def subscribe(h, b, topic="sec/events", qos=2):
    h.client.subscribe(topic, qos)
    body = b.expect("SUBSCRIBE")
    mid = struct.unpack("!H", body[:2])[0]
    b.send("SUBACK", paho_test.gen_suback(mid, qos))
    time.sleep(0.2)


def make_recorder(tr, received, event, hook=None):
    def on_message(client, userdata, msg):
        received.append(msg.mid)
        tr.log("APP", f"on_message 第{len(received)}次被调用 mid={msg.mid} payload={msg.payload!r}")
        event.set()
        if hook:
            hook(client, msg)
    return on_message


# ---------------------------------------------------------------- E1
def e1():
    tr = Transcript("E1 基线: QoS2 + manual_ack=True, PUBCOMP 只在应用 ack() 后发送")
    received, event = [], threading.Event()
    h = Harness(tr, clean_session=True, manual_ack=True,
                on_message=make_recorder(tr, received, event))
    b = h.broker()
    handshake(h, b, session_present=False)
    subscribe(h, b)
    b.send("PUBLISH(mid=10,qos2)", paho_test.gen_publish("sec/events", 2, payload=b'{"alert":1}', mid=10))
    body = b.expect("PUBREC")
    assert body == struct.pack("!H", 10)
    tr.log(" ...", "(PUBREC 在 on_message 之前立即发出)")
    b.send("PUBREL(mid=10)", paho_test.gen_pubrel(10))
    assert event.wait(5), "callback not called"
    b.assert_silence(1.0, "回调已返回, 但未 ack => 无 PUBCOMP")
    time.sleep(0.8)
    tr.log("APP", "应用完成入库, 调用 client.ack(mid=10, qos=2)")
    h.client.ack(10, 2)
    assert b.expect("PUBCOMP") == struct.pack("!H", 10)
    assert received == [10], received
    tr.log("OK", f"on_message 共 {len(received)} 次; PUBCOMP 仅在 ack() 后发出")
    h.stop()
    return tr


# ---------------------------------------------------------------- E2
def e2():
    tr = Transcript("E2 重复 PUBLISH (DUP=1, 同 mid): 重发 PUBREC, 不重复回调")
    received, event = [], threading.Event()
    h = Harness(tr, clean_session=True, manual_ack=True,
                on_message=make_recorder(tr, received, event))
    b = h.broker()
    handshake(h, b, session_present=False)
    subscribe(h, b)
    pkt = paho_test.gen_publish("sec/events", 2, payload=b'{"alert":2}', mid=11)
    b.send("PUBLISH(mid=11,qos2,dup=0)", pkt)
    b.expect("PUBREC")
    b.send("PUBLISH(mid=11,qos2,DUP=1) 重传",
           paho_test.gen_publish("sec/events", 2, payload=b'{"alert":2}', mid=11, dup=True))
    assert b.expect("PUBREC") == struct.pack("!H", 11)
    assert not event.wait(0.5), "duplicate PUBLISH must not trigger callback"
    tr.log(" ...", "(重复 PUBLISH: 再次 PUBREC, on_message 未触发)")
    b.send("PUBREL(mid=11)", paho_test.gen_pubrel(11))
    assert event.wait(5)
    h.client.ack(11, 2)
    b.expect("PUBCOMP")
    assert received == [11], received
    tr.log("OK", f"on_message 共 {len(received)} 次")
    h.stop()
    return tr


# ---------------------------------------------------------------- E3
def e3():
    tr = Transcript("E3 重复 PUBREL: 不重复回调; manual_ack 下重传 PUBREL 得不到任何响应")
    received, event = [], threading.Event()
    h = Harness(tr, clean_session=True, manual_ack=True,
                on_message=make_recorder(tr, received, event))
    b = h.broker()
    handshake(h, b, session_present=False)
    subscribe(h, b)
    b.send("PUBLISH(mid=12,qos2)", paho_test.gen_publish("sec/events", 2, payload=b'{"alert":3}', mid=12))
    b.expect("PUBREC")
    b.send("PUBREL(mid=12)", paho_test.gen_pubrel(12))
    assert event.wait(5)
    b.send("PUBREL(mid=12) 重传#1", paho_test.gen_pubrel(12, dup=True))
    b.assert_silence(1.0, "manual_ack: 重复 PUBREL 无回调也无 PUBCOMP")
    assert received == [12]
    tr.log("APP", "应用延迟后调用 ack(12, 2)")
    h.client.ack(12, 2)
    b.expect("PUBCOMP")
    b.send("PUBREL(mid=12) 重传#2 (ack 之后)", paho_test.gen_pubrel(12, dup=True))
    b.assert_silence(1.0, "ack 之后重复 PUBREL 依然静默")
    assert received == [12], received
    tr.log("OK", f"on_message 共 {len(received)} 次")
    h.stop()
    return tr


# ---------------------------------------------------------------- E4
def e4():
    tr = Transcript("E4 断线(PUBREL 之前) + 重连 Session Present=1: 回调仍只一次")
    received, event = [], threading.Event()
    h = Harness(tr, clean_session=False, manual_ack=True,
                on_message=make_recorder(tr, received, event))
    b = h.broker()
    handshake(h, b, session_present=False)
    subscribe(h, b)
    b.send("PUBLISH(mid=20,qos2)", paho_test.gen_publish("sec/events", 2, payload=b'{"alert":4}', mid=20))
    b.expect("PUBREC")
    b.drop()
    b = h.broker()  # 客户端自动重连 -> 新 socketpair
    handshake(h, b, session_present=True)
    b.send("PUBLISH(mid=20,qos2,DUP=1) broker 按会话状态重传",
           paho_test.gen_publish("sec/events", 2, payload=b'{"alert":4}', mid=20, dup=True))
    assert b.expect("PUBREC") == struct.pack("!H", 20)
    b.send("PUBREL(mid=20)", paho_test.gen_pubrel(20))
    assert event.wait(5)
    h.client.ack(20, 2)
    b.expect("PUBCOMP")
    assert received == [20], received
    tr.log("OK", f"on_message 共 {len(received)} 次 (_in_messages 跨重连保留)")
    h.stop()
    return tr


# ---------------------------------------------------------------- E5
def e5():
    tr = Transcript("E5 断线(回调后、ack 前) + 重连 Session Present=1: 不重投, 需应用补 ack")
    received, event = [], threading.Event()
    h = Harness(tr, clean_session=False, manual_ack=True,
                on_message=make_recorder(tr, received, event))
    b = h.broker()
    handshake(h, b, session_present=False)
    subscribe(h, b)
    b.send("PUBLISH(mid=30,qos2)", paho_test.gen_publish("sec/events", 2, payload=b'{"alert":5}', mid=30))
    b.expect("PUBREC")
    b.send("PUBREL(mid=30)", paho_test.gen_pubrel(30))
    assert event.wait(5)
    tr.log("APP", "回调已执行, 但应用在 ack() 之前断线 (模拟崩溃窗口)")
    b.drop()
    b = h.broker()
    handshake(h, b, session_present=True)
    b.send("PUBREL(mid=30) broker 重传", paho_test.gen_pubrel(30, dup=True))
    b.assert_silence(1.0, "消息已交付过: 不再回调, manual_ack 下也不回 PUBCOMP")
    assert received == [30]
    tr.log("APP", "应用用持久化的 mid 补 ack(30, 2)")
    h.client.ack(30, 2)
    assert b.expect("PUBCOMP") == struct.pack("!H", 30)
    assert received == [30], received
    tr.log("OK", f"on_message 共 {len(received)} 次; 若应用丢失 mid, broker 将无限重传 PUBREL")
    h.stop()
    return tr


# ---------------------------------------------------------------- E6
def e6():
    tr = Transcript("E6 断线(回调后、ack 前) + 重连 Session Present=0: 回调第二次执行 (应用层重复!)")
    received, event = [], threading.Event()
    h = Harness(tr, clean_session=False, manual_ack=True,
                on_message=make_recorder(tr, received, event))
    b = h.broker()
    handshake(h, b, session_present=False)
    subscribe(h, b)
    b.send("PUBLISH(mid=40,qos2)", paho_test.gen_publish("sec/events", 2, payload=b'{"alert":6}', mid=40))
    b.expect("PUBREC")
    b.send("PUBREL(mid=40)", paho_test.gen_pubrel(40))
    assert event.wait(5)
    event.clear()
    tr.log("APP", "回调已执行 (假设已入库), 未 ack 即断线")
    b.drop()
    b = h.broker()
    handshake(h, b, session_present=False)  # broker 侧会话丢失 (重启/过期)
    tr.log(" ...", "(Session Present=0: broker 没有会话状态, 从头重投 PUBLISH)")
    b.send("PUBLISH(mid=40,qos2,DUP=1)",
           paho_test.gen_publish("sec/events", 2, payload=b'{"alert":6}', mid=40, dup=True))
    b.expect("PUBREC")
    b.send("PUBREL(mid=40)", paho_test.gen_pubrel(40))
    assert event.wait(5), "second delivery expected"
    h.client.ack(40, 2)
    b.expect("PUBCOMP")
    assert received == [40, 40], received
    tr.log("OK", f"on_message 共 {len(received)} 次, mid 相同 => 应用必须自行幂等去重")
    h.stop()
    return tr


# ---------------------------------------------------------------- E7
def e7():
    tr = Transcript("E7 回调抛异常: (a) suppress_exceptions=True 循环存活但不重投; (b) 默认 False 网络线程死亡")
    # (a) suppress_exceptions=True
    received, event = [], threading.Event()

    def raising_hook(client, msg):
        raise RuntimeError("db write failed (simulated)")

    h = Harness(tr, clean_session=True, manual_ack=True, suppress_exceptions=True,
                on_message=make_recorder(tr, received, event, hook=raising_hook))
    b = h.broker()
    handshake(h, b, session_present=False)
    subscribe(h, b)
    b.send("PUBLISH(mid=50,qos2)", paho_test.gen_publish("sec/events", 2, payload=b'{"alert":7}', mid=50))
    b.expect("PUBREC")
    b.send("PUBREL(mid=50)", paho_test.gen_pubrel(50))
    assert event.wait(5)
    tr.log("APP", "on_message 抛出 RuntimeError (suppress_exceptions=True, 已记录日志并吞掉)")
    b.send("PUBREL(mid=50) 重传", paho_test.gen_pubrel(50, dup=True))
    b.assert_silence(1.0, "消息在回调前已出队: 不重投, 也不回 PUBCOMP")
    assert received == [50]
    alive = h.client._thread is not None and h.client._thread.is_alive()
    tr.log(" ...", f"网络线程存活={alive}")
    assert alive
    h.client.ack(50, 2)
    b.expect("PUBCOMP")
    h.stop()

    # (b) suppress_exceptions=False (默认)
    received2, event2 = [], threading.Event()
    h2 = Harness(tr, clean_session=True, manual_ack=True,
                 on_message=make_recorder(tr, received2, event2, hook=raising_hook))
    b2 = h2.broker()
    handshake(h2, b2, session_present=False)
    subscribe(h2, b2)
    b2.send("PUBLISH(mid=51,qos2)", paho_test.gen_publish("sec/events", 2, payload=b'{"alert":7b}', mid=51))
    b2.expect("PUBREC")
    b2.send("PUBREL(mid=51)", paho_test.gen_pubrel(51))
    assert event2.wait(5)
    tr.log("APP", "on_message 抛出 RuntimeError (suppress_exceptions=False, 默认)")
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        t = h2.client._thread
        if t is None or not t.is_alive():
            break
        time.sleep(0.1)
    t = h2.client._thread
    dead = t is None or not t.is_alive()
    tr.log(" ...", f"异常传播 => 网络线程已退出={dead} (loop_start 模式下客户端停止一切处理)")
    assert dead
    b2.send("PUBREL(mid=51) 重传", paho_test.gen_pubrel(51, dup=True))
    b2.assert_silence(1.0, "线程已死, 无任何响应")
    assert received2 == [51]
    h2.stop()
    tr.log("OK", "回调异常: 消息不重投; 默认配置下整个网络线程终止")
    return tr


# ---------------------------------------------------------------- E8
def e8():
    tr = Transcript("E8 对照组 manual_ack=False: PUBCOMP 自动发送, 重复 PUBREL 自动重答")
    received, event = [], threading.Event()
    h = Harness(tr, clean_session=True, manual_ack=False,
                on_message=make_recorder(tr, received, event))
    b = h.broker()
    handshake(h, b, session_present=False)
    subscribe(h, b)
    b.send("PUBLISH(mid=60,qos2)", paho_test.gen_publish("sec/events", 2, payload=b'{"alert":8}', mid=60))
    b.expect("PUBREC")
    b.send("PUBREL(mid=60)", paho_test.gen_pubrel(60))
    assert event.wait(5)
    assert b.expect("PUBCOMP") == struct.pack("!H", 60)
    tr.log(" ...", "(回调返回后立即自动 PUBCOMP)")
    b.send("PUBREL(mid=60) 重传", paho_test.gen_pubrel(60, dup=True))
    assert b.expect("PUBCOMP") == struct.pack("!H", 60)
    tr.log(" ...", "(重复 PUBREL 自动重答 PUBCOMP, 不重复回调)")
    assert received == [60], received
    tr.log("OK", f"on_message 共 {len(received)} 次")
    h.stop()
    return tr


def main():
    def create_connection(addr, timeout=None, source_address=None):
        client_end, broker_end = socket.socketpair()
        client_end.settimeout(timeout if timeout else 10)
        holder[0].broker_socks.put(broker_end)
        return client_end

    scenarios = [e1, e2, e3, e4, e5, e6, e7, e8]
    transcripts = []
    # 沙箱禁止绑定本地端口, 用 AF_UNIX socketpair 替换 paho 的 TCP loopback 唤醒对
    # (仅替换线程唤醒机制, 不影响被测的 QoS 协议逻辑)
    with unittest.mock.patch.object(client_mod.socket, "create_connection", create_connection), \
         unittest.mock.patch.object(client_mod, "_socketpair_compat", socket.socketpair):
        for fn in scenarios:
            transcripts.append(fn())

    out = Path(__file__).with_name("transcript.txt")
    text = "\n\n".join(t.render() for t in transcripts)
    out.write_text(text + "\n", encoding="utf-8")
    print(text)
    print(f"\n全部 {len(scenarios)} 个场景通过, 报文序列已写入 {out}")


if __name__ == "__main__":
    main()
