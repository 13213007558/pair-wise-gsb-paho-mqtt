# QoS 2 + manual_ack=True 行为验证报告

**目的**：安全事件入库链路使用 MQTT QoS 2 + `manual_ack=True`，验证 paho-mqtt 在重复
PUBLISH、重复 PUBREL、回调完成前断线、回调抛异常、应用延迟 ack 时的实际行为：
消息交给应用几次、PUBCOMP 什么时候发。

**被测对象**：本仓库源码（paho-mqtt 2.2.1.dev0），MQTT v3.1.1，`CallbackAPIVersion.VERSION2`。

**方法**：不连接真实 broker。用 `socket.socketpair()` 替换 `socket.create_connection`，
由测试脚本直接构造/解析协议报文（CONNECT/CONNACK/PUBLISH/PUBREC/PUBREL/PUBCOMP），
记录双向完整报文序列。沙箱禁止绑定本地端口，因此 paho 内部用于唤醒网络线程的
TCP loopback 对（`client.py:_socketpair_compat`）被替换为 AF_UNIX socketpair——
这只是线程唤醒机制，不涉及被测的 QoS 状态机。

**产物**：
- `qos2-manual-ack/run_experiments.py` — 8 个场景（E1–E8），全部断言通过
- `qos2-manual-ack/transcript.txt` — 每场景完整报文级时序（含时间戳）

**关键实现位置**（`src/paho/mqtt/client.py`）：
- 收到 QoS2 PUBLISH 立即发 PUBREC 并存入 `_in_messages`（:4145-4153），此时**不**回调
- 收到 PUBREL 时从 `_in_messages` **弹出**后才回调（:4190-4195）——弹出去重
- `manual_ack=True` 时 `_handle_pubrel` 不发 PUB 任何报文（:4208-4209）；
  PUBCOMP 只能由应用调 `ack(mid, 2)` 发出（:4157-4168）
- 重连时 `_in_messages` 仅当 clean session 才清空（:3747-3758），且**全部在内存**
- 回调异常：记录日志后按 `suppress_exceptions`（默认 False）重新抛出（:4453-4461）

---

## 一、实测结果摘要

| 场景 | 结果 |
|---|---|
| E1 基线 + 延迟 ack | PUBREC 在回调**之前**立即发出；回调返回后客户端**静默**；PUBCOMP 只在应用 `ack()` 时发出（延迟 ~1.8s 后正常发出） |
| E2 重复 PUBLISH (DUP=1, 同 mid) | 再次回 PUBREC；`on_message` **不**触发；后续 PUBREL 正常交付 1 次 |
| E3 重复 PUBREL | 第 2、3 次 PUBREL **无回调、也无任何响应报文**（manual_ack 下不会自动重答 PUBCOMP）；`on_message` 共 1 次 |
| E4 断线(PUBREL 前) + 重连 Session Present=1 | `_in_messages` 跨重连保留；broker 重发 PUBLISH(DUP=1) → 再回 PUBREC → PUBREL → 回调仍只 1 次 |
| E5 断线(回调后、ack 前) + 重连 Session Present=1 | broker 重传 PUBREL → **不再回调、也不回 PUBCOMP（完全静默）**；应用用持久化的 mid 补 `ack()` 后才发出 PUBCOMP |
| E6 断线(回调后、ack 前) + 重连 Session Present=0 | broker 从头重投 PUBLISH(DUP=1) → 完整四步握手 → **`on_message` 第 2 次执行，mid 相同** → 应用层重复投递 |
| E7a 回调抛异常 (suppress_exceptions=True) | 异常被记日志吞掉，网络线程存活；消息在回调前已出队 → broker 重传 PUBREL 无回调无响应；仍可手动 `ack()` 收尾 |
| E7b 回调抛异常 (默认 suppress_exceptions=False) | 异常传播，**`loop_start` 的网络线程整个终止**，客户端停止一切收发，对后续报文零响应 |
| E8 对照 manual_ack=False | 回调返回后**立即自动**发 PUBCOMP；重复 PUBREL 会**自动重答** PUBCOMP（自愈），回调仍只 1 次 |

### 典型时序（E5，Session Present=1，断线发生在回调后、ack 前）

```
C->B CONNECT (clean_session=0)          B->C CONNACK(flags=0)
C->B SUBSCRIBE                          B->C SUBACK
B->C PUBLISH(mid=30,qos2)
C->B PUBREC(30)                         ← 回调之前就发出
B->C PUBREL(30)
APP  on_message 第1次 (mid=30)           ← 消息在此刻从 _in_messages 弹出
APP  未 ack 即断线
C->B CONNECT (clean_session=0)          B->C CONNACK(flags=1)  ← Session Present=1
B->C PUBREL(30) 重传
...  静默: 不回调、不回 PUBCOMP           ← manual_ack 下 paho 对未知 mid 的 PUBREL 零响应
APP  ack(30, 2)（应用自己记住的 mid）
C->B PUBCOMP(30)
```

### 典型时序（E6，Session Present=0，broker 侧会话丢失）

```
... 同上直到断线 ...
C->B CONNECT (clean_session=0)          B->C CONNACK(flags=0)  ← 会话不存在
B->C PUBLISH(mid=40,qos2,DUP=1)          ← broker 从头重投
C->B PUBREC(40)
B->C PUBREL(40)
APP  on_message 第2次 (mid=40)           ← 同一事件第二次交给应用
APP  ack(40, 2)
C->B PUBCOMP(40)
```

---

## 二、协议保证了什么（进程存活、会话未丢失的前提下）

1. **应用级去重只在"已收到 PUBREL"之后生效**：同一 mid 的重复 PUBLISH 只换来重复
   PUBREC，不进回调；重复 PUBREL 因消息已弹出 `_in_messages` 而不重复回调。
   在**单个进程生命周期内、且会话状态连续**时，`on_message` 对每个 QoS2 消息恰好触发一次。
2. **PUBREC 与业务处理无关**：收到 PUBLISH 立即回 PUBREC，不代表已入库。
3. **PUBCOMP 的时机完全由应用控制**（manual_ack=True）：只有 `ack(mid, 2)` 会发
   PUBCOMP；在此之前客户端对 broker 的重传**不做任何响应**（E1/E3/E5 实测静默）。
   这与 manual_ack=False 形成鲜明对比：后者回调返回即发 PUBCOMP，且对重复 PUBREL
   自动重答（E8）。**manual_ack 在获得"先入库后确认"能力的同时，放弃了自动自愈**。
4. **持久会话（clean_session=False）+ Session Present=1 的重连**：内存中的
   `_in_messages` 保留，未完成的 QoS2 消息可跨重连续传，回调仍只一次（E4）。

## 三、进程退出后应用仍需保证什么（协议管不到的部分）

1. **QoS 2 ≠ 数据库操作只执行一次。** QoS 2 的 "exactly once" 只覆盖 MQTT 报文传输
   层面；消息在 PUBREL 到达时就被交给应用并弹出队列，"已交付"与"已入库"之间没有
   任何协议级绑定。
2. **"已入库、未 ack"窗口必然产生重复**（E6）：应用在回调里完成入库、还没调用
   `ack()` 时进程崩溃/断线，且 broker 侧会话丢失（Session Present=0，如 broker
   重启、会话过期、clean session），同一消息会被完整重投，`on_message` 再次执行。
   **安全事件表必须有幂等去重**：以事件唯一 ID（不是 MQTT mid——mid 只有 16 位且
   会被 broker 复用）建唯一约束或去重表，重复插入应识别为已处理并照常 `ack()`。
3. **manual_ack 下的卡死风险**（E5/E7a）：消息一旦交付即出队，若应用崩溃前没记住
   mid，或回调异常后无人补 `ack()`，broker 会无限重传 PUBREL 而 paho 永远静默——
   消息既不重投也不完成，占用 broker 会话状态。应用需要：
   - 入库与"待 ack 的 mid"一起持久化，重启后补 `ack()`；或
   - 接受重启后按重复消息重投（配合第 2 点的幂等）来自然推进握手。
4. **回调异常是致命路径**（E7b）：默认 `suppress_exceptions=False` 下，回调里任何
   未捕获异常会**杀死整个网络线程**，客户端静默停摆（不再收发任何报文）。回调内
   必须自行捕获所有异常；入库失败时明确选择"重试后 ack"或"记录死信后 ack"，
   不能让异常漏出回调。
5. **`_in_messages` 纯内存**：进程退出即丢失。重启后是否重复取决于 clean_session
   与 broker 会话状态——这正是第 2 点幂等必须覆盖的场景。

## 四、对安全事件入库链路的建议

1. 回调内只做：解析 → 以事件唯一 ID 幂等写入（唯一约束/UPSERT）→ `ack(mid, 2)`；
   全部包在 try/except 里，异常不外抛。
2. 不要用 MQTT mid 做去重键；用事件本身的业务 ID。
3. 监控 broker 侧长时间未完成的 QoS2 入站消息（PUBREL 重传无响应即 manual_ack
   卡死的信号）。
4. 若无法接受"已入库未 ack"导致的重复投递窗口，需要把去重做在数据库层
   （这是唯一可靠的兜底），而不是指望 MQTT 层。
