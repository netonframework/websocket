# websocket — 规格说明（SPEC）

> Kotlin/Native 的 WebSocket（RFC 6455）协议库，建在 `com.netonstream:io` 之上。
> 坐标 `com.netonstream:websocket`，包 `neton.websocket`。仓库 `websocket`。
> 状态：草案 v1（2026-09-27，按 GPT 评审修订：写侧背压不阻塞读侧、帧的接受与所有权、零拷贝与缓冲池的关系；待评审）。

## 0. 依据与范围

- **建设方法**：见 neton-io SPEC §28.14。首版以参考实现为准，做到能力对等；API 与并发模型按 neton.io 与 Kotlin 协程落地，不逐字翻译 Rust。
- **参考实现**（只读学习副本，固定版本）：
  - `~/projects/reference/rust/tungstenite-0.30.0`：协议核心。
  - `~/projects/reference/rust/tokio-tungstenite-0.30.0`：异步接线。
  - 版本与提交号见 `~/projects/reference/rust/README.md`。
- **能力盘点**：来自对上述源码的逐项阅读（2026-09-27）。下文 `T/…` 指 tungstenite 源码根，`TT/…` 指 tokio-tungstenite 源码根。
- **首版范围**：参考实现的全部能力，每项标注为以下之一：
  - ✅ 对等：行为与参考实现相同。
  - ⚖️ 有意不同：附理由；默认取更安全或更符合 RFC 的一方。
  - ⛔ 不适用：附理由。
- **不在本库**：
  - TLS：TLS 以 `IoStream` 包装的形式由独立模块提供，本库只接受任意 `IoStream`。
  - HTTP / SOCKS 代理：参考实现同样没有。
  - permessage-deflate（RFC 7692）：参考 0.30 未实现，Autobahn 12.x / 13.x 的 216 个用例标记为 UNIMPLEMENTED；作为后续版本另起章节。

## 1. 依赖与分层

```
neton.websocket（本库）
   ├── 协议核心：帧编解码、掩码、消息重组、UTF-8 校验、关闭状态机、握手报文的生成与校验（不做 I/O、不读系统时间）
   └── 协程驱动：在 IoStream 上运行协议核心；client / server 入口；连接与握手
        ↓
com.netonstream:http（通用 HTTP 类型与 HTTP/1.1 头部解析；握手、以及从 HTTP 服务端 Upgrade 接管连接）
        ↓
com.netonstream:io（IoStream、Buffer / Bytes、connect / listen、反应器）
```

- **协议核心不依赖 I/O**：输入字节，输出帧、消息、要发送的字节与事件，可用确定性测试覆盖，对应参考实现中 `WebSocketContext` 以流为参数的设计（`T/src/protocol/mod.rs:362-782`）。
- **握手用 `http` 库**：参考实现用 `httparse` + `http` crate（`T/src/handshake/headers.rs`）。本库使用 `com.netonstream:http` 的通用类型（`neton.http` 的 Request / Response / HeaderMap），以及它的 HTTP/1.1 头部解析；因此本库在 `http` 首版之后实现。
- **从 HTTP 服务端接管连接**：参考示例 `TT/examples/server-custom-accept.rs` 先在 hyper 中完成 Upgrade，再用 `WebSocketStream::from_raw_socket`；本库提供同样的"已握手流 → WebSocket"入口，配合 `http` 库的 Upgrade 能力。

## 2. 类型（对等清单）

| 参考 | 本库 | 标注 |
|---|---|---|
| `WebSocket<Stream>`（`T/src/protocol/mod.rs:162-358`） | `WebSocket`（持有 `IoStream`） | ✅ 能力对等；API 见 §6 |
| `WebSocketContext`（流作为参数） | 协议核心 `WebSocketCore`（不做 I/O） | ✅ |
| `Role{Server, Client}` | `Role` | ✅ |
| `Message{Text(Utf8Bytes), Binary(Bytes), Ping, Pong, Close(Option<CloseFrame>), Frame(Frame)}`（`message.rs:157`） | `sealed interface Message`，同样六种 | ✅ `Frame` 只用于发送，`read` 从不产生 |
| `Frame{header, payload}`、`FrameHeader{is_final, rsv1..3, opcode, mask}`，`MAX_SIZE = 14` | 同名 | ✅ |
| `OpCode`：Data{Continue=0, Text=1, Binary=2, Reserved(3..7)}，Control{Close=8, Ping=9, Pong=10, Reserved(11..15)} | 同名 | ✅ |
| `CloseCode`：1000–1013、1015，以及 Bad / Reserved / Iana / Library 区间；`is_allowed()` 对 Bad、Reserved、1005、1006、1015 返回 false（`coding.rs:119-259`） | 同名 | ✅ 区间与判定逐一对等 |
| `CloseFrame{code, reason: Utf8Bytes}` | 同名 | ✅ |
| `Utf8Bytes`（保证 UTF-8 的 `Bytes`，零拷贝校验） | `Utf8Bytes`（包装 neton-io `Bytes`） | ✅ |
| `FrameSocket`（帧级读写，128 KiB 读缓冲，总是去掩码、接受未掩码帧） | 同名 | ✅ |
| `MidHandshake` / `HandshakeError{Interrupted, Failure}`（可中断、可恢复的握手） | 挂起函数完成握手；无 `Interrupted` | ⚖️ 协程挂起取代"WouldBlock 后重入"，恢复能力由挂起本身提供 |
| `MaybeTlsStream` / `Connector` / `client_tls*` | 无 | ⛔ TLS 由 `IoStream` 包装提供 |

## 3. 握手

### 3.1 通用
- **防攻击上限**（`AttackCheck`，`T/src/handshake/machine.rs:148-190`，写死）：
  - 头部总字节 ≤ 65536。
  - 读取次数 ≤ 512。
  - 读满 64 次后，平均每次读取 ≥ 128 字节。
  - 违反任一条 → `AttackAttempt`。
  - 本库 ✅ 对等，改为可配置、默认值相同；读取次数与平均字节数的判定照搬。
- **头部个数上限**：124（`MAX_HEADERS`，超过 → `TooManyHeaders`）✅。
- **HTTP 版本**：解析后必须是 HTTP/1.1，低于 1.1 → `WrongHttpVersion` ✅。
- **性能**：参考实现每轮重新解析整个缓冲（`machine.rs:50` 的 TODO）。本库增量解析 ⚖️，行为不变。
- **Accept 键**：SHA-1(key + `258EAFA5-E914-47DA-95CA-C5AB0DC85B11`) 后 base64。本库自带 SHA-1 与 base64 实现，并用 RFC 向量测试 ✅。

### 3.2 客户端（`T/src/client.rs`、`T/src/handshake/client.rs`）
- **`IntoClientRequest`**：字符串 / URI / 已构造的请求 / 构建器 ✅。
  - URI 必须有 authority；去掉 `user:pass@`。
  - 无主机 → `NoHostName`；空主机 → `EmptyHostName`。
  - 方案只能是 `ws` / `wss`，否则 → `UnsupportedUrlScheme`。
- **`Sec-WebSocket-Key`**：16 字节随机数经 base64 编码。随机源用平台 CSPRNG（`arc4random_buf` / `getrandom` / `BCryptGenRandom`）⚖️。参考实现用 `rand::random`；RFC 要求随机值不可预测。
- **请求报文**（`generate_request`，`client.rs:112-199`）✅：
  - 请求行 `GET {path_and_query} HTTP/1.1`，缺路径 → `NoPathOrQuery`。
  - 五个必需头部按固定顺序、规范大小写写出：`Host`、`Connection: Upgrade`、`Upgrade: websocket`、`Sec-WebSocket-Version: 13`、`Sec-WebSocket-Key`；缺任一 → `InvalidHeader`。
  - 其余头部原样写出，允许非 ASCII / Latin-1 值；与必需头部重复 → `InvalidHeader`。
  - `sec-websocket-protocol` 改写为 `Sec-WebSocket-Protocol`，`origin` 改写为 `Origin`。
- **子协议**：`ClientRequestBuilder.withSubProtocol`，多个值以 `", "` 连接 ✅。
- **响应校验**（`verify_response`，`client.rs:220-295`）：
  - 状态码非 101 → `Http(response)`，响应体是头部之后已读到的字节 ✅。
  - `Upgrade` 头与 `websocket` 做整值、不区分大小写比较 ✅。
  - `Connection` 头：参考实现按整值比较，本库按 RFC 7230 逗号分隔的 token 列表判断含 `Upgrade` ⚖️。理由：`Connection: keep-alive, Upgrade` 是合法响应；服务端一侧参考实现本身就按 token 解析。
  - `Sec-WebSocket-Accept` 必须精确匹配 ✅。
  - `Sec-WebSocket-Extensions`：参考实现未校验（TODO）。本库在未协商任何扩展时，服务端返回扩展即报错 ⚖️（RFC 6455 §4.1 要求）。
  - 子协议三种错误 ✅：
    - 未请求却返回 → `ServerSentSubProtocolNoneRequested`。
    - 请求了却未返回 → `NoSubProtocol`。
    - 返回的不在请求列表中 → `InvalidSubProtocol`。
- **尾随字节**：响应头部之后已读到的字节交给 `WebSocket`，对应 `from_partially_read` ✅。
- **`connect`**：DNS 解析与逐地址尝试 ✅，全部失败 → `UnableToConnect`；IPv6 方括号去除 ✅；默认端口 80 / 443 ✅。
  - **重定向**：同步 `connect` 跟随 3xx（最多 3 次），异步 `connect_async` 不跟随。本库提供 `maxRedirects` 参数，默认 0，与异步参考实现一致 ⚖️。
  - **`TCP_NODELAY`**：同步版置位，异步版默认不置位。本库沿用 neton-io 的 `SocketOptions` 默认值 `noDelay = true` ⚖️。
  - **`wss`**：需要调用方提供 TLS 包装的连接工厂；未提供时 → `TlsNotAvailable`，对应参考的 `TlsFeatureNotEnabled` ✅。

### 3.3 服务端（`T/src/handshake/server.rs`、`T/src/server.rs`）
- **校验顺序**（`create_parts`，`server.rs:37-87`）✅：
  1. 方法 GET，否则 `WrongHttpMethod`。
  2. 版本 ≥ 1.1，否则 `WrongHttpVersion`。
  3. `Connection` 按空格与逗号拆分后，不区分大小写地含 `Upgrade`，否则 `MissingConnectionUpgradeHeader`。
  4. `Upgrade` 为 `websocket`，否则 `MissingUpgradeWebSocketHeader`。
  5. `Sec-WebSocket-Version` 为 `13`，否则 `MissingSecWebSocketVersionHeader`。
  6. `Sec-WebSocket-Key` 存在，否则 `MissingSecWebSocketKey`。
  7. key 长 24 且 base64 解码为 16 字节，否则 `InvalidSecWebSocketKey`。
- `Host` 与 `Origin` 不检查（参考同样）✅；需要时由回调检查。
- **成功响应**：`101`，带 `Connection: Upgrade`、`Upgrade: websocket`、`Sec-WebSocket-Accept` ✅。
- **回调**：`onRequest(request, response) -> response | errorResponse` ✅。
  - 子协议选择与附加头部由回调完成；参考实现不做自动协商，本库同样。
  - 回调返回 2xx 错误响应 → `CustomResponseSuccessful`。
  - 返回其他错误响应 → 写出（含可选字符串体）并 flush，然后 → `Http(response)`。
- **请求头之后若已有字节** → `JunkAfterRequest` ✅。
- **入口**：`accept` / `acceptWithConfig` / `acceptHdr` / `acceptHdrWithConfig` ✅。另有"由 HTTP 服务端 Upgrade 后接管"的入口（§1）。

## 4. 帧与消息

### 4.1 解析（`T/src/protocol/frame/frame.rs:138-203`、`frame/mod.rs:161-231`）
- **头部**：FIN / RSV1–3 / opcode / MASK / 7 位长度（126 → 16 位，127 → 64 位，大端）✅；数据不全时不消费 ✅；保留 opcode 在头部解析时 → `InvalidOpcode` ✅。
- **64 位长度的最高位与最短编码**：参考不校验。本库对最高位为 1 → `ProtocolError`（RFC 6455 §5.2 要求）⚖️。非最短编码与参考一致、不拒绝 ✅（RFC 未要求拒绝）。
- **长度检查在读负载之前**：与 `maxFrameSize` 比较（在 64 位上比较，32 位平台安全），超出 → `MessageTooLong{size, maxSize}` ✅；随后一次性预留整个负载 ✅。
- **单次读取上限**：`inBufMaxRead = max(readBufferSize, 14)` ✅。
- **读到 EOF** → 由状态机决定结果（§5）✅。
- **掩码规则**：
  - 服务端收到带掩码的帧原地去掩码；收到未掩码帧 → `UnmaskedFrameFromClient`，除非 `acceptUnmaskedFrames`。✅
  - 客户端收到带掩码的帧 → `MaskedFrameFromServer` ✅。
- **校验顺序**（`read_message_frame`，`protocol/mod.rs:610-714`）逐项对等 ✅：
  1. EOF。
  2. `!canRead` → `ReceivedAfterClosing`。
  3. RSV 位 → `NonZeroReservedBits`。
  4. 客户端收到带掩码帧。
  5. 控制帧：FIN = 0 → `FragmentedControlFrame`；负载 > 125 → `ControlFrameTooBig`。
     - 参考在读完负载后才检查，且只受 `maxFrameSize` 约束。本库在头部解析后即检查 ⚖️，避免为超长控制帧读入负载；错误类型相同。
  6. 数据帧：无进行中消息时收到 Continue → `UnexpectedContinueFrame`；有进行中消息时收到 Text / Binary → `ExpectedFragment`。
- **关闭帧负载**：长 0 → 无关闭帧；长 1 → `InvalidCloseSequence`；否则前 2 字节为码，其余为原因，原因非 UTF-8 → `Utf8` 错误 ✅。

### 4.2 消息重组与 UTF-8（`message.rs`、`T/src/utf8.rs`）
- **大小检查**：`IncompleteMessage.extend` 做溢出安全的 `maxMessageSize` 检查 ✅；单帧消息同样检查 ✅。
- **单帧文本**：原地校验、零拷贝得到 `Utf8Bytes` ✅。
- **分片文本**：逐帧增量校验，跨帧的不完整码点暂存最多 4 字节；结束时仍有残留 → 错误 ✅。
  - **快速失败的粒度**：参考在整帧到达后才校验，Autobahn 6.4.3 / 6.4.4 记为 NON-STRICT。本库校验粒度与参考相同 ✅，目标结果与参考一致；逐字节快速失败作为后续优化，届时这两项可升为 OK。
- **分片二进制**：拼接到一块缓冲 ✅。复制的次数作为性能项测量。
- **UTF-8 错误的类型**：`Utf8`，不是 `ProtocolError`，与参考一致 ✅。

### 4.3 写（`frame/mod.rs:250-291`、`frame.rs:362-373`、`mask.rs`）
- **写缓冲**：帧编码进同一输出缓冲，掩码在输出缓冲内原地施加（一次拷贝，无临时缓冲）✅。
- **写出时机**：输出缓冲超过 `writeBufferSize` 才写到流上 ✅；写出不等于 flush ✅。
- **写缓冲已满**：超过 `maxWriteBufferSize` → `WriteBufferFull(message)`，把消息交还调用方 ✅。
  - 在协程模型下，本库的 `send` 会挂起等待而不是失败，`WriteBufferFull` 只在非挂起的 `trySend` 中出现 ⚖️。理由：挂起即背压（neton-io §28.6）。
- **掩码**：客户端每帧随机掩码 ✅（随机源同 §3.2）。掩码按 8 字节字处理并考虑对齐 ⚖️（参考为 4 字节字），属于性能差异，结果以逐偏移量的对照测试保证一致。
- **发送时不自动分片** ✅；分片通过 `Message.Frame` 发送 ✅。
- **发送侧校验**（参考没有）⚖️：Ping / Pong 负载 > 125、关闭原因 > 123 字节、出站消息超过 `maxMessageSize` 均报错。理由：参考的文档声称会报 Capacity 错误，但代码未实现；RFC 要求控制帧 ≤ 125。

### 4.4 配置（`WebSocketConfig`，`protocol/mod.rs:44-153`）
| 字段 | 默认 | 语义 | 标注 |
|---|---|---|---|
| `readBufferSize` | 128 KiB | 读缓冲容量，也是单次读取上限 | ✅（缓冲来自 neton-io 池，读空后归还） |
| `writeBufferSize` | 128 KiB | 输出缓冲超过此值才写到流；0 表示每帧立即写 | ✅ |
| `maxWriteBufferSize` | 无上限 | 超过 → `WriteBufferFull`；必须大于 `writeBufferSize` | ⚖️ 默认改为 `writeBufferSize` 的 4 倍（资源有界，neton-io §28.1）；参考在不满足约束时 panic，本库构造时抛 `IllegalArgumentException` |
| `maxMessageSize` | 64 MiB | 入站重组消息上限 | ✅ |
| `maxFrameSize` | 16 MiB | 入站帧负载上限（读负载前检查） | ✅ |
| `acceptUnmaskedFrames` | false | 服务端容忍未掩码的客户端帧 | ✅ |

`setConfig` 重新校验并重新应用缓冲上限 ✅。

## 5. 关闭状态机、ping / pong、flush

- **状态**：`Active` → `ClosedByUs` / `ClosedByPeer` → `CloseAcknowledged` → `Terminated`（`protocol/mod.rs:795-828`）✅。
  - `canRead`：Active、ClosedByUs。
  - `canWrite`：只有 Active。
- **收到 Close**（`do_close`，`mod.rs:718-752`）✅：
  - Active 时：进入 ClosedByPeer；关闭码不被允许时改为 `{1002, "Protocol violation"}`；回显该帧；把 `Close` 交给调用方。
  - ClosedByUs 时：进入 CloseAcknowledged，交给调用方。
  - 其他状态：忽略。
- **终止的不对称**（RFC 6455 TIME_WAIT 规则）✅：
  - 服务端在回复写出后置 Terminated，并返回"连接已正常关闭"，由服务端先断开 TCP。
  - 客户端要等到 TCP EOF 才得到该结果。
  - 已关闭后连接被重置，也视为正常关闭。
- **两种结束的区分** ✅：
  - 正常结束 → `ConnectionClosed`（本库：`receive()` 返回 null）。
  - Terminated 之后再读写 → `AlreadyClosed`（编程错误）。
- **状态相关的错误** ✅：
  - 非 Active 时写 → `SendAfterClosing`。
  - Active 或 ClosedByPeer 等状态下 EOF → `ResetWithoutClosingHandshake`。
- **`close()`** ✅：只在 Active 时发送并进入 ClosedByUs，并总会 flush。`send(Message.Close)` 等价于 `close`。
- **协议错误不自动发送关闭帧**：参考只返回错误，由调用方断开；Autobahn 接受直接断开 TCP。本库默认与参考一致 ✅，另提供配置 `sendCloseOnProtocolError`（默认 false），开启时先发送 1002 / 1007 / 1009 再关闭。
- **自动 pong** ✅：
  - 收到 Ping 时，若为 Active，排入一个回复槽位（与 Ping 共享同一负载），并把 Ping 交给调用方。
  - 回复槽位只在为空或为 Pong 时被替换，因此待发的 Close 回复优先，新的 pong 覆盖旧的。
  - 用户发送的 Pong 同样经过该槽位。
  - 我方发出 Close 后不再回 pong。
- **读路径也会写，且写侧背压不得阻塞读侧推进**：
  - 参考的做法：`read` 先尝试 flush 待发的回复；遇到 WouldBlock 就记下 `unflushedAdditional` 并**继续读**，写与读由两个 waker 代理协调（`TT/src/compat.rs:21-121`）。
    所以对端暂时不读、却持续发送时，参考仍在消费入站数据。
  - 草案 v0 规定"写通道空闲时由读协程直接写出"。这会在对端不读时让读协程停在写操作上、不再消费入站数据，与参考不等价，**撤回**。
  - 本库：每个 `WebSocket` 有一个**写驱动**（连接作用域内的一个协程），独占流的写方向（与 neton-io §28.6"每个流同一时刻至多一个写"一致）。
    - 读协程产生的自动回复只放入回复槽位（槽位规则见上，至多一个待发回复），唤醒写驱动后立即继续读，**从不等待写出**。
    - 用户的 `feed` / `send` 把帧放入连接级输出缓冲（§6"帧的接受与所有权"），由写驱动写出。
    - 写驱动的待发数据有界：输出缓冲 ≤ `maxWriteBufferSize`，外加至多一个回复。
  - 对端既不读也不停发时，读侧的推进只受入站上限约束（`maxFrameSize` / `maxMessageSize` / 读缓冲），不受出站背压约束；回复槽位被新 Pong 覆盖（同参考），不会无限堆积。
  - **测试**（双向背压）：
    - 对端不读、持续发送 Ping：本端的 `receive` 持续返回 Ping，回复槽位只保留最新一个 Pong。
    - 对端不读时本端 `close`：Close 帧在槽位中优先，读侧继续推进直到收到对端 Close 或 EOF。
    - 以上情形下取消 `receive` / `send`：连接状态一致，后续操作正常，或按 §6 关闭。
    - 移植的参考 `auto_pong_flush.rs`。
- **flush 语义** ✅：`write`（本库 `feed`）只缓冲，`flush` 写出并 flush 流，`send` = `feed` + `flush`。

## 6. 协程 API 与 neton.io 映射

- **连接对象** `class WebSocket`（持有 `IoStream`；所属反应器上使用，遵守 neton-io §28.6 的线程规则）：
  - `suspend fun receive(): Message?`：null 表示正常结束；读完一条消息才返回，自动处理 ping / close 回复。
  - `suspend fun send(message)`、`suspend fun feed(message)`、`suspend fun flush()`、`fun trySend(message): Boolean`。
  - `suspend fun close(frame: CloseFrame? = null)`。
  - `val config`、`fun setConfig { }`、`val role`、`val canRead`、`val canWrite`。
  - `receive` 的取消：帧中途被取消时，已读入的字节留在读缓冲中，下次 `receive` 继续解析，对应参考 `poll_next` 的"读可取消"。
  - **帧的接受与所有权（`feed` / `send` 的取消）**：
    - **接受**：一个帧被编码并整体追加进连接级输出缓冲（在所属反应器上一次完成，不可分割），即为"已接受"。从此它归连接所有，由写驱动写出，
      与调用方协程无关；`feed` 在接受后返回。
    - **接受之前**：输出缓冲已满（达到 `maxWriteBufferSize`）时，`feed` 挂起等待空间。在此期间被取消，帧**未被接受**，没有任何字节进入缓冲或网络，
      调用方仍拥有该消息。
    - **接受之后**：调用方被取消（包括在 `flush` 中等待时）不影响已接受的帧，写驱动继续写完。网络上可能已发出半帧，取消无法撤回；由于写驱动
      持有剩余字节与写入位置，后续帧一定排在它之后，连接保持可用。
    - **写驱动自身被迫结束**（流出错、连接关闭）：写到一半的帧无法补全，连接进入 Terminated，此后读写抛 `AlreadyClosed`；不会出现"半帧之后接着写新帧"。
    - **测试**：用 neton-io `memoryStreamPair(capacity = 1)` 让每个字节都可能挂起，在写出过程中的每个字节位置取消调用方，对端收到的始终是完整、按序的帧，
      连接随后仍可正常收发；在每个字节位置让流出错，本端进入 Terminated，对端从未收到"半帧后接新帧"。
- **读写分离**：`fun split(): Pair<WebSocketReader, WebSocketWriter>`。参考借助 `futures::split`（BiLock）实现；本库原生提供，读与写可由两个协程分别使用，二者共享 §5 的写通道。
- **入口**：
  - 客户端：`connect(request, config)`、`client(request, stream, config)`。
  - 服务端：`accept(stream, config)`、`accept(stream, config) { request, response -> … }`。
  - 已握手的流：`WebSocket.fromRawStream(stream, role, config, prefix: Bytes? = null)`，对应 `from_raw_socket` / `from_partially_read`。
- **缓冲**：读缓冲使用 neton-io `Buffer`（池化，读空后归还）；负载以 neton-io `Bytes` 切片交出（零拷贝，对应参考的 `BytesMut.split_to().freeze()`）。
  - **零拷贝与缓冲池的关系**：按 neton-io §23.7 现行实现，一个数组一旦被 `Bytes` 切片共享，就不再归还缓冲池，之后由 GC 回收（写时复制保证切片内容
    不被改写）。因此"零拷贝交出负载"与"持续复用同一块池化数组"不能同时成立：交出了切片的读缓冲会换用新数组。每消息的分配与复制以 callgrind 实测，
    取两种交付方式（零拷贝切片 / 复制进池化缓冲）中的更优者作为默认，并记录数据。
- **错误类型**：`WebSocketException` 层级，与参考的 `Error` / `ProtocolError` / `CapacityError` / `SubProtocolError` / `UrlError` 各变体一一对应，保证能力对等；`ConnectionClosed` 以 `receive()` 返回 null 表达，不作为异常抛出。

## 7. 测试（移植清单）

- **单元测试**（逐条移植）：
  - `protocol/mod.rs`：receive_messages、两个 size_limiting 用例。
  - `frame/mod.rs`：read / write / from_partially_read / parse_overflow / size_limit_hit / 32 位长度溢出。
  - `frame/frame.rs`：parse / format / display。
  - `coding.rs`：opcode 与关闭码转换。
  - `mask.rs`：对照实现，偏移 0–7、长度 0–32。
  - `utf8.rs`。
  - `message.rs`。
  - `handshake`：RFC 向量 `dGhlIHNhbXBsZSBub25jZQ==` → `s3pPLMBiTxaQ9kYGzzhZRbK+xOo=`、客户端请求格式（主机、端口、userinfo）、响应解析、非 ASCII 头部、服务端四种非法 key。
  - `headers.rs`。
- **集成测试**（移植 `T/tests/`，每个文件一一对应）：
  - `auto_pong_flush`、`write`（`writeBufferSize = 600` 下的合并与 flush 次数）、`connection_reset`（含以 SO_LINGER 0 发 RST 的"恶意"服务端）。
  - `no_send_after_close`、`receive_after_init_close`、`handshake`（六种子协议协商）、`client_headers`。
  - 从 `TT/tests/` 移植：`communication`、`split_communication`、`handshakes`。
  - 以上都用 neton-io `memoryStreamPair` 与真实 TCP 各跑一遍。
- **有意不同项各有测试**：
  - `Connection` 头按 token 列表判断。
  - 64 位长度最高位为 1 时拒绝。
  - 超长控制帧在读负载前拒绝。
  - 发送侧的上限校验。
  - `maxWriteBufferSize` 的默认值。
  - `sendCloseOnProtocolError`。
- **模糊测试**：`parse_frame_header`、`read_message_client`、`read_message_server` 三个目标；以参考的种子语料（15 / 564 / 244 个文件，`T/fuzz/seeds/`）作为输入，再加随机变异；要求不崩溃、不挂起、错误类型合法。
- **Autobahn**（`crossbario/autobahn-testsuite`，客户端与服务端两种模式）：
  - 结果与参考的 `T/autobahn/expected-results.json` 逐项比较：OK 296、UNIMPLEMENTED 216（压缩）、NON-STRICT 2、INFORMATIONAL 3。
  - 本库每项至少达到参考的结果；任何一项差于参考即不通过。
  - 运行环境：153 或 colima 中的 docker，环境待确认。

## 8. 性能对照

- **基准**：移植 `T/benches/`：
  - `read`：10 万条小消息；服务端需去掩码，客户端不需要。
  - `write`：10 万条小消息后 flush；客户端需加掩码。
  - `e2e`：TCP 往返，512 B 到 1 GiB，每档乘 8，关闭上限检查。
  - 另加：多连接的回显吞吐与 p99（`echo-client-mass` 的 WebSocket 模式）。
- **对照对象**：tungstenite / tokio-tungstenite 0.30.0，以同等配置运行（相同缓冲大小、相同上限、均不压缩）。在 153 上按 neton-io §28.4 的规程与验收指标进行（吞吐、每消息 CPU、每消息分配数（callgrind）、p99、每连接公平性）。
- **首版的性能要求**：不低于参考实现，有差距时记录原因。协议核心的热路径（帧解析、去掩码、单帧消息交付）以零分配为目标。

## 9. 需要 neton-io 提供的能力（缺口清单，实现时逐项确认）

| 需要 | 现状 | 处理 |
|---|---|---|
| 字节流、半关闭、超时、取消语义 | `IoStream` + neton-io §28.6 | 已有，§28.6 落地后按能力声明使用 |
| 零拷贝负载切片 | `Bytes` / `Buffer.readSlice` | 已有 |
| 随机数（掩码、key） | 无 | 本库自带平台 CSPRNG 的 expect / actual；若 `http`、`quic` 也需要，再评估移入公共模块 |
| SHA-1、base64 | 无 | 本库自带（小，且只用于握手） |
| 非阻塞写（"能写多少写多少，写不了立即返回"） | `IoStream.write` 写完全部或挂起 | 不需要：写驱动独占写方向，读侧不写（§5） |
| 连接工厂（明文 / TLS） | `connect` | `wss` 由调用方传入 TLS 包装的工厂 |

## 10. 实施顺序（在 `http` 首版之后）

1. 协议核心：帧、掩码、UTF-8、重组、关闭状态机、配置；单元测试与模糊测试。
2. 握手：客户端与服务端；以 `http` 库的类型与头部解析实现；握手测试。
3. 协程驱动：`WebSocket` / `split` / 入口；集成测试（内存流与 TCP）。
4. Autobahn 客户端与服务端。
5. 性能对照（153）。

每一步单独验证、单独提交，结果记入本 SPEC。

## 11. 实施记录

### 11.1 步骤 1：协议核心（2026-09-28）
- 代码：`neton.websocket`（`WebSocketCore`、`Message`、`Utf8`、`WebSocketConfig`、`Error`）与 `neton.websocket.frame`（`Coding`、`Mask`、`Frame`、`FrameCodec` / `FrameSocket`），sans-I/O：读侧把收到的字节追加到 `core.input`，`read()` 返回消息或 null（需要更多输入）；写侧只排队，写驱动调用 `bufferReply()` → 写出 `output` → flush → `flushed()`。
- 测试：87 个，macosArm64 全过；linuxX64、mingwX64、androidNativeArm32、androidNativeX86 编译通过。
  - 参考 `src/` 单元测试全部移植，`error.rs` 的 3 个（Rust 内存大小断言）不适用 ⛔。
  - 参考 `tests/`：`auto_pong_flush`、`write`、`connection_reset`（3）、`no_send_after_close`、`receive_after_init_close` 用测试内的同步驱动移植；握手、TLS 相关的留给步骤 2，TCP / 内存流的集成运行留给步骤 3（§6）。
  - 另有 `DeviationTest`（每个 ⚖️ 一个以上）与 `WebSocketCoreTest`（29 个：解析检查顺序、跨分片重组与 UTF-8、关闭状态机、Ping 洪泛只保留最新 Pong、Close 优先于 Pong、客户端掩码）。
- 实现中新增的决定（相对 §4、§5 的补充）：
  - **单帧超过 `maxWriteBufferSize`**：输出缓冲为空时接受 ⚖️。否则有界默认值（4 × `writeBufferSize` = 512 KiB）会使大于 512 KiB 的消息永远无法发送，而入站允许 64 MiB。输出缓冲非空时照常 `WriteBufferFull`。
  - **我方 Close 经回复槽位发送**（在已排队的帧之后写出），覆盖待发的 Pong，且不会 `WriteBufferFull`；与 §5"槽位中 Close 优先""我方发出 Close 后不再回 pong"一致，与参考代码的路径不同（参考在 Close 之后仍可能写出更早的 Pong，RFC 6455 不允许 Close 之后再发帧）。
  - `writeBufferSize = 0` 时 `maxWriteBufferSize` 默认 512 KiB（4 × 0 不是合法配置）。
  - `WriteBufferFull` 交还调用方原来的 `Message`；长度错误报告完整的 64 位长度；`WebSocketState` 公开供驱动使用。
- 热路径分配：帧头解析进复用字段；解掩码与单帧 UTF-8 校验在输入缓冲内原地完成；负载以零拷贝 `Bytes` 切片交出，每条消息只分配切片与外层对象。以 callgrind 实测留待步骤 5。
