# websocket

WebSocket (RFC 6455) for Kotlin/Native on top of `com.netonstream:io`, with the handshake on `com.netonstream:http`
types. The first version replicates tungstenite 0.30.0 / tokio-tungstenite 0.30.0. Maven coordinate
`com.netonstream:websocket`, package `neton.websocket`.

Specification, every deliberate difference from the reference (marked ⚖️), and the implementation record:
[SPEC.md](SPEC.md).

## Status

Release coordinate: `com.netonstream:websocket:0.2.0`, built against `com.netonstream:io:0.3.2` and `com.netonstream:http:0.2.0`.
0.2.0 adds permessage-deflate (RFC 7692, off by default).
Kotlin 2.4.20 (the build refuses anything lower), native targets only.

- **Frames and messages:** masking, fragmentation, UTF-8 validation of text messages (incremental), control frames,
  and the close handshake.
- **Handshake:** client and server, including tungstenite's callback for inspecting or rejecting the server
  handshake. TLS is left to the caller (`TlsConnector`).
- **Configuration:** tungstenite's `WebSocketConfig`: message, frame and buffer limits and the write-buffer policy.
- **permessage-deflate (RFC 7692):** off by default (tungstenite 0.30 has none); `WebSocketConfig(compression =
  PerMessageDeflateConfig())` offers it as a client or accepts it as a server, with the four RFC parameters (SPEC §11.7).

## Conformance

- tungstenite's own tests are ported, and its three fuzz targets run as deterministic tests over its seed corpus with
  seeded mutations (SPEC §11.6); the suite has 218 tests.
- Autobahn|Testsuite, run in both the client and the server direction (517 cases each):
  - no failures;
  - behaviour: 296 OK, 2 NON-STRICT (the same as the reference), 3 INFORMATIONAL;
  - without compression, 216 UNIMPLEMENTED: the permessage-deflate cases, which tungstenite does not implement;
  - with permessage-deflate on, those 216 pass too: 512 OK, 2 NON-STRICT, 3 INFORMATIONAL in each direction.

## Usage

Server (echo):

```kotlin
serveTcp("127.0.0.1", 9001, reactors = 1, shutdownOnSignals = true) { stream ->
    val ws = accept(stream)
    while (true) {
        when (val msg = ws.receive() ?: break) {
            is Message.Text, is Message.Binary -> ws.send(msg)
            else -> {}
        }
    }
}
```

Client: `client("ws://host:port/path", stream)` on a connected `IoStream`, then the same `receive` / `send` / `close`.
`WebSocketReader` / `WebSocketWriter` split one connection between two coroutines.

A complete program: `websocket-bench/src/nativeMain/kotlin/neton/websocket/bench/Autobahn.kt`.

## Performance

Measured in one scenario only (SPEC §11.5): echo of 64-byte binary messages, 50 closed-loop connections, the server
on one pinned core, on a 4-vCPU Linux VM, against tokio-tungstenite.

| | neton | tokio-tungstenite |
|---|---|---|
| Throughput (msg/s), default | 130.5k / 131.7k | 129.8k / 131.1k |
| Throughput (msg/s), GC thread at nice 19 | 149.3k / 146.7k | 131.6k / 127.6k |
| p99 | ~0.53 ms | ~0.65 ms |
| p99.9 | 1.4 ms | 0.8 ms |

Other message sizes, text and fragmented messages, fan-out and large connection counts have not been compared.
Passing the protocol tests, performance in this one scenario, and production maturity are three separate claims;
only the first two are established, and only within what was measured.

## Building and testing

```
./gradlew :websocket:macosArm64Test          # or linuxX64Test (NETON_IO_DRIVER=epoll|iouring)
```

Autobahn scripts and configuration are in `bench/autobahn/`, and the echo benchmark in `bench/ws/`.
