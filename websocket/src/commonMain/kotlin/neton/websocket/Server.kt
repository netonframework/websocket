package neton.websocket

import neton.io.core.IoStream
import neton.websocket.handshake.Callback
import neton.websocket.handshake.HandshakeLimits
import neton.websocket.handshake.ServerHandshake
import neton.websocket.handshake.TryParse
import neton.websocket.handshake.tryParseRequest

// Server entry points (tungstenite `T/src/server.rs`, tokio-tungstenite `accept_async*`; SPEC §3.3, §6).
//
// ⚖️ Suspend functions complete the handshake: there is no `Interrupted` / `MidHandshake` (SPEC §2).
// The caller keeps ownership of the stream: it is not closed on failure. Each returns the protocol
// core for the connection (`Role.Server`); nothing is left over, since bytes after the request are
// [ProtocolError.JunkAfterRequest].

/** Accept [stream] as a WebSocket (`accept`). */
suspend fun accept(stream: IoStream): WebSocketCore = acceptWithConfig(stream, null)

/** Accept [stream] as a WebSocket with [config] (`accept_with_config`); null is the default configuration. */
suspend fun acceptWithConfig(stream: IoStream, config: WebSocketConfig?, limits: HandshakeLimits = HandshakeLimits()): WebSocketCore =
    acceptHdrWithConfig(stream, Callback.None, config, limits)

/**
 * Accept [stream] as a WebSocket, letting [callback] inspect the request and change or reject the
 * response (`accept_hdr`).
 */
suspend fun acceptHdr(stream: IoStream, callback: Callback): WebSocketCore = acceptHdrWithConfig(stream, callback, null)

/**
 * The server handshake (`accept_hdr_with_config`, `ServerHandshake`, `T/src/handshake/server.rs:198-305`):
 * read the request, check it, run [callback], write and flush the response.
 *
 * @throws WebSocketException.Http [callback] rejected the request; its response was sent.
 * @throws WebSocketException.Protocol an invalid request (no response is sent),
 *   [ProtocolError.JunkAfterRequest], or [ProtocolError.CustomResponseSuccessful].
 * @throws WebSocketException for a limit of [limits] or an I/O error.
 */
suspend fun acceptHdrWithConfig(
    stream: IoStream,
    callback: Callback,
    config: WebSocketConfig?,
    limits: HandshakeLimits = HandshakeLimits(),
): WebSocketCore {
    val (request, tail) = readHead(stream, limits, TryParse(::tryParseRequest))
    val reply = ServerHandshake(callback).reply(request, tail)
    writeAndFlush(stream, reply.bytes)
    reply.error?.let { throw it }
    return WebSocketCore(Role.Server, config ?: WebSocketConfig())
}
