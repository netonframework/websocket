package neton.websocket

/** Server or client side of a connection (tungstenite `Role`, `T/src/protocol/mod.rs:26-32`). */
enum class Role { Server, Client }

/**
 * Connection settings (tungstenite `WebSocketConfig`, `T/src/protocol/mod.rs:44-153`; SPEC §4.4).
 *
 * Sizes are in bytes; `null` for [maxMessageSize] / [maxFrameSize] means no limit (buffers are
 * `Int`-indexed, so the effective ceiling is [Int.MAX_VALUE]).
 *
 * @property readBufferSize read buffer capacity, also the most one read should ask for.
 * @property writeBufferSize the output is written to the stream once it holds more than this;
 *   0 writes every frame at once. `flush` always writes everything.
 * @property maxWriteBufferSize bound on the queued output; must be greater than [writeBufferSize].
 *   ⚖️ Defaults to 4 × [writeBufferSize] (the reference: unlimited); with [writeBufferSize] 0,
 *   to 4 × [DEFAULT_WRITE_BUFFER_SIZE].
 * @property maxMessageSize bound on an incoming (reassembled) message, and ⚖️ on outgoing ones.
 * @property maxFrameSize bound on an incoming frame payload, checked before the payload is read.
 * @property acceptUnmaskedFrames a server tolerates unmasked client frames (RFC 6455 says fail).
 * @property sendCloseOnProtocolError when reading fails with a protocol, UTF-8 or size error, queue
 *   a close frame with 1002 / 1007 / 1009 before the error is reported (SPEC §5; default off, like
 *   the reference, which never sends one).
 * @property compression ⚖️ permessage-deflate (RFC 7692) to offer (client) or accept (server); null, the default,
 *   is off (the reference has none). See [PerMessageDeflateConfig].
 * @property closeTimeoutMillis ⚖️ once a close frame was sent or received, the connection ends within this long even
 *   if the peer never completes the closing handshake (never answers, never closes TCP): it then fails with
 *   [WebSocketException.Timeout]. 0: no limit, as the reference. Default 10 s (SPEC §11.8).
 * @property idleTimeoutMillis ⚖️ the connection fails with [WebSocketException.Timeout] after this long without
 *   reading or writing anything. 0, the default: no limit (keep-alive pings are the application's).
 * @throws IllegalArgumentException if [maxWriteBufferSize] <= [writeBufferSize] (the reference panics).
 */
data class WebSocketConfig(
    val readBufferSize: Int = DEFAULT_READ_BUFFER_SIZE,
    val writeBufferSize: Int = DEFAULT_WRITE_BUFFER_SIZE,
    val maxWriteBufferSize: Int = defaultMaxWriteBufferSize(writeBufferSize),
    val maxMessageSize: Int? = DEFAULT_MAX_MESSAGE_SIZE,
    val maxFrameSize: Int? = DEFAULT_MAX_FRAME_SIZE,
    val acceptUnmaskedFrames: Boolean = false,
    val sendCloseOnProtocolError: Boolean = false,
    val compression: PerMessageDeflateConfig? = null,
    val closeTimeoutMillis: Long = DEFAULT_CLOSE_TIMEOUT_MILLIS,
    val idleTimeoutMillis: Long = 0,
) {
    init {
        require(readBufferSize >= 0) { "readBufferSize must not be negative" }
        require(writeBufferSize >= 0) { "writeBufferSize must not be negative" }
        require(maxWriteBufferSize > writeBufferSize) {
            "maxWriteBufferSize ($maxWriteBufferSize) must be greater than writeBufferSize ($writeBufferSize)"
        }
        require(maxMessageSize == null || maxMessageSize >= 0) { "maxMessageSize must not be negative" }
        require(maxFrameSize == null || maxFrameSize >= 0) { "maxFrameSize must not be negative" }
        require(closeTimeoutMillis >= 0) { "closeTimeoutMillis must not be negative" }
        require(idleTimeoutMillis >= 0) { "idleTimeoutMillis must not be negative" }
    }

    companion object {
        const val DEFAULT_READ_BUFFER_SIZE = 128 * 1024
        const val DEFAULT_WRITE_BUFFER_SIZE = 128 * 1024
        const val DEFAULT_MAX_MESSAGE_SIZE = 64 shl 20
        const val DEFAULT_MAX_FRAME_SIZE = 16 shl 20
        const val DEFAULT_CLOSE_TIMEOUT_MILLIS: Long = 10_000

        /** ⚖️ The default [maxWriteBufferSize] for a given [writeBufferSize] (SPEC §4.4). */
        fun defaultMaxWriteBufferSize(writeBufferSize: Int): Int {
            val base = if (writeBufferSize == 0) DEFAULT_WRITE_BUFFER_SIZE else writeBufferSize
            return if (base > Int.MAX_VALUE / 4) Int.MAX_VALUE else base * 4
        }
    }
}
