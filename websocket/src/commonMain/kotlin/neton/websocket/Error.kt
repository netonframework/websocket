package neton.websocket

import neton.http.HttpException
import neton.http.Response
import neton.http.h1.parse.HttpParseError
import neton.websocket.frame.OpCode

/**
 * Every error of this library (tungstenite `Error`, `T/src/error.rs:14-77`).
 *
 * The coroutine API (SPEC §6) expresses [ConnectionClosed] as `receive()` returning null; the
 * sans-I/O [WebSocketCore] throws it, like tungstenite returns `Err(ConnectionClosed)`.
 * The reference's `Tls` variant does not exist here: TLS is an [neton.io.core.IoStream] wrapper
 * supplied by the caller, whose errors reach the caller as they are (SPEC §2 ⛔).
 */
sealed class WebSocketException(message: String, cause: Throwable? = null) : Exception(message, cause) {
    /**
     * The connection closed normally (both close frames exchanged; for a client, also the TCP
     * connection closed by the server). Not an error as such: the connection may be dropped now.
     */
    class ConnectionClosed : WebSocketException("Connection closed normally")

    /** Used the connection after [ConnectionClosed] was reported: a programming error. */
    class AlreadyClosed : WebSocketException("Trying to work with closed connection")

    /** Error of the underlying stream. */
    class Io(cause: Throwable) : WebSocketException("IO error: ${cause.message}", cause)

    /** A size limit was exceeded. */
    class Capacity(val error: CapacityError) : WebSocketException("Space limit exceeded: $error")

    /** Protocol violation. */
    class Protocol(val error: ProtocolError) : WebSocketException("WebSocket protocol error: $error")

    /** The write buffer is full; [rejected] was not queued and still belongs to the caller. */
    class WriteBufferFull(val rejected: Message) : WebSocketException("Write buffer is full")

    /** Invalid UTF-8 in a text message or a close reason (not a [Protocol] error, like the reference). */
    class Utf8(val detail: String) : WebSocketException("UTF-8 encoding error: $detail")

    /** Attack attempt detected (handshake limits, SPEC §3.1). */
    class AttackAttempt : WebSocketException("Attack attempt detected")

    /** Invalid URL. */
    class Url(val error: UrlError) : WebSocketException("URL error: $error")

    /**
     * The handshake got an HTTP response other than the upgrade: the peer's non-101 response on
     * the client, or the error response the server callback chose (and sent). [response]'s body
     * is what followed the head (client) or the callback's body (server), if any.
     */
    class Http(val response: Response<ByteArray?>) : WebSocketException("HTTP error: ${response.status}")

    /** An HTTP value (URI, header name or value, status code) could not be built or parsed. */
    class HttpFormat(cause: HttpException) : WebSocketException("HTTP format error: ${cause.message}", cause)
}

/** Which size limit was exceeded (tungstenite `CapacityError`, `error.rs:141-156`). */
sealed class CapacityError(private val description: String) {
    final override fun toString(): String = description

    /** Too many headers in a handshake. */
    data object TooManyHeaders : CapacityError("Too many headers")

    /**
     * A message or frame is bigger than allowed. Sizes are [Long] so a 64-bit wire length is
     * reported as received (the reference narrows it to `usize`).
     */
    data class MessageTooLong(val size: Long, val maxSize: Long) : CapacityError("Message too long: $size > $maxSize")
}

/** Sub-protocol negotiation errors (tungstenite `SubProtocolError`, `error.rs:158-173`). */
sealed class SubProtocolError(private val description: String) {
    final override fun toString(): String = description
    data object ServerSentSubProtocolNoneRequested : SubProtocolError("Server sent a subprotocol but none was requested")
    data object InvalidSubProtocol : SubProtocolError("Server sent an invalid subprotocol")
    data object NoSubProtocol : SubProtocolError("Server sent no subprotocol")
}

/** Kinds of protocol violation (tungstenite `ProtocolError`, `error.rs:175-275`). */
sealed class ProtocolError(private val description: String) {
    final override fun toString(): String = description

    // ---- handshake (SPEC §3) ----
    data object WrongHttpMethod : ProtocolError("Unsupported HTTP method used - only GET is allowed")
    data object WrongHttpVersion : ProtocolError("HTTP version must be 1.1 or higher")
    data object MissingConnectionUpgradeHeader : ProtocolError("No \"Connection: upgrade\" header")
    data object MissingUpgradeWebSocketHeader : ProtocolError("No \"Upgrade: websocket\" header")
    data object MissingSecWebSocketVersionHeader : ProtocolError("No \"Sec-WebSocket-Version: 13\" header")
    data object MissingSecWebSocketKey : ProtocolError("No \"Sec-WebSocket-Key\" header")
    data object InvalidSecWebSocketKey : ProtocolError("Invalid \"Sec-WebSocket-Key\" header value")
    data object SecWebSocketAcceptKeyMismatch : ProtocolError("Key mismatch in \"Sec-WebSocket-Accept\" header")
    data class SecWebSocketSubProtocolError(val error: SubProtocolError) : ProtocolError("SubProtocol error: $error")
    data object JunkAfterRequest : ProtocolError("Junk after client request")
    data object CustomResponseSuccessful : ProtocolError("Custom response must not be successful")
    data class InvalidHeader(val name: String) : ProtocolError("Missing, duplicated or incorrect header $name")
    data object HandshakeIncomplete : ProtocolError("Handshake not finished")
    data class HttparseError(val error: HttpParseError) : ProtocolError("httparse error: $error")

    /**
     * ⚖️ Not in the reference (its check is a TODO): the server's `Sec-WebSocket-Extensions` names
     * an extension the client did not offer (RFC 6455 §4.1, step 5 of the client's checks; SPEC §3.2).
     */
    data class ExtensionNotRequested(val extension: String) : ProtocolError("Server sent an extension that was not requested: $extension")

    // ---- frames and the close state machine (SPEC §4, §5) ----
    data object SendAfterClosing : ProtocolError("Sending after closing is not allowed")
    data object ReceivedAfterClosing : ProtocolError("Remote sent after having closed")
    data object NonZeroReservedBits : ProtocolError("Reserved bits are non-zero")
    data object UnmaskedFrameFromClient : ProtocolError("Received an unmasked frame from client")
    data object MaskedFrameFromServer : ProtocolError("Received a masked frame from server")
    data object FragmentedControlFrame : ProtocolError("Fragmented control frame")
    data object ControlFrameTooBig : ProtocolError("Control frame too big (payload must be 125 bytes or less)")
    data class UnknownControlFrameType(val code: Int) : ProtocolError("Unknown control frame type: $code")
    data class UnknownDataFrameType(val code: Int) : ProtocolError("Unknown data frame type: $code")
    data object UnexpectedContinueFrame : ProtocolError("Continue frame but nothing to continue")
    data class ExpectedFragment(val data: OpCode.Data) : ProtocolError("While waiting for more fragments received: $data")
    data object ResetWithoutClosingHandshake : ProtocolError("Connection reset without closing handshake")
    data class InvalidOpcode(val code: Int) : ProtocolError("Encountered invalid opcode: $code")
    data object InvalidCloseSequence : ProtocolError("Invalid close sequence")

    /**
     * ⚖️ Not in the reference: a 64-bit payload length with the most significant bit set
     * (RFC 6455 §5.2 requires it to be 0; SPEC §4.1).
     */
    data object InvalidPayloadLength : ProtocolError("Payload length has the most significant bit set")
}

/** URL errors (tungstenite `UrlError`, `error.rs:277-299`); used by the client entry points (SPEC §3.2). */
sealed class UrlError(private val description: String) {
    final override fun toString(): String = description
    data object TlsNotAvailable : UrlError("TLS support not available")
    data object NoHostName : UrlError("No host name in the URL")
    data class UnableToConnect(val target: String) : UrlError("Unable to connect to $target")
    data object UnsupportedUrlScheme : UrlError("URL scheme not supported")
    data object EmptyHostName : UrlError("URL contains empty host name")
    data object NoPathOrQuery : UrlError("No path/query in URL")
}

internal fun protocolError(e: ProtocolError): Nothing = throw WebSocketException.Protocol(e)
