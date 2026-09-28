package neton.websocket.handshake

import neton.http.Method
import neton.http.Request
import neton.http.Response
import neton.http.ResponseBuilder
import neton.http.StatusCode
import neton.http.Version
import neton.http.h1.parse.HeaderSlots
import neton.http.h1.parse.ParseStatus
import neton.http.h1.parse.ParsedRequest
import neton.http.header.HeaderValue
import neton.http.uri.Uri
import neton.io.bytes.Buffer
import neton.io.bytes.Bytes
import neton.websocket.ProtocolError
import neton.websocket.WebSocketException
import neton.websocket.protocolError

// The server side of the handshake (tungstenite `T/src/handshake/server.rs`; SPEC §3.3), sans-I/O.

/** The client's handshake request (tungstenite `handshake::server::Request`, `http::Request<()>`). */
typealias ServerRequest = Request<Unit>

/** The server's handshake response (tungstenite `handshake::server::Response`, `http::Response<()>`). */
typealias ServerResponse = Response<Unit>

/** A response rejecting the handshake, with an optional body (tungstenite `ErrorResponse`). */
typealias ErrorResponse = Response<String?>

/**
 * Check the request (`create_parts`, `server.rs:37-87`; SPEC §3.3, in this order) and start the
 * `101 Switching Protocols` response: the request's version, `Connection: Upgrade`,
 * `Upgrade: websocket` and `Sec-WebSocket-Accept`.
 *
 * `Host` and `Origin` are not checked (as the reference); a [Callback] can.
 */
private fun createParts(request: Request<*>): ResponseBuilder {
    if (request.method != Method.GET) protocolError(ProtocolError.WrongHttpMethod)
    if (request.version < Version.HTTP_11) protocolError(ProtocolError.WrongHttpVersion)
    val headers = request.headers
    if (!headers["Connection"].hasToken("Upgrade")) protocolError(ProtocolError.MissingConnectionUpgradeHeader)
    if (!headers["Upgrade"].equalsIgnoreCase("websocket")) protocolError(ProtocolError.MissingUpgradeWebSocketHeader)
    if (headers["Sec-WebSocket-Version"]?.contentEquals("13") != true) protocolError(ProtocolError.MissingSecWebSocketVersionHeader)
    val key = headers["Sec-WebSocket-Key"] ?: protocolError(ProtocolError.MissingSecWebSocketKey)
    if (!isValidSecWebSocketKey(key)) protocolError(ProtocolError.InvalidSecWebSocketKey)

    return httpFormat {
        Response.builder()
            .status(StatusCode.SWITCHING_PROTOCOLS)
            .version(request.version)
            .header("Connection", "Upgrade")
            .header("Upgrade", "websocket")
            .header("Sec-WebSocket-Accept", deriveAcceptKey(key.asBytes()))
    }
}

/** 24 characters of base64 that decode to 16 bytes (`is_valid_sec_websocket_key`, `server.rs:89-99`). */
private fun isValidSecWebSocketKey(key: HeaderValue): Boolean {
    if (key.length != 24) return false
    return Base64.decode(key.asBytes())?.size == 16
}

/** Create the handshake response for [request] (`create_response`). @throws WebSocketException.Protocol an invalid request. */
fun createResponse(request: Request<*>): ServerResponse = httpFormat { createParts(request).body(Unit) }

/** Create the handshake response for [request] with a body (`create_response_with_body`). */
fun <T> createResponseWithBody(request: Request<*>, generateBody: () -> T): Response<T> {
    val parts = createParts(request)
    return httpFormat { parts.body(generateBody()) }
}

/**
 * Write the head of [response] to [out] (`write_response`, `server.rs:115-131`): the status line,
 * each header as `name: value` (names in their lowercase form), and the empty line.
 * @throws WebSocketException.Protocol [ProtocolError.WrongHttpVersion] for HTTP/2 or 3.
 * @throws WebSocketException.Utf8 a header value that is not visible ASCII.
 */
fun writeResponse(out: Buffer, response: Response<*>) {
    val sb = StringBuilder()
    sb.append(versionAsStr(response.version)).append(' ').append(response.status).append("\r\n")
    response.headers.forEach { name, value -> sb.append(name.asStr()).append(": ").append(value.toStrOrThrow()).append("\r\n") }
    sb.append("\r\n")
    out.writeBytes(sb.toString().encodeToByteArray())
}

/**
 * Parse a request head (`TryParse for Request`, `server.rs:133-143`, and `FromHttparse`,
 * `server.rs:145-166`): null while incomplete, else the bytes consumed and the request (method GET,
 * version HTTP/1.1).
 * @throws WebSocketException.Protocol [ProtocolError.WrongHttpMethod] (checked first),
 *   [ProtocolError.WrongHttpVersion] for HTTP/1.0, or a parse error.
 * @throws WebSocketException.Capacity more than [MAX_HEADERS] headers.
 * @throws WebSocketException.HttpFormat an invalid header or request target.
 */
fun tryParseRequest(bytes: ByteArray, offset: Int = 0, length: Int = bytes.size - offset): Pair<Int, ServerRequest>? {
    val raw = ParsedRequest(HeaderSlots(MAX_HEADERS))
    val status = raw.parse(bytes, offset, length)
    if (ParseStatus.isPartial(status)) return null
    if (ParseStatus.isError(status)) throwParseError(status)
    return status to requestFromParsed(raw)
}

/** A complete [ParsedRequest] as a request (`FromHttparse for Request`, `server.rs:145-166`). */
internal fun requestFromParsed(raw: ParsedRequest): ServerRequest {
    val bytes = checkNotNull(raw.bytes) { "request not parsed" }
    if (raw.methodString() != "GET") protocolError(ProtocolError.WrongHttpMethod)
    if (raw.version < 1) protocolError(ProtocolError.WrongHttpVersion)
    val headers = headersFrom(raw.headers, bytes)
    val request = Request(Unit)
    request.method = Method.GET
    request.parts.headers = headers
    request.uri = httpFormat { Uri.parse(checkNotNull(raw.pathString()) { "no path in request" }) }
    // httparse only knows HTTP/0.9-1.1, so the only valid version here is 1.1.
    request.version = Version.HTTP_11
    return request
}

/**
 * What a [Callback] decided: go on with a (possibly changed) response, or reject the handshake
 * with an error response (the reference's `Result<Response, ErrorResponse>`).
 */
sealed interface CallbackResult {
    /** Complete the handshake with [response]. */
    class Accept(val response: ServerResponse) : CallbackResult

    /** Send [response] (it must not be 2xx) and fail the handshake with [WebSocketException.Http]. */
    class Reject(val response: ErrorResponse) : CallbackResult
}

/**
 * Called once the server has read the client's request and is ready to reply (tungstenite
 * `Callback`, `server.rs:168-196`): it may inspect the request, add headers to the response (a
 * sub-protocol, say; there is no automatic negotiation) or reject the connection.
 */
fun interface Callback {
    fun onRequest(request: ServerRequest, response: ServerResponse): CallbackResult

    companion object {
        /** Accepts every request with the default response (`NoCallback`). */
        val None: Callback = Callback { _, response -> CallbackResult.Accept(response) }
    }
}

/**
 * A server handshake without I/O (tungstenite `ServerHandshake`, `server.rs:198-305`): given the
 * parsed request and the bytes read after it, [reply] decides the bytes to send and the outcome.
 */
class ServerHandshake(private val callback: Callback = Callback.None) {

    /**
     * What to send back and how the handshake ends.
     * @property bytes the response to write and flush.
     * @property error when set, the handshake failed: throw it once [bytes] are flushed.
     */
    class Reply(val bytes: ByteArray, val error: WebSocketException.Http?)

    /**
     * Process the request (`stage_finished` for `DoneReading`, `server.rs:242-289`): bytes after the
     * request are [ProtocolError.JunkAfterRequest]; then the checks of [createResponse] (no response
     * is sent when they fail); then the callback.
     * @throws WebSocketException.Protocol [ProtocolError.JunkAfterRequest], an invalid request, or
     *   [ProtocolError.CustomResponseSuccessful] for a 2xx error response.
     */
    fun reply(request: ServerRequest, tail: Bytes): Reply {
        if (!tail.isEmpty) protocolError(ProtocolError.JunkAfterRequest)
        val response = createResponse(request)
        val out = Buffer(256)
        return when (val decision = callback.onRequest(request, response)) {
            is CallbackResult.Accept -> {
                writeResponse(out, decision.response)
                Reply(out.readAll(), null)
            }
            is CallbackResult.Reject -> {
                val resp = decision.response
                if (resp.status.isSuccess()) protocolError(ProtocolError.CustomResponseSuccessful)
                writeResponse(out, resp)
                val body = resp.body?.encodeToByteArray()
                if (body != null) out.writeBytes(body)
                Reply(out.readAll(), WebSocketException.Http(Response(resp.parts, body)))
            }
        }
    }
}
