package neton.websocket.handshake

import neton.http.Method
import neton.http.Request
import neton.http.Response
import neton.http.StatusCode
import neton.http.Version
import neton.http.h1.parse.HeaderSlots
import neton.http.h1.parse.ParseStatus
import neton.http.h1.parse.ParsedResponse
import neton.http.header.HeaderMap
import neton.http.header.HeaderName
import neton.http.header.HeaderValue
import neton.io.bytes.Buffer
import neton.io.core.secureRandom
import neton.websocket.ProtocolError
import neton.websocket.SubProtocolError
import neton.websocket.UrlError
import neton.websocket.WebSocketException
import neton.websocket.protocolError
import neton.websocket.uriMode

// The client side of the handshake (tungstenite `T/src/handshake/client.rs`; SPEC §3.2), sans-I/O.

/** A client handshake request (tungstenite `handshake::client::Request`, `http::Request<()>`). */
typealias ClientRequest = Request<Unit>

/**
 * The server's handshake response as the client sees it (tungstenite `handshake::client::Response`,
 * `http::Response<Option<Vec<u8>>>`): the body is null for a successful handshake; for a failed
 * one ([WebSocketException.Http]) it holds the bytes read after the head.
 */
typealias ClientResponse = Response<ByteArray?>

/**
 * A client handshake in progress (tungstenite `ClientHandshake`, `client.rs:33-81`), without I/O:
 * send [request], read the response head, then [verifyResponse].
 *
 * @property request the request bytes to send (`generate_request`).
 */
class ClientHandshake private constructor(val request: ByteArray, private val verifyData: VerifyData) {

    /**
     * Check the server's response (`VerifyData::verify_response`, `client.rs:220-295`; SPEC §3.2).
     * @throws WebSocketException.Http the status is not 101 (the caller sets the body).
     * @throws WebSocketException.Protocol a missing or wrong upgrade header, accept key, extension
     *   or sub-protocol.
     */
    fun verifyResponse(response: ClientResponse): ClientResponse = verifyData.verifyResponse(response)

    companion object {
        /**
         * Initiate a client handshake (`ClientHandshake::start`, `client.rs:42-80`): check the
         * method, version and URI scheme, note the requested sub-protocols and extensions, and
         * generate the request bytes.
         */
        fun start(request: ClientRequest): ClientHandshake {
            if (request.method != Method.GET) protocolError(ProtocolError.WrongHttpMethod)
            if (request.version < Version.HTTP_11) protocolError(ProtocolError.WrongHttpVersion)
            // Check the URI scheme: only ws or wss are supported.
            uriMode(request.uri)
            val subprotocols = extractSubprotocolsFromRequest(request)
            val extensions = extractExtensionNames(request.headers)
            val (bytes, key) = generateRequest(request)
            return ClientHandshake(bytes, VerifyData(deriveAcceptKey(key.encodeToByteArray()), subprotocols, extensions))
        }
    }
}

private const val KEY_HEADERNAME = "Sec-WebSocket-Key"

/** Headers that must be present in a correct request, in the order they are written. */
private val WEBSOCKET_HEADERS = arrayOf("Host", "Connection", "Upgrade", "Sec-WebSocket-Version", KEY_HEADERNAME)

private fun invalidHeader(name: String): Nothing = protocolError(ProtocolError.InvalidHeader(name.lowercase()))

/**
 * Verify [request] and write it as the handshake request (`generate_request`, `client.rs:112-199`;
 * SPEC §3.2). Returns the bytes and the `Sec-WebSocket-Key`. [request]'s headers are consumed:
 * the five required ones are removed.
 *
 * The request line is `GET {path_and_query} {version}`; the five required headers follow in a
 * fixed order and canonical case, then every other header as raw bytes (non-ASCII values are
 * kept), with `sec-websocket-protocol` and `origin` written as `Sec-WebSocket-Protocol` and
 * `Origin` for servers that match names case-sensitively.
 */
fun generateRequest(request: ClientRequest): Pair<ByteArray, String> {
    val out = Buffer(256)
    val path = request.uri.pathAndQuery?.asStr() ?: throw WebSocketException.Url(UrlError.NoPathOrQuery)
    out.writeAscii("GET $path ${versionAsStr(request.version)}\r\n")

    // We must extract a WebSocket key from a properly formed request or fail if it's not present.
    val key = (request.headers[KEY_HEADERNAME] ?: invalidHeader(KEY_HEADERNAME)).toStrOrThrow()

    // Some servers match header names case-sensitively (hyper issue 1492): write the required ones
    // in their canonical case. Removing a name removes all of its values.
    val headers = request.headers
    for (header in WEBSOCKET_HEADERS) {
        val value = headers.remove(header) ?: invalidHeader(header)
        val text = value.tryToStr()
            ?: throw WebSocketException.Utf8("failed to convert header to a str for header name '$header' with value: $value")
        out.writeAscii("$header: $text\r\n")
    }

    // The required headers were written once and removed; seeing one again makes the request invalid.
    var failed: HeaderName? = null
    headers.forEach { name, value ->
        if (failed != null) return@forEach
        if (WEBSOCKET_HEADERS.any { name.equalsIgnoreCase(it) }) { failed = name; return@forEach }
        val written = when (name.asStr()) {
            "sec-websocket-protocol" -> "Sec-WebSocket-Protocol"
            "origin" -> "Origin"
            else -> name.asStr()
        }
        // Raw bytes: header values are octets (RFC 7230), not UTF-8 strings.
        out.writeAscii(written)
        out.writeAscii(": ")
        out.writeBytes(value.asBytes())
        out.writeAscii("\r\n")
    }
    failed?.let { protocolError(ProtocolError.InvalidHeader(it.asStr())) }

    out.writeAscii("\r\n")
    return out.readAll() to key
}

private fun Buffer.writeAscii(s: String) = writeBytes(s.encodeToByteArray())

/** The requested sub-protocols (`extract_subprotocols_from_request`, `client.rs:201-207`). */
private fun extractSubprotocolsFromRequest(request: ClientRequest): List<String>? =
    request.headers["Sec-WebSocket-Protocol"]?.toStrOrThrow()?.split(',')?.map { it.trim() }

/**
 * ⚖️ The extension names offered in `Sec-WebSocket-Extensions` headers (RFC 6455 §9.1: a
 * comma-separated list of `name; params`), lowercased; non-ASCII values offer nothing.
 */
private fun extractExtensionNames(headers: HeaderMap<HeaderValue>): Set<String> {
    val names = mutableSetOf<String>()
    for (value in headers.getAll("Sec-WebSocket-Extensions")) names += extensionNames(value)
    return names
}

private fun extensionNames(value: HeaderValue): List<String> =
    value.tryToStr()?.split(',')?.map { it.substringBefore(';').trim().lowercase() }?.filter { it.isNotEmpty() } ?: emptyList()

/** What the response is checked against (`VerifyData`, `client.rs:209-218`). */
private class VerifyData(
    /** Expected `Sec-WebSocket-Accept`. */
    val acceptKey: String,
    /** Requested sub-protocols, null when none were. */
    val subprotocols: List<String>?,
    /** ⚖️ Extension names the client offered. */
    val extensions: Set<String>,
) {
    fun verifyResponse(response: ClientResponse): ClientResponse {
        // 1. If the status code received from the server is not 101, the client handles the
        // response per HTTP [RFC2616] procedures. (RFC 6455)
        if (response.status != StatusCode.SWITCHING_PROTOCOLS) throw WebSocketException.Http(response)

        val headers = response.headers

        // 2. If the response lacks an |Upgrade| header field or the |Upgrade| header field
        // contains a value that is not an ASCII case-insensitive match for the value "websocket",
        // the client MUST _Fail the WebSocket Connection_. (RFC 6455)
        if (!headers["Upgrade"].equalsIgnoreCase("websocket")) protocolError(ProtocolError.MissingUpgradeWebSocketHeader)

        // 3. If the response lacks a |Connection| header field or the |Connection| header field
        // doesn't contain a token that is an ASCII case-insensitive match for the value "Upgrade",
        // the client MUST _Fail the WebSocket Connection_. (RFC 6455)
        // ⚖️ Checked as a token list (the reference compares the whole value, SPEC §3.2):
        // `Connection: keep-alive, Upgrade` is a valid response.
        if (!headers["Connection"].hasToken("Upgrade")) protocolError(ProtocolError.MissingConnectionUpgradeHeader)

        // 4. If the response lacks a |Sec-WebSocket-Accept| header field or the
        // |Sec-WebSocket-Accept| contains a value other than the base64-encoded SHA-1 of ... the
        // client MUST _Fail the WebSocket Connection_. (RFC 6455)
        if (headers["Sec-WebSocket-Accept"]?.contentEquals(acceptKey) != true) {
            protocolError(ProtocolError.SecWebSocketAcceptKeyMismatch)
        }

        // 5. If the response includes a |Sec-WebSocket-Extensions| header field and this header
        // field indicates the use of an extension that was not present in the client's handshake
        // (the server has indicated an extension not requested by the client), the client MUST
        // _Fail the WebSocket Connection_. (RFC 6455)
        // ⚖️ The reference leaves this as a TODO (SPEC §3.2).
        for (value in headers.getAll("Sec-WebSocket-Extensions")) {
            if (value.tryToStr() == null) protocolError(ProtocolError.ExtensionNotRequested(value.toString()))
            for (name in extensionNames(value)) if (name !in extensions) protocolError(ProtocolError.ExtensionNotRequested(name))
        }

        // 6. If the response includes a |Sec-WebSocket-Protocol| header field and this header
        // field indicates the use of a subprotocol that was not present in the client's handshake
        // (the server has indicated a subprotocol not requested by the client), the client MUST
        // _Fail the WebSocket Connection_. (RFC 6455)
        val returned = headers["Sec-WebSocket-Protocol"]
        if (returned == null && subprotocols != null) subProtocolError(SubProtocolError.NoSubProtocol)
        if (returned != null && subprotocols == null) subProtocolError(SubProtocolError.ServerSentSubProtocolNoneRequested)
        if (returned != null && subprotocols != null && returned.toStrOrThrow() !in subprotocols) {
            subProtocolError(SubProtocolError.InvalidSubProtocol)
        }

        return response
    }

    private fun subProtocolError(e: SubProtocolError): Nothing = protocolError(ProtocolError.SecWebSocketSubProtocolError(e))
}

/**
 * Parse a response head (`TryParse for Response`, `client.rs:297-307`, and `FromHttparse`,
 * `client.rs:309-327`): null while incomplete, else the bytes consumed and the response (body null,
 * version HTTP/1.1, reason phrase dropped).
 * @throws WebSocketException.Protocol [ProtocolError.WrongHttpVersion] for HTTP/1.0, or a parse error.
 * @throws WebSocketException.Capacity more than [MAX_HEADERS] headers.
 * @throws WebSocketException.HttpFormat an invalid header or a status code below 100.
 */
fun tryParseResponse(bytes: ByteArray, offset: Int = 0, length: Int = bytes.size - offset): Pair<Int, ClientResponse>? {
    val raw = ParsedResponse(HeaderSlots(MAX_HEADERS))
    val status = raw.parse(bytes, offset, length)
    if (ParseStatus.isPartial(status)) return null
    if (ParseStatus.isError(status)) throwParseError(status)
    if (raw.version < 1) protocolError(ProtocolError.WrongHttpVersion)
    val headers = headersFrom(raw.headers, bytes)
    val response = Response<ByteArray?>(null)
    response.status = httpFormat { StatusCode.fromU16(raw.code) }
    response.parts.headers = headers
    // httparse only knows HTTP/0.9-1.1, so the only valid version here is 1.1.
    response.version = Version.HTTP_11
    return status to response
}

/**
 * A random `Sec-WebSocket-Key` (`generate_key`, `client.rs:329-335`): 16 random bytes, base64.
 * ⚖️ From the platform CSPRNG (neton-io [secureRandom]; the reference uses `rand::random`), since
 * RFC 6455 §4.1 requires the nonce to be unpredictable (SPEC §3.2).
 */
fun generateKey(): String = Base64.encode(ByteArray(16).also { secureRandom(it) })
