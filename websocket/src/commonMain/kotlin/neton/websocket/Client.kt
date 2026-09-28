package neton.websocket

import neton.http.Request
import neton.http.RequestParts
import neton.http.h1.parse.ParsedRequest
import neton.http.header.HeaderName
import neton.http.header.HeaderValue
import neton.http.uri.Uri
import neton.io.bytes.Buffer
import neton.io.bytes.Bytes
import neton.io.core.IoException
import neton.io.core.IoStream
import neton.io.net.ConnectException
import neton.io.net.SocketOptions
import neton.websocket.handshake.ClientHandshake
import neton.websocket.handshake.ClientRequest
import neton.websocket.handshake.ClientResponse
import neton.websocket.handshake.HandshakeLimits
import neton.websocket.handshake.HeadReader
import neton.websocket.handshake.TryParse
import neton.websocket.handshake.generateKey
import neton.websocket.handshake.httpFormat
import neton.websocket.handshake.requestFromParsed
import neton.websocket.handshake.toStrOrThrow
import neton.websocket.handshake.tryParseResponse
import neton.io.net.connect as tcpConnect

// Client entry points (tungstenite `T/src/client.rs`, tokio-tungstenite `TT/src/connect.rs`; SPEC §3.2, §6).

/** Plain or TLS connection, from the URI scheme (tungstenite `stream::Mode`). */
enum class Mode { Plain, Tls }

/**
 * The mode of [uri] (`uri_mode`): `ws` is [Mode.Plain], `wss` is [Mode.Tls].
 * @throws WebSocketException.Url [UrlError.UnsupportedUrlScheme] for any other scheme.
 */
fun uriMode(uri: Uri): Mode = when (uri.schemeStr) {
    "ws" -> Mode.Plain
    "wss" -> Mode.Tls
    else -> throw WebSocketException.Url(UrlError.UnsupportedUrlScheme)
}

/**
 * Something that can become a client handshake request (tungstenite `IntoClientRequest`). Strings,
 * [Uri]s, [ParsedRequest]s and requests convert with the `intoClientRequest()` extensions below;
 * [ClientRequestBuilder] implements this interface.
 */
fun interface IntoClientRequest {
    fun intoClientRequest(): ClientRequest
}

/** Parse a `ws://` / `wss://` URL and build the request for it (`IntoClientRequest for &str`). */
fun String.intoClientRequest(): ClientRequest = httpFormat { Uri.parse(this) }.intoClientRequest()

/**
 * The request for this URI (`IntoClientRequest for Uri`): `GET`, with `Host` (the authority
 * without `user:pass@`), `Connection: Upgrade`, `Upgrade: websocket`, `Sec-WebSocket-Version: 13`
 * and a fresh `Sec-WebSocket-Key`.
 * @throws WebSocketException.Url [UrlError.NoHostName] without an authority,
 *   [UrlError.EmptyHostName] when the host is empty.
 */
fun Uri.intoClientRequest(): ClientRequest {
    val authority = authority?.asStr() ?: throw WebSocketException.Url(UrlError.NoHostName)
    val host = authority.substringAfter('@')
    if (host.isEmpty()) throw WebSocketException.Url(UrlError.EmptyHostName)
    return httpFormat {
        Request.builder()
            .method("GET")
            .header("Host", host)
            .header("Connection", "Upgrade")
            .header("Upgrade", "websocket")
            .header("Sec-WebSocket-Version", "13")
            .header("Sec-WebSocket-Key", generateKey())
            .uri(this)
            .body(Unit)
    }
}

/** A request is used as it is: no header or URL is added or changed (`IntoClientRequest for Request`). */
fun ClientRequest.intoClientRequest(): ClientRequest = this

/** A parsed request head (`IntoClientRequest for httparse::Request`); it must be a complete `GET`. */
fun ParsedRequest.intoClientRequest(): ClientRequest = requestFromParsed(this)

/**
 * Builds a request from a URI with extra headers and sub-protocols (tungstenite
 * `ClientRequestBuilder`, `client.rs:268-330`).
 *
 * ```
 * val request = ClientRequestBuilder(Uri.parse("ws://localhost:3012/socket"))
 *     .withHeader("Authorization", "Bearer $token")
 *     .withSubProtocol("my_sub_protocol")
 * ```
 */
class ClientRequestBuilder(private val uri: Uri) : IntoClientRequest {
    private val additionalHeaders = mutableListOf<Pair<String, String>>()
    private val subprotocols = mutableListOf<String>()

    /** Add a header to the handshake request. */
    fun withHeader(key: String, value: String): ClientRequestBuilder = apply { additionalHeaders += key to value }

    /** Add a sub-protocol to `Sec-WebSocket-Protocol` (several are joined with `", "`). */
    fun withSubProtocol(protocol: String): ClientRequestBuilder = apply { subprotocols += protocol }

    override fun intoClientRequest(): ClientRequest {
        val request = uri.intoClientRequest()
        val headers = request.headers
        httpFormat {
            for ((k, v) in additionalHeaders) headers.append(HeaderName.fromStr(k), HeaderValue.fromStr(v))
            if (subprotocols.isNotEmpty()) headers.append("Sec-WebSocket-Protocol", HeaderValue.fromStr(subprotocols.joinToString(", ")))
        }
        return request
    }
}

/**
 * A completed client handshake.
 *
 * @property stream the stream the handshake ran on (for [connect], the one it opened, possibly TLS).
 * @property core the protocol core for the connection (`Role.Client`), already holding [leftover]
 *   as its first input (`from_partially_read`).
 * @property response the server's response (body null).
 * @property leftover bytes that came after the response head (the start of the WebSocket stream).
 */
class ClientHandshakeResult(
    val stream: IoStream,
    val core: WebSocketCore,
    val response: ClientResponse,
    val leftover: Bytes,
)

/**
 * Wraps a connected TCP stream in TLS for `wss` (the caller's TLS library; this library has none,
 * SPEC §0). [domain] is the host of the URL, for SNI and certificate checks.
 */
fun interface TlsConnector {
    suspend fun connect(stream: IoStream, domain: String): IoStream
}

/**
 * Do the client handshake over [stream] (tungstenite `client_with_config`, tokio-tungstenite
 * `client_async_with_config`): send the request, read and verify the response.
 *
 * ⚖️ A suspend function completes the handshake: there is no `Interrupted` / `MidHandshake`
 * (SPEC §2). The caller keeps ownership of [stream]: it is not closed on failure.
 *
 * @throws WebSocketException.Http the server answered with another status; the body holds the bytes
 *   read after the head.
 * @throws WebSocketException for an invalid request or response, a limit of [limits], or an I/O error.
 */
suspend fun clientHandshake(
    stream: IoStream,
    request: ClientRequest,
    config: WebSocketConfig? = null,
    limits: HandshakeLimits = HandshakeLimits(),
): ClientHandshakeResult {
    val handshake = ClientHandshake.start(request)
    writeAndFlush(stream, handshake.request)
    val (response, tail) = readHead(stream, limits, TryParse(::tryParseResponse))
    val verified = try {
        handshake.verifyResponse(response)
    } catch (e: WebSocketException.Http) {
        e.response.body = tail.toByteArray()
        throw e
    }
    val core = WebSocketCore(Role.Client, config ?: WebSocketConfig(), tail.takeUnless { it.isEmpty })
    return ClientHandshakeResult(stream, core, verified, tail)
}

/** [clientHandshake] for a URL. */
suspend fun clientHandshake(stream: IoStream, url: String, config: WebSocketConfig? = null, limits: HandshakeLimits = HandshakeLimits()) =
    clientHandshake(stream, url.intoClientRequest(), config, limits)

/** [clientHandshake] for a URI. */
suspend fun clientHandshake(stream: IoStream, uri: Uri, config: WebSocketConfig? = null, limits: HandshakeLimits = HandshakeLimits()) =
    clientHandshake(stream, uri.intoClientRequest(), config, limits)

/** [clientHandshake] for a [ClientRequestBuilder] or another [IntoClientRequest]. */
suspend fun clientHandshake(stream: IoStream, request: IntoClientRequest, config: WebSocketConfig? = null, limits: HandshakeLimits = HandshakeLimits()) =
    clientHandshake(stream, request.intoClientRequest(), config, limits)

/**
 * Connect to a `ws://` or `wss://` URL and do the handshake (tungstenite `connect_with_config`,
 * tokio-tungstenite `connect_async_with_config`; SPEC §3.2).
 *
 * - The host (IPv6 brackets removed) is resolved and each address tried in turn (neton-io
 *   `connect`); when none connects: [UrlError.UnableToConnect].
 * - The port defaults to 80 for `ws` and 443 for `wss`.
 * - `wss` needs [tlsConnector]; without one: [UrlError.TlsNotAvailable] (the reference's
 *   `TlsFeatureNotEnabled`), before anything is connected.
 * - ⚖️ [maxRedirects] defaults to 0, like `connect_async` (the blocking `connect` follows 3): a 3xx
 *   response with a `Location` is followed that many times, with the same headers.
 * - ⚖️ `TCP_NODELAY` follows [socketOptions] (neton-io's default: on), like the blocking reference;
 *   `connect_async` leaves it off unless asked.
 * - ⚖️ A name that does not resolve is also [UrlError.UnableToConnect] (the reference: an I/O
 *   error), because neton-io's `connect` reports both the same way.
 *
 * On failure the connection opened here is closed.
 */
suspend fun connect(
    request: ClientRequest,
    config: WebSocketConfig? = null,
    maxRedirects: Int = 0,
    tlsConnector: TlsConnector? = null,
    socketOptions: SocketOptions = SocketOptions.Default,
    limits: HandshakeLimits = HandshakeLimits(),
): ClientHandshakeResult {
    require(maxRedirects >= 0) { "maxRedirects must not be negative" }
    val parts = request.parts
    var uri = parts.uri
    var attempt = 0
    while (true) {
        // The handshake consumes the request's headers: give each attempt its own copy.
        val next = Request(RequestParts(parts.method, uri, parts.version, parts.headers.clone()), Unit)
        try {
            return tryConnect(next, config, tlsConnector, socketOptions, limits)
        } catch (e: WebSocketException.Http) {
            if (!e.response.status.isRedirection() || attempt >= maxRedirects) throw e
            val location = e.response.headers["Location"] ?: throw e
            uri = httpFormat { Uri.parse(location.toStrOrThrow()) }
            attempt++
        }
    }
}

/** [connect] to a URL. */
suspend fun connect(
    url: String,
    config: WebSocketConfig? = null,
    maxRedirects: Int = 0,
    tlsConnector: TlsConnector? = null,
    socketOptions: SocketOptions = SocketOptions.Default,
    limits: HandshakeLimits = HandshakeLimits(),
) = connect(url.intoClientRequest(), config, maxRedirects, tlsConnector, socketOptions, limits)

/** [connect] to a URI. */
suspend fun connect(
    uri: Uri,
    config: WebSocketConfig? = null,
    maxRedirects: Int = 0,
    tlsConnector: TlsConnector? = null,
    socketOptions: SocketOptions = SocketOptions.Default,
    limits: HandshakeLimits = HandshakeLimits(),
) = connect(uri.intoClientRequest(), config, maxRedirects, tlsConnector, socketOptions, limits)

/** [connect] with a [ClientRequestBuilder] or another [IntoClientRequest]. */
suspend fun connect(
    request: IntoClientRequest,
    config: WebSocketConfig? = null,
    maxRedirects: Int = 0,
    tlsConnector: TlsConnector? = null,
    socketOptions: SocketOptions = SocketOptions.Default,
    limits: HandshakeLimits = HandshakeLimits(),
) = connect(request.intoClientRequest(), config, maxRedirects, tlsConnector, socketOptions, limits)

/** One connection and handshake (`try_client_handshake`, `client.rs:44-76`). */
private suspend fun tryConnect(
    request: ClientRequest,
    config: WebSocketConfig?,
    tlsConnector: TlsConnector?,
    socketOptions: SocketOptions,
    limits: HandshakeLimits,
): ClientHandshakeResult {
    val uri = request.uri
    val mode = uriMode(uri)
    if (mode == Mode.Tls && tlsConnector == null) throw WebSocketException.Url(UrlError.TlsNotAvailable)
    val rawHost = uri.host ?: throw WebSocketException.Url(UrlError.NoHostName)
    val host = if (rawHost.startsWith('[')) rawHost.substring(1, rawHost.length - 1) else rawHost
    val port = uri.portU16 ?: if (mode == Mode.Plain) 80 else 443

    val tcp = try {
        tcpConnect(host, port, socketOptions)
    } catch (_: ConnectException) {
        throw WebSocketException.Url(UrlError.UnableToConnect(uri.toString()))
    }
    var stream = tcp
    try {
        if (tlsConnector != null && mode == Mode.Tls) stream = tlsConnector.connect(tcp, host)
        return clientHandshake(stream, request, config, limits)
    } catch (e: Throwable) {
        stream.close()
        if (stream !== tcp) tcp.close()
        throw e
    }
}

// ---- shared by the client and server entry points ----

/** Write all of [bytes] and flush (the handshake machine's writing and flushing states). */
internal suspend fun writeAndFlush(stream: IoStream, bytes: ByteArray) = io {
    stream.write(Buffer(bytes.size).also { it.writeBytes(bytes) })
    stream.flush()
}

/** Read one HTTP head (the handshake machine's reading state; see [HeadReader]). */
internal suspend fun <T> readHead(stream: IoStream, limits: HandshakeLimits, parser: TryParse<T>): Pair<T, Bytes> {
    val reader = HeadReader(limits, parser)
    while (true) {
        reader.prepareRead()
        val n = io { stream.read(reader.input) }
        if (n < 0) reader.receivedEof()
        reader.received(n)?.let { return it }
    }
}

/** An I/O error of the stream is [WebSocketException.Io]. */
private inline fun <R> io(block: () -> R): R = try {
    block()
} catch (e: IoException) {
    throw WebSocketException.Io(e)
}
