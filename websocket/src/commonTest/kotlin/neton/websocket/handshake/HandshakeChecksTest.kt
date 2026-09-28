package neton.websocket.handshake

import neton.http.Method
import neton.http.Request
import neton.http.Response
import neton.http.StatusCode
import neton.http.Version
import neton.http.h1.parse.HttpParseError
import neton.http.header.HeaderValue
import neton.http.uri.Uri
import neton.io.bytes.Bytes
import neton.websocket.CapacityError
import neton.websocket.ClientRequestBuilder
import neton.websocket.Mode
import neton.websocket.ProtocolError
import neton.websocket.SubProtocolError
import neton.websocket.UrlError
import neton.websocket.WebSocketException
import neton.websocket.assertProtocolError
import neton.websocket.intoClientRequest
import neton.websocket.uriMode
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

/**
 * The handshake rules of SPEC §3.2 / §3.3 that the reference's unit tests do not reach: request
 * conversion and generation, response verification in order, the server's check order, the
 * callback outcomes and the error response.
 */
class HandshakeChecksTest {
    // ---- IntoClientRequest / uri_mode ----

    @Test fun uriRequestHeaders() {
        val r = "ws://user:pass@example.com:8080/chat?room=1".intoClientRequest()
        assertEquals(Method.GET, r.method)
        assertTrue(r.headers["Host"]!!.contentEquals("example.com:8080"))
        assertTrue(r.headers["Connection"]!!.contentEquals("Upgrade"))
        assertTrue(r.headers["Upgrade"]!!.contentEquals("websocket"))
        assertTrue(r.headers["Sec-WebSocket-Version"]!!.contentEquals("13"))
        assertEquals(16, Base64.decode(r.headers["Sec-WebSocket-Key"]!!.asBytes())!!.size)
        assertEquals("/chat?room=1", r.uri.pathAndQuery!!.asStr())
    }

    @Test fun urlErrors() {
        assertEquals(UrlError.NoHostName, assertFailsWith<WebSocketException.Url> { "/just/a/path".intoClientRequest() }.error)
        // An authority ending in `@` is already an invalid URI (as in the `http` crate), so
        // `EmptyHostName` stays a defensive check.
        assertFailsWith<WebSocketException.HttpFormat> { "ws://user@/x".intoClientRequest() }
        assertFailsWith<WebSocketException.HttpFormat> { "ws://exa mple.com/".intoClientRequest() }
        assertEquals(Mode.Plain, uriMode(Uri.parse("ws://a/")))
        assertEquals(Mode.Tls, uriMode(Uri.parse("wss://a/")))
        assertEquals(UrlError.UnsupportedUrlScheme, assertFailsWith<WebSocketException.Url> { uriMode(Uri.parse("http://a/")) }.error)
        // The scheme is checked when the handshake starts.
        assertEquals(UrlError.UnsupportedUrlScheme, assertFailsWith<WebSocketException.Url> { ClientHandshake.start("http://a/".intoClientRequest()) }.error)
    }

    @Test fun startChecksMethodAndVersion() {
        val post = "ws://a/".intoClientRequest().also { it.method = Method.POST }
        assertProtocolError<ProtocolError.WrongHttpMethod> { ClientHandshake.start(post) }
        val old = "ws://a/".intoClientRequest().also { it.version = Version.HTTP_10 }
        assertProtocolError<ProtocolError.WrongHttpVersion> { ClientHandshake.start(old) }
        val h2 = "ws://a/".intoClientRequest().also { it.version = Version.HTTP_2 }
        assertProtocolError<ProtocolError.WrongHttpVersion> { ClientHandshake.start(h2) }
    }

    @Test fun builderAddsHeadersAndSubProtocols() {
        val r = ClientRequestBuilder(Uri.parse("ws://a/s"))
            .withHeader("Authorization", "Bearer t")
            .withSubProtocol("one")
            .withSubProtocol("two")
            .intoClientRequest()
        assertTrue(r.headers["Authorization"]!!.contentEquals("Bearer t"))
        assertTrue(r.headers["Sec-WebSocket-Protocol"]!!.contentEquals("one, two"))
        assertFailsWith<WebSocketException.HttpFormat> { ClientRequestBuilder(Uri.parse("ws://a/")).withHeader("Bad Name", "v").intoClientRequest() }
    }

    // ---- generate_request ----

    @Test fun generateRequestOrderAndRenames() {
        val r = Request.builder()
            .uri("ws://h/p")
            .header("origin", "http://h")
            .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
            .header("upgrade", "websocket")
            .header("sec-websocket-protocol", "chat")
            .header("connection", "Upgrade")
            .header("sec-websocket-version", "13")
            .header("host", "h")
            .body(Unit)
        val (bytes, key) = generateRequest(r)
        assertEquals("dGhlIHNhbXBsZSBub25jZQ==", key)
        val text = bytes.decodeToString()
        assertTrue(text.startsWith("GET /p HTTP/1.1\r\nHost: h\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"), text)
        assertTrue("\r\nOrigin: http://h\r\n" in text, text)
        assertTrue("\r\nSec-WebSocket-Protocol: chat\r\n" in text, text)
        assertTrue(text.endsWith("\r\n\r\n"))
    }

    @Test fun generateRequestMissingHeader() {
        for (missing in listOf("Host", "Connection", "Upgrade", "Sec-WebSocket-Version", "Sec-WebSocket-Key")) {
            val r = "ws://h/".intoClientRequest()
            r.headers.remove(missing)
            val e = assertProtocolError<ProtocolError.InvalidHeader> { generateRequest(r) }
            assertEquals(missing.lowercase(), e.name)
        }
    }

    @Test fun generateRequestNoPath() {
        // Authority-form: no path and query.
        val r = "ws://h/".intoClientRequest().also { it.uri = Uri.parse("h:80") }
        assertEquals(UrlError.NoPathOrQuery, assertFailsWith<WebSocketException.Url> { generateRequest(r) }.error)
    }

    @Test fun generateRequestNonAsciiRequiredHeader() {
        val r = "ws://h/".intoClientRequest()
        r.headers.insert("Host", HeaderValue.fromBytes(byteArrayOf('h'.code.toByte(), 0xe9.toByte())))
        assertFailsWith<WebSocketException.Utf8> { generateRequest(r) }
    }

    // ---- verify_response ----

    private class Setup(subprotocols: String? = null, extensions: String? = null) {
        val request = "ws://h/".intoClientRequest().also { r ->
            subprotocols?.let { r.headers.insert("Sec-WebSocket-Protocol", HeaderValue.fromStr(it)) }
            extensions?.let { r.headers.insert("Sec-WebSocket-Extensions", HeaderValue.fromStr(it)) }
        }
        val accept = deriveAcceptKey(request.headers["Sec-WebSocket-Key"]!!.asBytes())
        val handshake = ClientHandshake.start(request)

        fun response(vararg headers: Pair<String, String>, status: Int = 101): ClientResponse {
            val b = Response.builder().status(status)
            for ((k, v) in headers) b.header(k, v)
            return b.body(null)
        }

        fun good(vararg extra: Pair<String, String>) =
            response("Upgrade" to "websocket", "Connection" to "Upgrade", "Sec-WebSocket-Accept" to accept, *extra)
    }

    @Test fun verifyResponseChecksInOrder() {
        val s = Setup()
        val ok = s.good()
        assertSame(ok, s.handshake.verifyResponse(ok))
        val notUpgraded = s.response(status = 200)
        assertSame(notUpgraded, assertFailsWith<WebSocketException.Http> { s.handshake.verifyResponse(notUpgraded) }.response)
        assertProtocolError<ProtocolError.MissingUpgradeWebSocketHeader> {
            s.handshake.verifyResponse(s.response("Connection" to "x", "Upgrade" to "h2c"))
        }
        assertProtocolError<ProtocolError.MissingConnectionUpgradeHeader> {
            s.handshake.verifyResponse(s.response("Upgrade" to "WebSocket", "Sec-WebSocket-Accept" to "x"))
        }
        assertProtocolError<ProtocolError.SecWebSocketAcceptKeyMismatch> {
            s.handshake.verifyResponse(s.response("Upgrade" to "websocket", "Connection" to "upgrade", "Sec-WebSocket-Accept" to "x"))
        }
    }

    @Test fun subProtocolErrors() {
        fun err(e: ProtocolError) = (e as ProtocolError.SecWebSocketSubProtocolError).error
        val none = Setup()
        assertEquals(SubProtocolError.ServerSentSubProtocolNoneRequested, err(assertProtocolError<ProtocolError.SecWebSocketSubProtocolError> {
            none.handshake.verifyResponse(none.good("Sec-WebSocket-Protocol" to "chat"))
        }))
        val some = Setup(subprotocols = "chat, superchat")
        assertEquals(SubProtocolError.NoSubProtocol, err(assertProtocolError<ProtocolError.SecWebSocketSubProtocolError> {
            some.handshake.verifyResponse(some.good())
        }))
        assertEquals(SubProtocolError.InvalidSubProtocol, err(assertProtocolError<ProtocolError.SecWebSocketSubProtocolError> {
            some.handshake.verifyResponse(some.good("Sec-WebSocket-Protocol" to "other"))
        }))
        some.handshake.verifyResponse(some.good("Sec-WebSocket-Protocol" to "superchat"))
    }

    // ---- parsing ----

    @Test fun responseParsingErrors() {
        assertNull(tryParseResponse("HTTP/1.1 101 Switching".encodeToByteArray()))
        assertProtocolError<ProtocolError.WrongHttpVersion> { tryParseResponse("HTTP/1.0 101 x\r\n\r\n".encodeToByteArray()) }
        assertFailsWith<WebSocketException.HttpFormat> { tryParseResponse("HTTP/1.1 099 x\r\n\r\n".encodeToByteArray()) }
        val e = assertProtocolError<ProtocolError.HttparseError> { tryParseResponse("HTTX/1.1 101 x\r\n\r\n".encodeToByteArray()) }
        assertEquals(HttpParseError.Version, e.error)
        val (size, r) = tryParseResponse("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n\r\nrest".encodeToByteArray())!!
        assertEquals(StatusCode.SWITCHING_PROTOCOLS, r.status)
        assertEquals(Version.HTTP_11, r.version)
        assertNull(r.body)
        assertEquals(56, size)
    }

    @Test fun requestParsingErrors() {
        // The method is checked before the version.
        assertProtocolError<ProtocolError.WrongHttpMethod> { tryParseRequest("POST / HTTP/1.0\r\n\r\n".encodeToByteArray()) }
        assertProtocolError<ProtocolError.WrongHttpVersion> { tryParseRequest("GET / HTTP/1.0\r\n\r\n".encodeToByteArray()) }
        val many = "GET / HTTP/1.1\r\n" + (0..MAX_HEADERS).joinToString("") { "X-$it: v\r\n" } + "\r\n"
        assertEquals(CapacityError.TooManyHeaders, assertFailsWith<WebSocketException.Capacity> { tryParseRequest(many.encodeToByteArray()) }.error)
    }

    // ---- create_parts / create_response ----

    private fun request(vararg headers: Pair<String, String>, method: Method = Method.GET, version: Version = Version.HTTP_11): ServerRequest {
        val b = Request.builder().method(method).version(version).uri("/")
        for ((k, v) in headers) b.header(k, v)
        return b.body(Unit)
    }

    private val valid = arrayOf(
        "Connection" to "keep-alive, Upgrade",
        "Upgrade" to "WebSocket",
        "Sec-WebSocket-Version" to "13",
        "Sec-WebSocket-Key" to "dGhlIHNhbXBsZSBub25jZQ==",
    )

    @Test fun createResponseChecksInOrder() {
        assertProtocolError<ProtocolError.WrongHttpMethod> { createResponse(request(method = Method.POST, version = Version.HTTP_10)) }
        assertProtocolError<ProtocolError.WrongHttpVersion> { createResponse(request(version = Version.HTTP_10)) }
        assertProtocolError<ProtocolError.MissingConnectionUpgradeHeader> { createResponse(request("Connection" to "keep-alive")) }
        assertProtocolError<ProtocolError.MissingUpgradeWebSocketHeader> { createResponse(request("Connection" to "upgrade", "Upgrade" to "websocket2")) }
        assertProtocolError<ProtocolError.MissingSecWebSocketVersionHeader> {
            createResponse(request("Connection" to "Upgrade", "Upgrade" to "websocket", "Sec-WebSocket-Version" to "8"))
        }
        assertProtocolError<ProtocolError.MissingSecWebSocketKey> {
            createResponse(request("Connection" to "Upgrade", "Upgrade" to "websocket", "Sec-WebSocket-Version" to "13"))
        }
        val r = createResponse(request(*valid))
        assertEquals(StatusCode.SWITCHING_PROTOCOLS, r.status)
        assertTrue(r.headers["Sec-WebSocket-Accept"]!!.contentEquals("s3pPLMBiTxaQ9kYGzzhZRbK+xOo="))
        // Connection: split at spaces too.
        createResponse(request("Connection" to "keep-alive Upgrade", *valid.drop(1).toTypedArray()))
        assertEquals("body", createResponseWithBody(request(*valid)) { "body" }.body)
    }

    @Test fun writeResponseFormat() {
        val out = neton.io.bytes.Buffer()
        writeResponse(out, createResponse(request(*valid)))
        assertEquals(
            "HTTP/1.1 101 Switching Protocols\r\nconnection: Upgrade\r\nupgrade: websocket\r\nsec-websocket-accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n",
            out.readAll().decodeToString(),
        )
        val bad = createResponse(request(*valid)).also { it.headers.insert("X", HeaderValue.fromBytes(byteArrayOf(0xe9.toByte()))) }
        assertFailsWith<WebSocketException.Utf8> { writeResponse(neton.io.bytes.Buffer(), bad) }
    }

    // ---- ServerHandshake (callback, junk, error response) ----

    @Test fun junkAfterRequest() {
        assertProtocolError<ProtocolError.JunkAfterRequest> { ServerHandshake().reply(request(*valid), Bytes.copyOf(byteArrayOf(1))) }
        // Junk is checked before the request itself.
        assertProtocolError<ProtocolError.JunkAfterRequest> { ServerHandshake().reply(request(), Bytes.copyOf(byteArrayOf(1))) }
    }

    @Test fun callbackAcceptsWithHeaders() {
        var seenPath: String? = null
        val reply = ServerHandshake { req, resp ->
            seenPath = req.uri.path
            resp.headers.append("Sec-WebSocket-Protocol", HeaderValue.fromStr("chat"))
            CallbackResult.Accept(resp)
        }.reply(request(*valid), Bytes.EMPTY)
        assertEquals("/", seenPath)
        assertNull(reply.error)
        assertTrue("\r\nsec-websocket-protocol: chat\r\n" in reply.bytes.decodeToString())
    }

    @Test fun callbackRejectsWithBody() {
        val reply = ServerHandshake { _, _ ->
            CallbackResult.Reject(Response.builder().status(403).header("X-Why", "nope").body<String?>("go away"))
        }.reply(request(*valid), Bytes.EMPTY)
        assertEquals("HTTP/1.1 403 Forbidden\r\nx-why: nope\r\n\r\ngo away", reply.bytes.decodeToString())
        val e = reply.error!!
        assertEquals(StatusCode.FORBIDDEN, e.response.status)
        assertContentEquals("go away".encodeToByteArray(), e.response.body)

        val noBody = ServerHandshake { _, _ -> CallbackResult.Reject(Response.builder().status(404).body<String?>(null)) }
            .reply(request(*valid), Bytes.EMPTY)
        assertEquals("HTTP/1.1 404 Not Found\r\n\r\n", noBody.bytes.decodeToString())
        assertNull(noBody.error!!.response.body)
    }

    @Test fun callbackSuccessfulErrorResponse() {
        assertProtocolError<ProtocolError.CustomResponseSuccessful> {
            ServerHandshake { _, _ -> CallbackResult.Reject(Response.builder().status(200).body<String?>(null)) }.reply(request(*valid), Bytes.EMPTY)
        }
    }

    @Test fun invalidRequestSkipsCallback() {
        var called = false
        assertProtocolError<ProtocolError.MissingSecWebSocketKey> {
            ServerHandshake { _, r -> called = true; CallbackResult.Accept(r) }
                .reply(request("Connection" to "Upgrade", "Upgrade" to "websocket", "Sec-WebSocket-Version" to "13"), Bytes.EMPTY)
        }
        assertTrue(!called)
    }
}
