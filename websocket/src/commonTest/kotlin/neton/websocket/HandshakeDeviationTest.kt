package neton.websocket

import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import neton.http.Response
import neton.http.StatusCode
import neton.http.h1.parse.HttpParseError
import neton.http.header.HeaderValue
import neton.io.bytes.Buffer
import neton.io.core.IoStream
import neton.io.core.memoryStreamPair
import neton.io.net.runReactor
import neton.websocket.handshake.Base64
import neton.websocket.handshake.CallbackResult
import neton.websocket.handshake.ClientHandshake
import neton.websocket.handshake.HandshakeLimits
import neton.websocket.handshake.HeadReader
import neton.websocket.handshake.TryParse
import neton.websocket.handshake.deriveAcceptKey
import neton.websocket.handshake.generateKey
import neton.websocket.handshake.tryParseRequest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/** One test (or more) per ⚖️ item of SPEC §3 (and §2's handshake row). */
class HandshakeDeviationTest {
    private fun setup(extensions: String? = null): Triple<ClientHandshake, String, (Array<out Pair<String, String>>) -> Response<ByteArray?>> {
        val request = "ws://h/".intoClientRequest()
        extensions?.let { request.headers.insert("Sec-WebSocket-Extensions", HeaderValue.fromStr(it)) }
        val accept = deriveAcceptKey(request.headers["Sec-WebSocket-Key"]!!.asBytes())
        val hs = ClientHandshake.start(request)
        return Triple(hs, accept) { headers ->
            val b = Response.builder().status(101).header("Upgrade", "websocket").header("Sec-WebSocket-Accept", accept)
            for ((k, v) in headers) b.header(k, v)
            b.body<ByteArray?>(null)
        }
    }

    /** ⚖️ §3.2: the client reads `Connection` as a token list (the reference: the whole value). */
    @Test fun connectionHeaderIsATokenList() {
        val (hs, _, response) = setup()
        hs.verifyResponse(response(arrayOf("Connection" to "keep-alive, Upgrade")))
        hs.verifyResponse(response(arrayOf("Connection" to "UPGRADE")))
        assertProtocolError<ProtocolError.MissingConnectionUpgradeHeader> { hs.verifyResponse(response(arrayOf("Connection" to "keep-alive"))) }
        assertProtocolError<ProtocolError.MissingConnectionUpgradeHeader> { hs.verifyResponse(response(arrayOf("Connection" to "Upgraded"))) }
    }

    /** ⚖️ §3.2: an extension in the response that the client did not offer fails the handshake. */
    @Test fun unrequestedExtensionIsRejected() {
        val (hs, _, response) = setup()
        val e = assertProtocolError<ProtocolError.ExtensionNotRequested> {
            hs.verifyResponse(response(arrayOf("Connection" to "Upgrade", "Sec-WebSocket-Extensions" to "permessage-deflate; client_max_window_bits")))
        }
        assertEquals("permessage-deflate", e.extension)
        // An empty value names no extension.
        hs.verifyResponse(response(arrayOf("Connection" to "Upgrade", "Sec-WebSocket-Extensions" to "")))
    }

    /** ⚖️ §3.2: an extension the client offered (added by hand) may be accepted by the server. */
    @Test fun offeredExtensionIsAllowed() {
        val (hs, _, response) = setup(extensions = "permessage-deflate; client_max_window_bits, x-other")
        hs.verifyResponse(response(arrayOf("Connection" to "Upgrade", "Sec-WebSocket-Extensions" to "Permessage-Deflate")))
        val e = assertProtocolError<ProtocolError.ExtensionNotRequested> {
            hs.verifyResponse(response(arrayOf("Connection" to "Upgrade", "Sec-WebSocket-Extensions" to "permessage-deflate, x-third")))
        }
        assertEquals("x-third", e.extension)
    }

    /** ⚖️ §3.2: the key is 16 bytes from the CSPRNG; keys do not repeat. */
    @Test fun generateKeyUsesSecureRandom() {
        val keys = HashSet<String>()
        repeat(1000) {
            val k = generateKey()
            assertEquals(16, Base64.decode(k.encodeToByteArray())!!.size)
            keys += k
        }
        assertEquals(1000, keys.size)
    }

    /** ⚖️ §3.1: the head is parsed once, when its end has arrived, not after every read. */
    @Test fun incrementalParsingParsesOnce() {
        val request = ("GET / HTTP/1.1\r\nHost: h\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n" +
            "Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n").encodeToByteArray()
        // 3 bytes per read: every CR LF CR LF split is exercised; no attack limit is hit (threshold off).
        val reader = HeadReader(HandshakeLimits(minPacketCheckThreshold = 10_000), TryParse(::tryParseRequest))
        var result: Any? = null
        var i = 0
        while (result == null) {
            val n = minOf(3, request.size - i)
            reader.prepareRead()
            reader.input.writeBytes(request, i, n)
            i += n
            result = reader.received(n)
        }
        assertEquals(request.size, i)
        assertEquals(1, reader.parses)
    }

    /** ⚖️ §3.1: an error in an unfinished head is still reported, at end of stream. */
    @Test fun incrementalParsingReportsErrorsAtEof() {
        val reader = HeadReader(HandshakeLimits(), TryParse(::tryParseRequest))
        val bad = "GET / HTTP/1.1\r\nBad Header: x\r\n".encodeToByteArray()
        reader.prepareRead()
        reader.input.writeBytes(bad)
        assertEquals(null, reader.received(bad.size))
        val e = assertProtocolError<ProtocolError.HttparseError> { reader.receivedEof() }
        assertEquals(HttpParseError.HeaderName, e.error)
    }

    /** ⚖️ §3.1: the attack limits are configurable (the reference hard-codes them). */
    @Test fun handshakeLimitsAreConfigurable() = runReactor {
        val (c, s) = memoryStreamPair()
        coroutineScope {
            launch { runCatching { clientHandshake(c, "ws://h/") } }
            assertFailsWith<WebSocketException.AttackAttempt> { acceptWithConfig(s, null, HandshakeLimits(maxBytes = 50)) }
            c.close(); s.close()
        }
    }

    /**
     * ⚖️ §2: a suspend function replaces `HandshakeError::Interrupted` / `MidHandshake`: with a
     * one-byte pipe every read and write suspends, and the handshake still completes.
     */
    @Test fun handshakeSuspendsInsteadOfInterrupted() = runReactor {
        val (c, s) = memoryStreamPair(capacity = 1)
        coroutineScope {
            // One byte per read would trip the slow-loris check: turn it off for this test.
            val limits = HandshakeLimits(minPacketCheckThreshold = 10_000)
            val server = async { acceptWithConfig(s, null, limits) }
            val client = clientHandshake(c, "ws://h/", limits = limits)
            assertEquals(StatusCode.SWITCHING_PROTOCOLS, client.response.status)
            assertEquals(Role.Server, server.await().role)
            c.close(); s.close()
        }
    }

    /** ⚖️ (inherited from the http library, its SPEC §3.9) a bare LF line ending is rejected; httparse accepts it. */
    @Test fun bareLfIsRejected() {
        val e = assertProtocolError<ProtocolError.HttparseError> {
            tryParseRequest("GET / HTTP/1.1\nHost: h\n\n".encodeToByteArray())
        }
        assertEquals(HttpParseError.NewLine, e.error)
    }

    /** A plain HTTP server on [listener] answering every request with [status] and [location]. */
    private suspend fun redirectOnce(listener: neton.io.net.TcpListener, status: Int, location: String) {
        val stream: IoStream = listener.accept()
        val buf = Buffer()
        do stream.read(buf) while (!buf.backingArray().decodeToString(buf.readerIndex(), buf.writerIndex()).endsWith("\r\n\r\n"))
        val out = Buffer()
        out.writeBytes("HTTP/1.1 $status Moved\r\nLocation: $location\r\nContent-Length: 0\r\n\r\n".encodeToByteArray())
        stream.write(out)
        stream.close()
    }

    /** ⚖️ §3.2: `maxRedirects` defaults to 0 (like `connect_async`); when set, a 3xx is followed. */
    @Test fun redirectsAreNotFollowedByDefault() = runReactor {
        val (redirector, port) = listenLoopback()
        val (target, targetPort) = listenLoopback()
        coroutineScope {
            launch { redirectOnce(redirector, 302, "ws://127.0.0.1:$targetPort/there") }
            val e = assertFailsWith<WebSocketException.Http> { connect("ws://127.0.0.1:$port/here") }
            assertEquals(StatusCode.FOUND, e.response.status)

            launch { redirectOnce(redirector, 302, "ws://127.0.0.1:$targetPort/there") }
            launch {
                val stream = target.accept()
                acceptHdr(stream) { req, r -> assertEquals("/there", req.uri.path); CallbackResult.Accept(r) }
                stream.close()
            }
            val ok = connect("ws://127.0.0.1:$port/here", maxRedirects = 1)
            assertEquals(StatusCode.SWITCHING_PROTOCOLS, ok.second.status)
            ok.first.abort()
            redirector.close(); target.close()
        }
    }

    /** ⚖️ §3.2: a name that does not resolve is `UnableToConnect` (the reference: an I/O error). */
    @Test fun unresolvableHostIsUnableToConnect() = runReactor {
        // A name with an empty label fails in the resolver itself: a fake-IP DNS proxy (as on the
        // development machine) answers every well-formed name.
        val e = assertFailsWith<WebSocketException.Url> { connect("ws://no-such-host..invalid/") }
        assertEquals(UrlError.UnableToConnect("ws://no-such-host..invalid/"), e.error)
    }

    /** §3.2: `wss` goes through the caller's TLS connector (here a pass-through stand-in). */
    @Test fun wssUsesTheTlsConnector() = runReactor {
        val (listener, port) = listenLoopback()
        var domain: String? = null
        coroutineScope {
            launch { accept(listener.accept()).abort() }
            val hs = connect("wss://127.0.0.1:$port/", tlsConnector = TlsConnector { stream, d -> domain = d; stream })
            assertEquals("127.0.0.1", domain)
            hs.first.abort()
            listener.close()
        }
        assertTrue(domain != null)
    }
}
