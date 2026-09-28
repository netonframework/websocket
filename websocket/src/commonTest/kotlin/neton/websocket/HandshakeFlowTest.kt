package neton.websocket

import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import neton.http.Response
import neton.http.StatusCode
import neton.http.header.HeaderValue
import neton.http.uri.Uri
import neton.io.core.IoStream
import neton.io.core.memoryStreamPair
import neton.io.net.runReactor
import neton.websocket.handshake.CallbackResult
import neton.websocket.handshake.ClientRequest
import neton.websocket.handshake.ServerRequest
import neton.websocket.handshake.ServerResponse
import neton.websocket.handshake.generateKey
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * Handshake flows over a stream (SPEC §7): `T/tests/handshake.rs` (6), `client_headers.rs` (1),
 * `url_feature.rs` (1), `wss_fails_when_no_tls.rs` (1) and `TT/tests/handshakes.rs` (1). Each
 * stream test runs over a neton-io `memoryStreamPair` and over loopback TCP (the `...Tcp` twin),
 * where the reference only uses TCP.
 */
class HandshakeFlowTest {
    // ---- T/tests/handshake.rs ----

    private fun createHttpRequest(uri: String, subprotocols: List<String>?): ClientRequest {
        val u = Uri.parse(uri)
        val authority = u.authority!!.asStr()
        val host = authority.substringAfter('@')
        check(host.isNotEmpty()) { "Empty host name" }
        val builder = neton.http.Request.builder()
            .method("GET")
            .header("Host", host)
            .header("Connection", "Upgrade")
            .header("Upgrade", "websocket")
            .header("Sec-WebSocket-Version", "13")
            .header("Sec-WebSocket-Key", generateKey())
        if (subprotocols != null) builder.header("Sec-WebSocket-Protocol", subprotocols.joinToString(", "))
        return builder.uri(u).body(Unit)
    }

    /** The server of `server_thread`: offer [serverSubprotocols], accept, send close. */
    private suspend fun serve(stream: IoStream, serverSubprotocols: List<String>?) {
        val core = acceptHdr(stream) { _: ServerRequest, response: ServerResponse ->
            if (serverSubprotocols != null) {
                response.headers.append("Sec-WebSocket-Protocol", HeaderValue.fromStr(serverSubprotocols.joinToString(",")))
            }
            CallbackResult.Accept(response)
        }
        try { CoreConnection(stream, core).close() } catch (_: Exception) { } finally { stream.close() }
    }

    /** Run the server over the given transport and return what the client's handshake gave. */
    private fun subprotocolCase(
        tcp: Boolean,
        clientSubprotocols: List<String>?,
        serverSubprotocols: List<String>?,
        check: (Result<ClientHandshakeResult>) -> Unit,
    ) = runReactor {
        if (tcp) {
            val (listener, port) = listenLoopback()
            coroutineScope {
                launch { serve(listener.accept(), serverSubprotocols); listener.close() }
                val result = runCatching { connect(createHttpRequest("ws://127.0.0.1:$port", clientSubprotocols)) }
                result.getOrNull()?.stream?.close()
                check(result)
            }
        } else {
            val (c, s) = memoryStreamPair()
            coroutineScope {
                launch { serve(s, serverSubprotocols) }
                val result = runCatching { clientHandshake(c, createHttpRequest("ws://127.0.0.1:3012", clientSubprotocols)) }
                c.close()
                check(result)
            }
        }
    }

    private fun subProtocolError(r: Result<ClientHandshakeResult>): SubProtocolError {
        val e = r.exceptionOrNull()
        assertTrue(e is WebSocketException.Protocol, "expected a protocol error, got $e")
        val p = e.error
        assertTrue(p is ProtocolError.SecWebSocketSubProtocolError, "got $p")
        return p.error
    }

    private fun protocol(r: Result<ClientHandshakeResult>): String =
        r.getOrThrow().response.headers["Sec-WebSocket-Protocol"]!!.toStr()

    private fun testServerSendNoSubprotocol(tcp: Boolean) = subprotocolCase(tcp, listOf("my-sub-protocol"), null) {
        assertEquals(SubProtocolError.NoSubProtocol, subProtocolError(it))
    }

    private fun testServerSentSubprotocolNoneRequested(tcp: Boolean) = subprotocolCase(tcp, null, listOf("my-sub-protocol")) {
        assertEquals(SubProtocolError.ServerSentSubProtocolNoneRequested, subProtocolError(it))
    }

    private fun testInvalidSubprotocol(tcp: Boolean) = subprotocolCase(tcp, listOf("my-sub-protocol"), listOf("invalid-sub-protocol")) {
        assertEquals(SubProtocolError.InvalidSubProtocol, subProtocolError(it))
    }

    private fun testRequestMultipleSubprotocols(tcp: Boolean) =
        subprotocolCase(tcp, listOf("my-sub-protocol", "my-sub-protocol-1", "my-sub-protocol-2"), listOf("my-sub-protocol")) {
            assertEquals("my-sub-protocol", protocol(it))
        }

    private fun testRequestMultipleSubprotocolsWithInitialUnknown(tcp: Boolean) =
        subprotocolCase(tcp, listOf("protocol-unknown-to-server", "my-sub-protocol"), listOf("my-sub-protocol")) {
            assertEquals("my-sub-protocol", protocol(it))
        }

    private fun testRequestSingleSubprotocol(tcp: Boolean) = subprotocolCase(tcp, listOf("my-sub-protocol"), listOf("my-sub-protocol")) {
        assertEquals("my-sub-protocol", protocol(it))
    }

    @Test fun testServerSendNoSubprotocol() = testServerSendNoSubprotocol(tcp = false)
    @Test fun testServerSendNoSubprotocolTcp() = testServerSendNoSubprotocol(tcp = true)
    @Test fun testServerSentSubprotocolNoneRequested() = testServerSentSubprotocolNoneRequested(tcp = false)
    @Test fun testServerSentSubprotocolNoneRequestedTcp() = testServerSentSubprotocolNoneRequested(tcp = true)
    @Test fun testInvalidSubprotocol() = testInvalidSubprotocol(tcp = false)
    @Test fun testInvalidSubprotocolTcp() = testInvalidSubprotocol(tcp = true)
    @Test fun testRequestMultipleSubprotocols() = testRequestMultipleSubprotocols(tcp = false)
    @Test fun testRequestMultipleSubprotocolsTcp() = testRequestMultipleSubprotocols(tcp = true)
    @Test fun testRequestMultipleSubprotocolsWithInitialUnknown() = testRequestMultipleSubprotocolsWithInitialUnknown(tcp = false)
    @Test fun testRequestMultipleSubprotocolsWithInitialUnknownTcp() = testRequestMultipleSubprotocolsWithInitialUnknown(tcp = true)
    @Test fun testRequestSingleSubprotocol() = testRequestSingleSubprotocol(tcp = false)
    @Test fun testRequestSingleSubprotocolTcp() = testRequestSingleSubprotocol(tcp = true)

    // ---- T/tests/client_headers.rs ----

    private fun testHeaders(tcp: Boolean) = runReactor {
        val token = "my_jwt_token"
        val fullToken = "Bearer $token"
        val subProtocol = "my_sub_protocol"
        val (listener, port) = if (tcp) listenLoopback() else null to 3013
        val builder = ClientRequestBuilder(Uri.parse("ws://127.0.0.1:$port/socket"))
            .withHeader("Authorization", fullToken)
            .withSubProtocol(subProtocol)
        val (clientEnd, serverEnd) = if (tcp) null to null else memoryStreamPair()

        coroutineScope {
            val client = async {
                val hs = if (tcp) connect(builder) else clientHandshake(clientEnd!!, builder)
                val conn = hs.connection()
                conn.send(Message.text("Hello WebSocket"))
                assertTrue(conn.read().isClose) // receive close from server
                assertFailsWith<WebSocketException.ConnectionClosed> { conn.read() } // now we should get ConnectionClosed
                hs.stream.close()
            }

            val stream = if (tcp) listener!!.accept() else serverEnd!!
            val core = acceptHdr(stream) { req, response ->
                assertEquals("/socket", req.uri.path)
                req.headers.forEach { name, value ->
                    if (name.asStr() == "authorization") {
                        assertEquals(fullToken, value.toStr())
                    } else if (name.asStr() == "sec-websocket-protocol") {
                        assertEquals(subProtocol, value.toStr())
                        // the server needs to respond with the same sub-protocol
                        response.headers.append("sec-websocket-protocol", HeaderValue.fromStr(subProtocol))
                    }
                }
                CallbackResult.Accept(response)
            }
            val handler = CoreConnection(stream, core)
            handler.close() // send close to client

            // This read should succeed even though we already initiated a close
            assertEquals("Hello WebSocket", handler.read().intoData().decodeToString())
            assertTrue(handler.read().isClose) // receive acknowledgement
            assertFailsWith<WebSocketException.ConnectionClosed> { handler.read() } // now we should get ConnectionClosed
            stream.close()
            client.await()
            listener?.close()
        }
    }

    @Test fun testHeaders() = testHeaders(tcp = false)
    @Test fun testHeadersTcp() = testHeaders(tcp = true)

    // ---- T/tests/url_feature.rs (the `url::Url` type becomes neton.http's Uri) ----

    @Test fun testWithUrl() = runReactor {
        val (listener, port) = listenLoopback()
        val url = Uri.parse("ws://127.0.0.1:$port")
        coroutineScope {
            val client = async { connect(url) }
            val stream = listener.accept()
            CoreConnection(stream, acceptHdr(stream) { _, r -> CallbackResult.Accept(r) }).close()
            client.await().stream.close()
            stream.close()
            listener.close()
        }
    }

    // ---- T/tests/wss_fails_when_no_tls.rs ----

    @Test fun wssUrlFailsWhenNoTlsSupport() = runReactor {
        val e = assertFailsWith<WebSocketException.Url> { connect("wss://127.0.0.1/ws") }
        assertEquals(UrlError.TlsNotAvailable, e.error)
    }

    // ---- TT/tests/handshakes.rs ----

    private fun handshakes(tcp: Boolean) = runReactor {
        if (tcp) {
            val (listener, port) = listenLoopback()
            coroutineScope {
                launch {
                    val connection = listener.accept()
                    accept(connection) // "Failed to handshake with connection"
                    connection.close()
                    listener.close()
                }
                val tcpStream = neton.io.net.connect("127.0.0.1", port)
                clientHandshake(tcpStream, "ws://localhost:$port/")
                tcpStream.close()
            }
        } else {
            val (c, s) = memoryStreamPair()
            coroutineScope {
                launch { accept(s); s.close() }
                clientHandshake(c, "ws://localhost:12345/")
                c.close()
            }
        }
    }

    @Test fun handshakes() = handshakes(tcp = false)
    @Test fun handshakesTcp() = handshakes(tcp = true)

    // ---- failures over a stream ----

    /** A non-101 response: `Http` with the bytes after the head as the body. */
    @Test fun clientGetsHttpErrorWithBody() = runReactor {
        val (c, s) = memoryStreamPair()
        coroutineScope {
            launch {
                // A plain HTTP server: read the request head, answer 404.
                val buf = neton.io.bytes.Buffer()
                do s.read(buf) while (!buf.backingArray().decodeToString(buf.readerIndex(), buf.writerIndex()).endsWith("\r\n\r\n"))
                val out = neton.io.bytes.Buffer()
                out.writeBytes("HTTP/1.1 404 Not Found\r\nContent-Length: 5\r\n\r\nnope!".encodeToByteArray())
                s.write(out)
                s.close()
            }
            val e = assertFailsWith<WebSocketException.Http> { clientHandshake(c, "ws://h/") }
            assertEquals(StatusCode.NOT_FOUND, e.response.status)
            assertContentEquals("nope!".encodeToByteArray(), e.response.body)
            c.close()
        }
    }

    /** The callback's error response is written (with its body) and the server fails with `Http`. */
    @Test fun serverRejectionReachesTheClient() = runReactor {
        val (c, s) = memoryStreamPair()
        coroutineScope {
            val server = async {
                runCatching {
                    acceptHdr(s) { _, _ -> CallbackResult.Reject(Response.builder().status(401).body<String?>("who are you")) }
                }.also { s.close() }
            }
            val e = assertFailsWith<WebSocketException.Http> { clientHandshake(c, "ws://h/") }
            assertEquals(StatusCode.UNAUTHORIZED, e.response.status)
            assertContentEquals("who are you".encodeToByteArray(), e.response.body)
            val se = server.await().exceptionOrNull()
            assertTrue(se is WebSocketException.Http, "got $se")
            assertEquals(StatusCode.UNAUTHORIZED, se.response.status)
            c.close()
        }
    }

    /** Bytes sent right behind the 101 response reach the client's core (`from_partially_read`). */
    @Test fun leftoverGoesToTheCore() = runReactor {
        val (c, s) = memoryStreamPair()
        coroutineScope {
            launch {
                val core = accept(s)
                core.write(Message.text("early"))
                s.write(core.output) // the frame goes out right behind the response, before the client reads
                s.flush()
            }
            // The server writes both before the client resumes, so they arrive in one read.
            val hs = clientHandshake(c, "ws://h/")
            assertTrue(hs.leftover.size > 0)
            assertEquals("early", hs.connection().read().toText())
            c.close(); s.close()
        }
    }

    @Test fun junkAfterRequestOverAStream() = runReactor {
        val (c, s) = memoryStreamPair()
        coroutineScope {
            launch {
                val out = neton.io.bytes.Buffer()
                out.writeBytes(("GET / HTTP/1.1\r\nHost: h\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n" +
                    "Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\njunk").encodeToByteArray())
                c.write(out)
            }
            assertProtocolError<ProtocolError.JunkAfterRequest> { accept(s) }
            c.close(); s.close()
        }
    }

    @Test fun eofDuringHandshake() = runReactor {
        val (c, s) = memoryStreamPair()
        c.close()
        assertProtocolError<ProtocolError.HandshakeIncomplete> { accept(s) }
        s.close()
    }

    @Test fun unreachablePortIsUnableToConnect() = runReactor {
        // Bind and close a listener, so nothing listens on that port.
        val (listener, port) = listenLoopback()
        listener.close()
        val e = assertFailsWith<WebSocketException.Url> { connect("ws://127.0.0.1:$port/") }
        assertEquals(UrlError.UnableToConnect("ws://127.0.0.1:$port/"), e.error)
    }
}
