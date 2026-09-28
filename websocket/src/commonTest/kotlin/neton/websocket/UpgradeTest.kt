package neton.websocket

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import neton.http.Body
import neton.http.EmptyBody
import neton.http.Method
import neton.http.Request
import neton.http.RequestParts
import neton.http.Response
import neton.http.StatusCode
import neton.http.h1.Http1ClientConfig
import neton.http.h1.Http1ServerConfig
import neton.http.h1.HttpService
import neton.http.h1.serveHttp1
import neton.http.header.HeaderValue
import neton.http.upgradeOn
import neton.http.uri.Uri
import neton.io.bytes.Buffer
import neton.io.core.IoStream
import neton.io.core.memoryStreamPair
import neton.io.net.runReactor
import neton.websocket.handshake.ClientHandshake
import neton.websocket.handshake.HandshakeLimits
import neton.websocket.handshake.TryParse
import neton.websocket.handshake.createResponse
import neton.websocket.handshake.tryParseResponse
import neton.websocket.handshake.verifyUpgradeResponse
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Taking over a connection upgraded by the http library (SPEC §1, §6; tokio-tungstenite's
 * `server-custom-accept` example): an HTTP/1 server answers `101` and hands the stream to
 * [WebSocket.fromUpgraded]; an HTTP/1 client does the same with the `101` it received. Over a
 * neton-io `memoryStreamPair` and loopback TCP.
 */
class UpgradeTest {
    private val serverConfig = Http1ServerConfig(headerReadTimeoutMillis = 0, keepAliveIdleTimeoutMillis = 0, upgrades = true)

    /** Echo text and binary messages until the connection ends. */
    private suspend fun echo(ws: WebSocket) {
        while (true) {
            val m = ws.receive() ?: break
            if (m.isText || m.isBinary) ws.send(m)
        }
    }

    /**
     * An HTTP service that upgrades valid WebSocket requests: `101` with the accept key
     * ([createResponse]), then the connection is taken over in [scope]; 400 otherwise.
     */
    private fun upgradingService(scope: CoroutineScope): HttpService = HttpService { request ->
        val response = try {
            createResponse(request)
        } catch (_: WebSocketException) {
            return@HttpService Response.builder().status(400).body(EmptyBody as Body)
        }
        scope.launch { echo(WebSocket.fromUpgraded(upgradeOn(request.extensions), Role.Server)) }
        Response(response.parts, EmptyBody as Body)
    }

    /** A stream to a running upgrading HTTP server (TCP or memory). */
    private suspend fun CoroutineScope.httpServer(tcp: Boolean): IoStream {
        if (tcp) {
            val (listener, port) = listenLoopback()
            launch {
                val stream = listener.accept()
                listener.close()
                serveHttp1(stream, serverConfig, upgradingService(this))
            }
            return neton.io.net.connect("127.0.0.1", port)
        }
        val (c, s) = memoryStreamPair()
        launch { serveHttp1(s, serverConfig, upgradingService(this)) }
        return c
    }

    /** A stream to a running WebSocket echo server that does its own handshake (TCP or memory). */
    private suspend fun CoroutineScope.wsServer(tcp: Boolean): IoStream {
        if (tcp) {
            val (listener, port) = listenLoopback()
            launch {
                val ws = accept(listener.accept())
                listener.close()
                echo(ws)
            }
            return neton.io.net.connect("127.0.0.1", port)
        }
        val (c, s) = memoryStreamPair()
        launch { echo(accept(s)) }
        return c
    }

    /** Talk to an echo server and close. */
    private suspend fun exchange(ws: WebSocket) {
        ws.send(Message.text("hello"))
        assertEquals("hello", ws.receive()!!.toText())
        ws.send(Message.binary(ByteArray(100_000) { it.toByte() }))
        assertEquals(100_000, (ws.receive() as Message.Binary).data.size)
        ws.close()
        assertTrue(ws.receive()!!.isClose)
        assertNull(ws.receive())
    }

    /** An upgrade through the HTTP/1 client: [verifyUpgradeResponse], then [WebSocket.fromUpgraded]. */
    private suspend fun CoroutineScope.httpClient(stream: IoStream): WebSocket {
        val (sender, connection) = Http1ClientConfig(upgrades = true).handshake(stream)
        val running = launch { connection.run() }
        val wsRequest = "ws://localhost/ws".intoClientRequest()
        val httpRequest = Request(
            RequestParts(Method.GET, Uri.parse("/ws"), wsRequest.version, wsRequest.headers.clone()),
            EmptyBody as Body,
        )
        val response = sender.sendRequest(httpRequest)
        verifyUpgradeResponse(wsRequest, response)
        val ws = WebSocket.fromUpgraded(upgradeOn(response.extensions), Role.Client)
        running.join()
        return ws
    }

    private fun run(block: suspend CoroutineScope.() -> Unit) = runReactor { withTimeout(5_000) { coroutineScope { block() } } }

    // ---- server side: the http server upgrades, the WebSocket takes over ----

    private fun serverUpgradeFromHttp(tcp: Boolean) = run {
        exchange(client("ws://localhost/ws", httpServer(tcp)).first)
    }

    @Test fun serverUpgradeFromHttp() = serverUpgradeFromHttp(tcp = false)
    @Test fun serverUpgradeFromHttpTcp() = serverUpgradeFromHttp(tcp = true)

    /** A frame sent right behind the request is read by the HTTP server and handed over with the stream. */
    @Test fun serverUpgradeKeepsBytesReadPastTheHead() = run {
        val c = httpServer(tcp = false)
        val handshake = ClientHandshake.start("ws://localhost/ws".intoClientRequest())
        c.write(Buffer().apply {
            writeBytes(handshake.request)
            writeBytes(fromClient(0x81, "early".encodeToByteArray()))
        })
        val (response, tail) = readHead(c, HandshakeLimits(), TryParse(::tryParseResponse))
        handshake.verifyResponse(response)
        val ws = WebSocket.fromRawStream(c, Role.Client, prefix = tail)
        assertEquals("early", ws.receive()!!.toText()) // echoed
        exchange(ws)
    }

    /** A request that is not a valid upgrade gets the service's 400, and nothing is taken over. */
    @Test fun serverUpgradeRejectsInvalidRequest() = run {
        val c = httpServer(tcp = false)
        val (sender, connection) = Http1ClientConfig().handshake(c)
        val running = launch { runCatching { connection.run() } } // ends when the stream is closed below
        val response = sender.sendRequest(Request.builder().uri("/ws").header("Upgrade", "websocket").body(EmptyBody as Body))
        assertEquals(StatusCode.BAD_REQUEST, response.status)
        c.close()
        running.join()
    }

    // ---- client side: the http client upgrades, the WebSocket takes over ----

    private fun clientUpgradeThroughHttpClient(tcp: Boolean) = run {
        exchange(httpClient(wsServer(tcp)))
    }

    @Test fun clientUpgradeThroughHttpClient() = clientUpgradeThroughHttpClient(tcp = false)
    @Test fun clientUpgradeThroughHttpClientTcp() = clientUpgradeThroughHttpClient(tcp = true)

    /** Both sides through the http library. */
    private fun httpClientToHttpServer(tcp: Boolean) = run {
        exchange(httpClient(httpServer(tcp)))
    }

    @Test fun httpClientToHttpServer() = httpClientToHttpServer(tcp = false)
    @Test fun httpClientToHttpServerTcp() = httpClientToHttpServer(tcp = true)

    /** [verifyUpgradeResponse] makes the client's checks: status, accept key. */
    @Test fun verifyUpgradeResponseChecksTheResponse() {
        val request = "ws://localhost/ws".intoClientRequest()
        val ok = createResponse(request)
        verifyUpgradeResponse(request, ok)

        val wrongKey = createResponse(request)
        wrongKey.headers.insert("Sec-WebSocket-Accept", HeaderValue.fromStr("s3pPLMBiTxaQ9kYGzzhZRbK+xOo="))
        assertProtocolError<ProtocolError.SecWebSocketAcceptKeyMismatch> { verifyUpgradeResponse(request, wrongKey) }

        val notUpgraded = Response.builder().status(200).body(Unit)
        val e = assertFailsWith<WebSocketException.Http> { verifyUpgradeResponse(request, notUpgraded) }
        assertEquals(StatusCode.OK, e.response.status)
    }
}
