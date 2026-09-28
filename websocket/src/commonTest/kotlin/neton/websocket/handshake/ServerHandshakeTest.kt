package neton.websocket.handshake

import neton.websocket.ProtocolError
import neton.websocket.assertProtocolError
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** Ported from `T/src/handshake/server.rs` (7). */
class ServerHandshakeTest {
    private fun requestWithKey(key: String): ServerRequest {
        val data = "GET /script.ws HTTP/1.1\r\n" +
            "Host: foo.com\r\n" +
            "Connection: upgrade\r\n" +
            "Upgrade: websocket\r\n" +
            "Sec-WebSocket-Version: 13\r\n" +
            "Sec-WebSocket-Key: $key\r\n" +
            "\r\n"
        return tryParseRequest(data.encodeToByteArray())!!.second
    }

    private fun assertInvalidSecWebSocketKey(key: String) {
        val req = requestWithKey(key)
        assertProtocolError<ProtocolError.InvalidSecWebSocketKey> { createResponse(req) }
    }

    @Test fun requestParsing() {
        val data = "GET /script.ws HTTP/1.1\r\nHost: foo.com\r\n\r\n".encodeToByteArray()
        val (_, req) = tryParseRequest(data)!!
        assertEquals("/script.ws", req.uri.path)
        assertTrue(req.headers["Host"]!!.contentEquals("foo.com"))
    }

    @Test fun requestReplying() {
        val data = ("GET /script.ws HTTP/1.1\r\n" +
            "Host: foo.com\r\n" +
            "Connection: upgrade\r\n" +
            "Upgrade: websocket\r\n" +
            "Sec-WebSocket-Version: 13\r\n" +
            "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n" +
            "\r\n").encodeToByteArray()
        val (_, req) = tryParseRequest(data)!!
        val response = createResponse(req)
        assertTrue(response.headers["Sec-WebSocket-Accept"]!!.contentEquals("s3pPLMBiTxaQ9kYGzzhZRbK+xOo="))
    }

    @Test fun testInvalidWebsocketKeyEmpty() = assertInvalidSecWebSocketKey("")

    @Test fun testInvalidWebsocketKeyTooLong() = assertInvalidSecWebSocketKey("dGhlIHNhbXBsZSBub25jZQ==AAAAAAAAAA")

    @Test fun testInvalidWebsocketKeyBase64Symbol() = assertInvalidSecWebSocketKey("dGhlIHNhbXBsZSBub25jZQ!!")

    @Test fun testInvalidWebsocketKeyDecodedLength() = assertInvalidSecWebSocketKey("AAAAAAAAAAAAAAAAAAAAAAAA")

    @Test fun testValidWebsocketKey() {
        val req = requestWithKey("dGhlIHNhbXBsZSBub25jZQ==")
        createResponse(req)
    }
}
