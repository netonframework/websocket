package neton.websocket.handshake

import neton.http.Request
import neton.http.StatusCode
import neton.http.header.HeaderValue
import neton.websocket.WebSocketException
import neton.websocket.intoClientRequest
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

/** Ported from `T/src/handshake/client.rs` (8). */
class ClientHandshakeTest {
    @Test fun randomKeys() {
        val k1 = generateKey()
        val k2 = generateKey()
        assertNotEquals(k1, k2)
        assertEquals(k1.length, k2.length)
        assertEquals(24, k1.length)
        assertEquals(24, k2.length)
        assertTrue(k1.endsWith("=="))
        assertTrue(k2.endsWith("=="))
        assertTrue('=' !in k1.substring(0, 22))
        assertTrue('=' !in k2.substring(0, 22))
    }

    private fun constructExpected(host: String, key: String): ByteArray = (
        "GET /getCaseCount HTTP/1.1\r\n" +
            "Host: $host\r\n" +
            "Connection: Upgrade\r\n" +
            "Upgrade: websocket\r\n" +
            "Sec-WebSocket-Version: 13\r\n" +
            "Sec-WebSocket-Key: $key\r\n" +
            "\r\n"
        ).encodeToByteArray()

    @Test fun requestFormatting() {
        val request = "ws://localhost/getCaseCount".intoClientRequest()
        val (bytes, key) = generateRequest(request)
        assertContentEquals(constructExpected("localhost", key), bytes)
    }

    @Test fun requestFormattingWithHost() {
        val request = "wss://localhost:9001/getCaseCount".intoClientRequest()
        val (bytes, key) = generateRequest(request)
        assertContentEquals(constructExpected("localhost:9001", key), bytes)
    }

    @Test fun requestFormattingWithAt() {
        val request = "wss://user:pass@localhost:9001/getCaseCount".intoClientRequest()
        val (bytes, key) = generateRequest(request)
        assertContentEquals(constructExpected("localhost:9001", key), bytes)
    }

    @Test fun responseParsing() {
        val data = "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n".encodeToByteArray()
        val (_, resp) = tryParseResponse(data)!!
        assertEquals(StatusCode.OK, resp.status)
        assertTrue(resp.headers["Content-Type"]!!.contentEquals("text/html"))
    }

    @Test fun invalidCustomRequest() {
        val request = Request.builder().method("GET").body(Unit)
        assertFailsWith<WebSocketException> { generateRequest(request) }
    }

    @Test fun requestWithNonAsciiHeader() {
        val request = "ws://localhost/path".intoClientRequest()
        // Add a header with non-ASCII value (UTF-8 encoded "Montréal")
        request.headers.insert("X-City", HeaderValue.fromBytes("Montréal".encodeToByteArray()))
        // This should succeed, not fail with UTF-8 error
        val (bytes, _) = generateRequest(request)
        // Verify the complete header with non-ASCII value is preserved in the output
        assertTrue(bytes.containsSlice("x-city: Montréal\r\n".encodeToByteArray()), "the complete non-ASCII header value")
    }

    @Test fun requestWithLatin1Header() {
        val request = "ws://localhost/path".intoClientRequest()
        // "café" in Latin-1: not valid UTF-8, but valid for HTTP headers
        val latin1 = byteArrayOf('c'.code.toByte(), 'a'.code.toByte(), 'f'.code.toByte(), 0xe9.toByte())
        request.headers.insert("X-Test", HeaderValue.fromBytes(latin1))
        val (bytes, _) = generateRequest(request)
        assertTrue(bytes.containsSlice("x-test: ".encodeToByteArray() + latin1 + "\r\n".encodeToByteArray()), "the raw Latin-1 bytes")
    }
}

fun ByteArray.containsSlice(needle: ByteArray): Boolean =
    (0..size - needle.size).any { i -> needle.indices.all { this[i + it] == needle[it] } }
