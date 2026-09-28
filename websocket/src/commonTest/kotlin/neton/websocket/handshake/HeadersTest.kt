package neton.websocket.handshake

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** Ported from `T/src/handshake/headers.rs` (3). */
class HeadersTest {
    @Test fun headers() {
        val data = "Host: foo.com\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n".encodeToByteArray()
        val (_, hdr) = tryParseHeaders(data)!!
        assertTrue(hdr["Host"]!!.contentEquals("foo.com"))
        assertTrue(hdr["Upgrade"]!!.contentEquals("websocket"))
        assertTrue(hdr["Connection"]!!.contentEquals("Upgrade"))
    }

    @Test fun headersIter() {
        val data = ("Host: foo.com\r\n" +
            "Sec-WebSocket-Extensions: permessage-deflate\r\n" +
            "Connection: Upgrade\r\n" +
            "Sec-WebSocket-ExtenSIONS: permessage-unknown\r\n" +
            "Upgrade: websocket\r\n" +
            "\r\n").encodeToByteArray()
        val (_, hdr) = tryParseHeaders(data)!!
        val iter = hdr.getAll("Sec-WebSocket-Extensions").iterator()
        assertTrue(iter.next().contentEquals("permessage-deflate"))
        assertTrue(iter.next().contentEquals("permessage-unknown"))
        assertFalse(iter.hasNext())
    }

    @Test fun headersIncomplete() {
        val data = "Host: foo.com\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n".encodeToByteArray()
        assertNull(tryParseHeaders(data))
    }

    /** [MAX_HEADERS] headers parse; one more is a capacity error, as httparse with a 124-slot buffer. */
    @Test fun headerLimit() {
        fun block(n: Int) = (0 until n).joinToString("") { "X-H$it: v\r\n" }.plus("\r\n").encodeToByteArray()
        assertEquals(MAX_HEADERS, tryParseHeaders(block(MAX_HEADERS))!!.second.len())
        val e = kotlin.test.assertFailsWith<neton.websocket.WebSocketException.Capacity> { tryParseHeaders(block(MAX_HEADERS + 1)) }
        assertEquals(neton.websocket.CapacityError.TooManyHeaders, e.error)
    }
}
