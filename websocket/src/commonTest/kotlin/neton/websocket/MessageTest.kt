package neton.websocket

import neton.io.bytes.Bytes
import neton.websocket.frame.CloseCode
import neton.websocket.frame.CloseFrame
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/** Ported from `T/src/protocol/message.rs` tests (6). */
class MessageTest {
    @Test fun display() {
        val t = Message.text("test")
        assertEquals("test", t.toString())
        val bin = Message.binary(bytesOf(0, 1, 3, 4, 241))
        assertEquals("Binary Data<length=5>", bin.toString())
    }

    @Test fun binaryConvert() {
        val bin = bytesOf(6, 7, 8, 9, 10, 241)
        val msg = Message.binary(bin) // `From<&[u8]>`: copies
        assertTrue(msg.isBinary)
        assertFailsWith<WebSocketException.Utf8> { msg.intoText() }
    }

    @Test fun binaryConvertBytes() {
        val bin = Bytes.copyOf(bytesOf(6, 7, 8, 9, 10, 241))
        val msg = Message.binary(bin)
        assertTrue(msg.isBinary)
        assertFailsWith<WebSocketException.Utf8> { msg.intoText() }
    }

    @Test fun binaryConvertVec() {
        val bin = bytesOf(6, 7, 8, 9, 10, 241)
        val msg = Message.binary(Bytes.wrap(bin)) // `From<Vec<u8>>`: takes ownership, no copy
        assertTrue(msg.isBinary)
        assertFailsWith<WebSocketException.Utf8> { msg.intoText() }
    }

    @Test fun binaryConvertIntoBytes() {
        val bin = bytesOf(6, 7, 8, 9, 10, 241)
        val binCopy = bin.copyOf()
        val msg = Message.binary(Bytes.wrap(bin))
        val serialized: Bytes = msg.intoData()
        assertContentEquals(binCopy, serialized.toByteArray())
    }

    @Test fun textConvert() {
        val s = "kiwotsukete"
        val msg = Message.text(s)
        assertTrue(msg.isText)
    }

    // ---- beyond the reference's tests ----

    @Test fun lengthsAndConversions() {
        assertEquals(3, Message.text("abc").length)
        assertEquals(0, Message.Close(null).length)
        assertEquals("bye", Message.Close(CloseFrame(CloseCode.Normal, "bye")).toText())
        assertEquals("bye", Message.Close(CloseFrame(CloseCode.Normal, "bye")).toString())
        assertEquals("", Message.Close(null).toString())
        assertEquals("bye (1000)", CloseFrame(CloseCode.Normal, "bye").toString())
        assertEquals(Message.text("x"), Message.Text("x"))
        assertTrue(Message.Ping(Bytes.EMPTY).isEmpty)
    }
}
