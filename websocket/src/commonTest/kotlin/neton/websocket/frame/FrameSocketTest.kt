package neton.websocket.frame

import neton.io.bytes.Bytes
import neton.websocket.CapacityError
import neton.websocket.WebSocketException
import neton.websocket.bytes
import neton.websocket.bytesOf
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull

/** Ported from `T/src/protocol/frame/mod.rs` tests (6). */
class FrameSocketTest {
    @Test fun readFrames() {
        val sock = FrameSocket()
        sock.input.writeBytes(bytesOf(0x82, 0x07, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x82, 0x03, 0x03, 0x02, 0x01, 0x99))
        sock.receivedEof()
        assertEquals(bytes(0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07), assertNotNull(sock.read()).intoPayload())
        assertEquals(bytes(0x03, 0x02, 0x01), assertNotNull(sock.read()).intoPayload())
        assertNull(sock.read())
        assertContentEquals(bytesOf(0x99), sock.remaining())
    }

    @Test fun fromPartiallyRead() {
        val sock = FrameSocket(prefix = bytes(0x82, 0x07, 0x01))
        assertNull(sock.read())
        sock.input.writeBytes(bytesOf(0x02, 0x03, 0x04, 0x05, 0x06, 0x07))
        assertEquals(bytes(0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07), assertNotNull(sock.read()).intoPayload())
    }

    @Test fun writeFrames() {
        val sock = FrameSocket()
        sock.write(Frame.ping(bytes(0x04, 0x05)))
        sock.write(Frame.pong(bytes(0x01)))
        assertContentEquals(bytesOf(0x89, 0x02, 0x04, 0x05, 0x8a, 0x01, 0x01), sock.output.readAll())
    }

    @Test fun parseOverflow() {
        val sock = FrameSocket()
        sock.input.writeBytes(bytesOf(0x83, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x00))
        runCatching { sock.read() } // should not crash
    }

    @Test fun sizeLimitHit() {
        val sock = FrameSocket()
        sock.input.writeBytes(bytesOf(0x82, 0x07, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07))
        val e = assertFailsWith<WebSocketException.Capacity> { sock.read(5) }
        assertEquals(CapacityError.MessageTooLong(size = 7, maxSize = 5), e.error)
    }

    /**
     * The reference runs this on 32-bit targets only; here lengths are `Int`-indexed everywhere, so
     * it applies on every target. The size is reported unnarrowed (the reference: wrapped to 5).
     */
    @Test fun lengthAboveUsizeMaxRejected() {
        val sock = FrameSocket()
        sock.input.writeBytes(bytesOf(0x82, 0x7f, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x05))
        val e = assertFailsWith<WebSocketException.Capacity> { sock.read(null) }
        assertEquals(CapacityError.MessageTooLong(size = 0x1_0000_0005L, maxSize = Int.MAX_VALUE.toLong()), e.error)
    }

    // ---- beyond the reference's tests ----

    @Test fun unmasksMaskedFramesAndAcceptsUnmasked() {
        val out = FrameSocket()
        val f = Frame.message(Bytes.wrap("hello".encodeToByteArray()), OpCode.Data.Text, true)
        f.header.mask = bytesOf(1, 2, 3, 4)
        out.write(f)
        out.write(Frame.ping(bytes(7)))
        val sock = FrameSocket()
        sock.input.writeBytes(out.output.readAll())
        val got = assertNotNull(sock.read())
        assertEquals("hello", got.toText())
        assertNull(got.header.mask)
        assertEquals(bytes(7), assertNotNull(sock.read()).payload)
    }

    @Test fun byteAtATimeInput() {
        val all = bytesOf(0x82, 0x7e, 0x00, 0x80) + ByteArray(128) { it.toByte() }
        val sock = FrameSocket()
        for (i in all.indices) {
            assertNull(sock.read())
            sock.input.writeByte(all[i])
        }
        assertEquals(128, assertNotNull(sock.read()).payload.size)
    }
}
