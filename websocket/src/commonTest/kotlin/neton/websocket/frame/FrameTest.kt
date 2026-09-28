package neton.websocket.frame

import neton.io.bytes.Buffer
import neton.io.bytes.Bytes
import neton.websocket.ProtocolError
import neton.websocket.assertProtocolError
import neton.websocket.bytes
import neton.websocket.bytesOf
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** Ported from `T/src/protocol/frame/frame.rs` tests (4). */
class FrameTest {
    @Test fun parse() {
        val raw = Buffer().apply { writeBytes(bytesOf(0x82, 0x07, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07)) }
        val (header, length) = assertNotNull(FrameHeader.parse(raw))
        assertEquals(7L, length)
        val frame = Frame.fromPayload(header, Bytes.wrap(raw.readAll()))
        assertEquals(bytes(0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07), frame.intoPayload())
    }

    @Test fun format() {
        val frame = Frame.ping(bytes(0x01, 0x02))
        val buf = Buffer(frame.length)
        frame.format(buf)
        assertContentEquals(bytesOf(0x89, 0x02, 0x01, 0x02), buf.readAll())
    }

    @Test fun formatIntoBuf() {
        val frame = Frame.ping(bytes(0x01, 0x02))
        val buf = Buffer(frame.length)
        frame.formatIntoBuf(buf)
        assertContentEquals(bytesOf(0x89, 0x02, 0x01, 0x02), buf.readAll())
    }

    @Test fun display() {
        val f = Frame.message(Bytes.wrap("hi there".encodeToByteArray()), OpCode.Data.Text, true)
        val view = f.toString()
        assertTrue(view.contains("payload:"))
    }

    // ---- beyond the reference's tests (SPEC §4.1) ----

    /** Incomplete data is not consumed, at every split point of a masked 64-bit-length header. */
    @Test fun incompleteHeaderConsumesNothing() {
        val full = bytesOf(0x82, 0xff, 0, 0, 0, 0, 0, 1, 0, 0, 1, 2, 3, 4)
        for (n in 0 until full.size) {
            val b = Buffer().apply { writeBytes(full, 0, n) }
            assertNull(FrameHeader.parse(b))
            assertEquals(n, b.readableBytes)
        }
        val b = Buffer().apply { writeBytes(full) }
        val (h, len) = assertNotNull(FrameHeader.parse(b))
        assertEquals(65536L, len)
        assertContentEquals(bytesOf(1, 2, 3, 4), h.mask)
        assertEquals(0, b.readableBytes)
    }

    @Test fun reservedOpcodeRejectedInHeader() {
        for (op in listOf(3, 4, 5, 6, 7, 11, 12, 13, 14, 15)) {
            val e = assertProtocolError<ProtocolError.InvalidOpcode> { FrameHeader.parse(Buffer().apply { writeBytes(bytesOf(0x80 or op, 0)) }) }
            assertEquals(op, e.code)
        }
    }

    /** ⚖️ RFC 6455 §5.2: the top bit of a 64-bit length must be 0 (the reference accepts it). */
    @Test fun lengthWithTopBitSetRejected() {
        val b = Buffer().apply { writeBytes(bytesOf(0x82, 0x7f, 0x80, 0, 0, 0, 0, 0, 0, 1)) }
        assertProtocolError<ProtocolError.InvalidPayloadLength> { FrameHeader.parse(b) }
    }

    /** Non-minimal length encodings are accepted, like the reference (RFC 6455 does not require rejecting them). */
    @Test fun nonMinimalLengthAccepted() {
        val b = Buffer().apply { writeBytes(bytesOf(0x82, 0x7e, 0x00, 0x05)) }
        assertEquals(5L, assertNotNull(FrameHeader.parse(b)).length)
        val c = Buffer().apply { writeBytes(bytesOf(0x82, 0x7f, 0, 0, 0, 0, 0, 0, 0, 5)) }
        assertEquals(5L, assertNotNull(FrameHeader.parse(c)).length)
    }

    @Test fun formatLengthsAndMask() {
        for (len in listOf(0, 125, 126, 65535, 65536)) {
            val f = Frame.message(Bytes.wrap(ByteArray(len) { it.toByte() }), OpCode.Data.Binary, true)
            f.header.mask = bytesOf(9, 8, 7, 6)
            val buf = Buffer()
            f.format(buf)
            assertEquals(f.length, buf.readableBytes)
            val (h, l) = assertNotNull(FrameHeader.parse(buf))
            assertEquals(len.toLong(), l)
            val payload = buf.readAll()
            applyMask(payload, 0, payload.size, packMask(h.mask!!))
            assertContentEquals(ByteArray(len) { it.toByte() }, payload)
        }
    }
}
