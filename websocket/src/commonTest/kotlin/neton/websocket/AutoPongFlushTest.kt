package neton.websocket

import neton.io.bytes.Buffer
import neton.io.bytes.Bytes
import neton.websocket.frame.Frame
import neton.websocket.frame.FrameHeader
import neton.websocket.frame.OpCode
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/** Ported from `T/tests/auto_pong_flush.rs` (1). */
class AutoPongFlushTest {
    private val numberOfFlushesToGetItToWork = 3

    /** Reads a single ping, then WouldBlock forever; writes work; flush blocks twice, then works. */
    private inner class MockWrite : MockStream {
        var writtenData = ByteArray(0)
        var flushedData = ByteArray(0)
        var writeCalls = 0
        var flushCalls = 0
        var readCalls = 0

        override fun read(dst: ByteArray, off: Int, len: Int): Int {
            readCalls++
            if (readCalls == 1) {
                val ping = Frame.ping(Bytes.EMPTY)
                val b = Buffer()
                ping.format(b)
                val n = b.readableBytes
                b.readAll().copyInto(dst, off)
                return n
            }
            throw WouldBlock()
        }

        override fun write(src: ByteArray, off: Int, len: Int): Int {
            writeCalls++
            writtenData += src.copyOfRange(off, off + len)
            return len
        }

        override fun flush() {
            flushCalls++
            if (flushCalls % numberOfFlushesToGetItToWork == 0) {
                flushedData = writtenData
                writtenData = ByteArray(0)
            } else {
                throw WouldBlock()
            }
        }
    }

    /** In read-only usage auto pong responses are written and flushed even if flushes would block. */
    @Test fun readUsageAutoPongFlush() {
        val mock = MockWrite()
        val ws = SyncWebSocket(mock, Role.Client)

        // Receiving a ping schedules a pong on the next read or write (not written yet).
        val msg = ws.read()
        assertTrue(msg is Message.Ping, "Unexpected msg $msg")
        assertEquals(1, mock.readCalls)
        assertTrue(mock.writtenData.isEmpty())
        assertTrue(mock.flushedData.isEmpty())

        // Next read fails as there is nothing else to read; it tried to write & flush the pong,
        // with the flush blocking.
        assertFailsWith<WouldBlock> { ws.read() }
        assertEquals(2, mock.readCalls)
        assertTrue(mock.writtenData.isNotEmpty(), "Should have written a pong frame")
        assertEquals(1, mock.writeCalls)

        val pongHeader = assertNotNull(FrameHeader.parse(Buffer().apply { writeBytes(mock.writtenData) })).header
        assertEquals(OpCode.Control.Pong, pongHeader.opcode)
        val writtenData = mock.writtenData.copyOf()

        assertEquals(1, mock.flushCalls)
        assertTrue(mock.flushedData.isEmpty())

        // Next read tries to flush the pong again, which again blocks.
        assertFailsWith<WouldBlock> { ws.read() }
        assertEquals(3, mock.readCalls)
        assertEquals(1, mock.writeCalls)
        assertEquals(2, mock.flushCalls)
        assertTrue(mock.flushedData.isEmpty())

        // 3rd flush attempt is the charm.
        assertFailsWith<WouldBlock> { ws.read() }
        assertEquals(4, mock.readCalls)
        assertEquals(1, mock.writeCalls)
        assertEquals(3, mock.flushCalls)
        assertContentEquals(writtenData, mock.flushedData)

        // On following reads no additional writes or flushes are necessary.
        assertFailsWith<WouldBlock> { ws.read() }
        assertEquals(5, mock.readCalls)
        assertEquals(1, mock.writeCalls)
        assertEquals(3, mock.flushCalls)
    }
}
