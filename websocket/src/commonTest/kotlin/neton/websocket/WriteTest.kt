package neton.websocket

import kotlin.test.Test
import kotlin.test.assertEquals

/** Ported from `T/tests/write.rs` (1). */
class WriteTest {
    /** Records call stats and drops the data; reads would block. */
    private class MockWrite : MockStream {
        var writtenBytes = 0
        var writeCount = 0
        var flushCount = 0
        override fun read(dst: ByteArray, off: Int, len: Int): Int = throw WouldBlock()
        override fun write(src: ByteArray, off: Int, len: Int): Int {
            writtenBytes += len
            writeCount++
            return len
        }
        override fun flush() { flushCount++ }
    }

    /** Write buffering and flushing behaviour. */
    @Test fun writeFlushBehaviour() {
        val sendMeLen = 10
        val batchMeLen = 11
        val writeBufferSize = 600

        val mock = MockWrite()
        val ws = SyncWebSocket(mock, Role.Server, WebSocketConfig(writeBufferSize = writeBufferSize))

        assertEquals(0, mock.writtenBytes)
        assertEquals(0, mock.writeCount)
        assertEquals(0, mock.flushCount)

        // `send` writes & flushes immediately.
        ws.send(Message.text("Send me!"))
        assertEquals(sendMeLen, mock.writtenBytes)
        assertEquals(1, mock.writeCount)
        assertEquals(1, mock.flushCount)

        // A batch of messages: after 55 writes the output exceeds writeBufferSize = 600 and is
        // written once (not flushed).
        repeat(100) { ws.write(Message.text("Batch me!")) }
        assertEquals(55 * batchMeLen + sendMeLen, mock.writtenBytes)
        assertEquals(2, mock.writeCount)
        assertEquals(1, mock.flushCount)

        // Flushing writes the rest once and flushes.
        ws.flush()
        assertEquals(100 * batchMeLen + sendMeLen, mock.writtenBytes)
        assertEquals(3, mock.writeCount)
        assertEquals(2, mock.flushCount)
    }
}
