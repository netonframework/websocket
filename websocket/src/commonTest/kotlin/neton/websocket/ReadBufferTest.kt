package neton.websocket

import neton.io.bytes.Buffer
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals

/**
 * Ported from `T/src/buffer.rs` tests (2). The reference's `ReadBuffer` (a FIFO filled by chunked
 * reads, consumed through a cursor) is replaced here by neton-io's [Buffer], the type the core
 * reads from; these check it gives the same observable behaviour, filled the way a driver fills
 * [WebSocketCore.input] (reserve, read into the backing array, commit).
 */
class ReadBufferTest {
    private fun readFrom(input: CursorStream, buf: Buffer, chunk: Int): Int {
        buf.reserve(chunk)
        val n = input.read(buf.backingArray(), buf.writerIndex(), chunk)
        buf.commitWrite(n)
        return n
    }

    @Test fun simpleReading() {
        val input = CursorStream("Hello World!".encodeToByteArray())
        val buffer = Buffer(4096)
        val size = readFrom(input, buffer, 4096)
        assertEquals(12, size)
        assertContentEquals("Hello World!".encodeToByteArray(), buffer.peekAll())
    }

    @Test fun readingInChunks() {
        val inp = CursorStream("Hello World!".encodeToByteArray())
        val buf = Buffer(4)

        var size = readFrom(inp, buf, 4)
        assertEquals(4, size)
        assertContentEquals("Hell".encodeToByteArray(), buf.peekAll())

        buf.skip(2)
        assertContentEquals("ll".encodeToByteArray(), buf.peekAll())

        size = readFrom(inp, buf, 4)
        assertEquals(4, size)
        assertContentEquals("llo Wo".encodeToByteArray(), buf.peekAll())

        size = readFrom(inp, buf, 4)
        assertEquals(4, size)
        assertContentEquals("llo World!".encodeToByteArray(), buf.peekAll())
    }
}
