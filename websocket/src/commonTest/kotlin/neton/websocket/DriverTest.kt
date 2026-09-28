package neton.websocket

import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.async
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.yield
import neton.io.bytes.Buffer
import neton.io.bytes.Bytes
import neton.io.core.IoException
import neton.io.core.IoStream
import neton.io.core.memoryStreamPair
import neton.io.net.runReactor
import neton.websocket.frame.CloseCode
import neton.websocket.frame.CloseFrame
import neton.websocket.frame.Frame
import neton.websocket.frame.FrameSocket
import neton.websocket.frame.OpCode
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The coroutine driver (SPEC §5, §6): reads never wait for writes (bidirectional backpressure),
 * close priority, cancellation of `receive` / `feed` / `send`, stream failures while writing, and
 * the connection-level rules (one reader and one writer, abort, failing on protocol errors).
 * The peer is a raw frame reader / writer on the other end of a neton-io `memoryStreamPair`.
 */
class DriverTest {
    /** The other end, speaking raw frames. */
    private class RawPeer(val stream: IoStream) {
        val frames = FrameSocket()

        suspend fun send(bytes: ByteArray) {
            stream.write(Buffer().apply { writeBytes(bytes) })
        }

        /** The next frame the connection wrote, reading as needed; null at end of stream. */
        suspend fun nextFrame(): Frame? {
            while (true) {
                frames.read()?.let { return it }
                if (frames.isEof) return null
                if (stream.read(frames.input) < 0) frames.receivedEof()
            }
        }
    }

    private fun intBytes(i: Int) = bytesOf(i ushr 24, (i ushr 16) and 0xFF, (i ushr 8) and 0xFF, i and 0xFF)

    private fun Bytes.int(): Int {
        val a = toByteArray()
        return ((a[0].toInt() and 0xFF) shl 24) or ((a[1].toInt() and 0xFF) shl 16) or
            ((a[2].toInt() and 0xFF) shl 8) or (a[3].toInt() and 0xFF)
    }

    private fun Frame.text(): String = payload.toByteArray().decodeToString()

    private fun test(block: suspend kotlinx.coroutines.CoroutineScope.() -> Unit) = runReactor {
        withTimeout(10_000) { coroutineScope { block() } }
    }

    // ---- reads never wait for writes (SPEC §5) ----

    /**
     * The peer floods pings and does not read: every ping is still received, the driver's pending
     * output stays one pong, and the pongs that go out answer ever newer pings, ending with the last.
     */
    @Test fun pingFloodWhilePeerDoesNotRead() = test {
        val (c, s) = memoryStreamPair(capacity = 64)
        val ws = WebSocket.fromRawStream(s, Role.Server)
        val peer = RawPeer(c)
        val n = 2000
        val sender = launch { for (i in 0 until n) peer.send(fromClient(0x89, intBytes(i))) }
        for (i in 0 until n) {
            val m = ws.receive()
            assertTrue(m is Message.Ping, "got $m")
            assertEquals(i, m.data.int())
        }
        sender.join()

        val answered = mutableListOf<Int>()
        while (answered.lastOrNull() != n - 1) {
            val f = peer.nextFrame()!!
            assertEquals(OpCode.Control.Pong, f.header.opcode)
            answered += f.payload.int()
        }
        assertTrue(answered.size < n / 10, "${answered.size} pongs for $n pings")
        assertEquals(answered.distinct().sorted(), answered)
        ws.abort()
    }

    /**
     * Our close while the peer floods pings and does not read: `close` waits for the flush, the
     * reader keeps going, the close frame replaces the pending pong and no pong follows it; the
     * closing handshake then completes normally.
     */
    @Test fun closeTakesPriorityWhilePeerDoesNotRead() = test {
        val (c, s) = memoryStreamPair(capacity = 64)
        val ws = WebSocket.fromRawStream(s, Role.Server)
        val peer = RawPeer(c)
        val n = 500
        val closeAt = 100
        val sender = launch { for (i in 0 until n) peer.send(fromClient(0x89, intBytes(i))) }
        repeat(closeAt) { assertTrue(ws.receive() is Message.Ping) }
        val closing = launch { ws.close(CloseFrame(CloseCode.Normal, "bye")) }
        yield()
        assertFalse(ws.canWrite)
        assertTrue(ws.canRead)
        assertTrue(closing.isActive, "close waits for its flush")
        // The reader is not held up by the pending close.
        for (i in closeAt until n) assertEquals(i, (ws.receive() as Message.Ping).data.int())
        sender.join()

        while (true) {
            val f = peer.nextFrame()!!
            if (f.header.opcode == OpCode.Control.Close) {
                assertEquals(CloseFrame(CloseCode.Normal, "bye"), f.intoClose())
                break
            }
            assertEquals(OpCode.Control.Pong, f.header.opcode)
            assertTrue(f.payload.int() < closeAt, "no pong for a ping received after our close")
        }
        closing.join()

        peer.send(fromClient(0x88, closePayload(1000))) // the peer answers
        assertTrue(ws.receive()!!.isClose) // receive acknowledgement
        assertNull(ws.receive())
        assertNull(peer.nextFrame(), "the server closed the connection; nothing followed the close frame")
    }

    // ---- cancellation (SPEC §6) ----

    /** A frame split at every position: cancelling the receive in between loses nothing. */
    @Test fun cancelledReceiveKeepsTheBytesRead() = test {
        val frame = fromClient(0x81, "hello, partial frame".encodeToByteArray())
        for (split in 1 until frame.size) {
            val (c, s) = memoryStreamPair()
            val ws = WebSocket.fromRawStream(s, Role.Server)
            val peer = RawPeer(c)
            peer.send(frame.copyOfRange(0, split))
            val receiving = launch { ws.receive() }
            repeat(3) { yield() }
            assertTrue(receiving.isActive)
            receiving.cancel()
            receiving.join()
            peer.send(frame.copyOfRange(split, frame.size))
            assertEquals("hello, partial frame", ws.receive()!!.toText(), "split at $split")
            ws.abort()
        }
    }

    /**
     * `feed` waiting for room in a full output buffer and cancelled: nothing of that message is
     * queued; later messages still go out behind the ones accepted before.
     */
    @Test fun cancelledFeedQueuesNothing() = test {
        val (c, s) = memoryStreamPair(capacity = 1)
        val ws = WebSocket.fromRawStream(s, Role.Server, WebSocketConfig(writeBufferSize = 0, maxWriteBufferSize = 64))
        val peer = RawPeer(c)
        val a = "a".repeat(40)
        ws.feed(Message.text(a)) // accepted; the driver starts writing it (a byte at a time)
        yield()
        val b = launch { ws.feed(Message.text("b".repeat(30))) } // 42 in flight + 32 > 64: waits
        repeat(3) { yield() }
        assertTrue(b.isActive, "feed waits for room")
        b.cancel()
        b.join()
        val c2 = launch { ws.send(Message.text("c")) }
        assertEquals(a, peer.nextFrame()!!.text())
        assertEquals("c", peer.nextFrame()!!.text())
        c2.join()
        ws.abort()
        assertNull(peer.nextFrame())
    }

    /** `trySend` never waits: false for a full output buffer, and the message is not queued. */
    @Test fun trySendReportsAFullBuffer() = test {
        val (c, s) = memoryStreamPair(capacity = 1)
        val ws = WebSocket.fromRawStream(s, Role.Server, WebSocketConfig(writeBufferSize = 0, maxWriteBufferSize = 64))
        val peer = RawPeer(c)
        assertTrue(ws.trySend(Message.text("a".repeat(40))))
        yield()
        assertFalse(ws.trySend(Message.text("b".repeat(30))))
        assertTrue(ws.trySend(Message.text("c")))
        assertEquals("a".repeat(40), peer.nextFrame()!!.text())
        assertEquals("c", peer.nextFrame()!!.text())
        ws.abort()
        assertNull(peer.nextFrame())
    }

    /**
     * SPEC §6: with a one-byte pipe every byte of a write may suspend. The sender is cancelled
     * after the peer has read each possible number of bytes: the peer still gets the whole frame,
     * then the next one, and the connection keeps working both ways.
     */
    @Test fun cancelledSendAtEveryBytePosition() = test {
        val first = "first message"
        val frameSize = 2 + first.length
        for (k in 0..frameSize) {
            val (c, s) = memoryStreamPair(capacity = 1)
            val ws = WebSocket.fromRawStream(s, Role.Server)
            val peer = RawPeer(c)
            // Undispatched: the frame is accepted before the peer reads (k = 0 cancels right after).
            val sending = launch(start = CoroutineStart.UNDISPATCHED) { ws.send(Message.text(first)) }
            repeat(k) { c.read(peer.frames.input) } // a byte at a time
            sending.cancel()
            sending.join()
            val second = launch { ws.send(Message.text("second")) }
            assertEquals(first, peer.nextFrame()!!.text(), "cancelled after $k bytes")
            assertEquals("second", peer.nextFrame()!!.text())
            second.join()
            val back = launch { peer.send(fromClient(0x81, "back".encodeToByteArray())) } // a byte at a time too
            assertEquals("back", ws.receive()!!.toText())
            back.join()
            ws.abort()
        }
    }

    /** Writes [limit] bytes in total, then fails every write. */
    private class FailAfter(private val inner: IoStream, private val limit: Int) : IoStream {
        private var written = 0
        override suspend fun read(dst: Buffer): Int = inner.read(dst)
        override suspend fun write(src: Buffer): Int {
            val allowed = limit - written
            if (src.readableBytes <= allowed) {
                val n = inner.write(src)
                written += n
                return n
            }
            val part = Buffer().apply { writeBytes(src.backingArray(), src.readerIndex(), allowed) }
            inner.write(part)
            src.skip(allowed)
            written += allowed
            throw IoException("injected write failure")
        }
        override suspend fun flush() = inner.flush()
        override fun close() = inner.close()
    }

    /**
     * SPEC §6: the stream fails at every byte position. The connection ends (later writes throw
     * `AlreadyClosed`), and the peer gets a prefix of the frames: never a new frame after a
     * partial one.
     */
    @Test fun streamErrorAtEveryBytePosition() = test {
        val f1 = fromServer(0x81, "first message".encodeToByteArray())
        val f2 = fromServer(0x81, "second".encodeToByteArray())
        val all = f1 + f2
        for (k in 0 until all.size) {
            val (c, s) = memoryStreamPair()
            val ws = WebSocket.fromRawStream(FailAfter(s, k), Role.Server)
            val r1 = runCatching { ws.send(Message.text("first message")) }
            val r2 = runCatching { ws.send(Message.text("second")) }
            if (k < f1.size) {
                assertTrue(r1.exceptionOrNull() is WebSocketException.Io, "k=$k: ${r1.exceptionOrNull()}")
                val e = r2.exceptionOrNull()
                assertTrue(e is WebSocketException.AlreadyClosed && e.cause is WebSocketException.Io, "k=$k: $e")
            } else {
                assertTrue(r1.isSuccess, "k=$k: ${r1.exceptionOrNull()}")
                assertTrue(r2.exceptionOrNull() is WebSocketException.Io, "k=$k: ${r2.exceptionOrNull()}")
            }
            assertFalse(ws.canWrite)
            assertFailsWith<WebSocketException.AlreadyClosed> { ws.send(Message.text("third")) }
            val got = Buffer()
            while (c.read(got) >= 0) { /* until the connection's close */ }
            assertContentEquals(all.copyOf(k), got.readAll(), "k=$k")
            // The reader learns the failure once, then AlreadyClosed.
            assertTrue(runCatching { ws.receive() }.exceptionOrNull() is WebSocketException.Io)
            assertFailsWith<WebSocketException.AlreadyClosed> { ws.receive() }
        }
    }

    // ---- ending the connection ----

    /** ⚖️ A protocol error fails the connection: the stream is closed, no close frame by default. */
    @Test fun protocolErrorFailsTheConnection() = test {
        val (c, s) = memoryStreamPair()
        val ws = WebSocket.fromRawStream(s, Role.Server)
        val peer = RawPeer(c)
        peer.send(fromServer(0x81, "x".encodeToByteArray())) // unmasked, from a client
        assertProtocolError<ProtocolError.UnmaskedFrameFromClient> { ws.receive() }
        assertNull(peer.nextFrame())
        assertFalse(ws.canRead)
        assertFailsWith<WebSocketException.AlreadyClosed> { ws.receive() }
        val e = assertFailsWith<WebSocketException.AlreadyClosed> { ws.send(Message.text("late")) }
        assertTrue(e.cause is WebSocketException.Protocol)
    }

    /** `sendCloseOnProtocolError`: the close frame for the error goes out first, then the stream closes. */
    @Test fun sendCloseOnProtocolErrorOverTheDriver() = test {
        val (c, s) = memoryStreamPair()
        val ws = WebSocket.fromRawStream(s, Role.Server, WebSocketConfig(sendCloseOnProtocolError = true))
        val peer = RawPeer(c)
        peer.send(fromServer(0x81, "x".encodeToByteArray()))
        assertProtocolError<ProtocolError.UnmaskedFrameFromClient> { ws.receive() }
        val f = peer.nextFrame()!!
        assertEquals(CloseCode.Protocol, f.intoClose()!!.code)
        assertNull(peer.nextFrame())
    }

    /** ⚖️ The peer closes the connection right after its close frame: our echo may fail; still a normal end. */
    @Test fun writeErrorAfterPeerCloseIsNormalEnd() = test {
        val (c, s) = memoryStreamPair()
        val ws = WebSocket.fromRawStream(s, Role.Client)
        RawPeer(c).send(fromServer(0x88, closePayload(1000, "going")))
        c.close()
        assertEquals(CloseFrame(CloseCode.Normal, "going"), (ws.receive() as Message.Close).frame)
        assertNull(ws.receive())
        assertFailsWith<WebSocketException.AlreadyClosed> { ws.receive() }
    }

    /** `abort` ends the connection at once: a waiting receive and later calls get `AlreadyClosed`. */
    @Test fun abortEndsWaitingOperations() = test {
        val (c, s) = memoryStreamPair()
        val ws = WebSocket.fromRawStream(s, Role.Server)
        val receiving = async { runCatching { ws.receive() } }
        yield()
        ws.abort()
        assertTrue(receiving.await().exceptionOrNull() is WebSocketException.AlreadyClosed)
        assertFailsWith<WebSocketException.AlreadyClosed> { ws.send(Message.text("x")) }
        assertFailsWith<WebSocketException.AlreadyClosed> { ws.close() }
        assertNull(RawPeer(c).nextFrame())
    }

    /** One receive and one write at a time, as for an `IoStream`. */
    @Test fun oneReceiveAtATime() = test {
        val (c, s) = memoryStreamPair()
        val ws = WebSocket.fromRawStream(s, Role.Server)
        val receiving = launch { ws.receive() }
        yield()
        assertFailsWith<IllegalStateException> { ws.receive() }
        receiving.cancel()
        ws.abort()
        c.close()
    }

    /** The halves of `split` in two coroutines: the reader hands texts to the writer, which echoes them. */
    @Test fun splitHalvesInTwoCoroutines() = test {
        val (c, s) = memoryStreamPair()
        val (rx, tx) = WebSocket.fromRawStream(s, Role.Server).split()
        val echo = Channel<Message>(Channel.UNLIMITED)
        val reader = launch {
            while (true) {
                val m = rx.receive() ?: break
                if (m.isText) echo.send(m)
            }
            echo.close()
        }
        val writer = launch { for (m in echo) tx.send(m) }
        val clientWs = WebSocket.fromRawStream(c, Role.Client)
        for (i in 0 until 50) clientWs.send(Message.text("m$i"))
        for (i in 0 until 50) assertEquals("m$i", clientWs.receive()!!.toText())
        clientWs.close()
        assertTrue(clientWs.receive()!!.isClose)
        assertNull(clientWs.receive())
        reader.join()
        writer.join()
        assertFalse(rx.canRead)
        assertFalse(tx.canWrite)
    }

    /** A message larger than `maxWriteBufferSize` is accepted into an empty buffer (SPEC §11.1) and echoed over TCP. */
    @Test fun largeMessagesBothWaysTcp() = test {
        val (listener, port) = listenLoopback()
        val size = 3 shl 20
        val server = launch {
            val ws = accept(listener.accept())
            listener.close()
            while (true) {
                val m = ws.receive() ?: break
                if (m.isBinary) ws.send(m)
            }
        }
        val (ws, _) = connect("ws://127.0.0.1:$port/")
        val data = ByteArray(size) { (it * 31).toByte() }
        repeat(3) {
            ws.send(Message.binary(data))
            val back = ws.receive() as Message.Binary
            assertContentEquals(data, back.data.toByteArray())
        }
        ws.close()
        assertTrue(ws.receive()!!.isClose)
        assertNull(ws.receive())
        server.join()
    }
}
