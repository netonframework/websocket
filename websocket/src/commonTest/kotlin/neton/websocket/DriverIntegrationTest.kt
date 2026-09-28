package neton.websocket

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.yield
import neton.io.bytes.Buffer
import neton.io.core.IoStream
import neton.io.core.memoryStreamPair
import neton.io.net.SocketOptions
import neton.io.net.runReactor
import neton.websocket.frame.Frame
import neton.websocket.frame.FrameHeader
import neton.websocket.frame.OpCode
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The reference's stream-level integration tests over the coroutine driver (SPEC §7):
 * `T/tests/connection_reset.rs` (3), `no_send_after_close.rs` (1), `receive_after_init_close.rs` (1),
 * `write.rs` (1) and `auto_pong_flush.rs` (1). The connection tests run over loopback TCP (the
 * `...Tcp` twin, as the reference) and over a neton-io `memoryStreamPair`; the reference's
 * `Error::ConnectionClosed` is `receive()` returning null.
 */
class DriverIntegrationTest {
    /**
     * `do_test`: a client connects to `/socket`, the server accepts; [clientTask] and [serverTask]
     * run concurrently. The reference's 5-second watchdog is a timeout.
     */
    private fun doTest(
        tcp: Boolean,
        serverOptions: SocketOptions = SocketOptions.Default,
        clientTask: suspend (WebSocket) -> Unit,
        serverTask: suspend (WebSocket) -> Unit,
    ) = runReactor {
        withTimeout(5_000) {
            coroutineScope {
                if (tcp) {
                    val (listener, port) = listenLoopback(serverOptions)
                    val clientJob = launch { clientTask(connect("ws://127.0.0.1:$port/socket").first) }
                    val server = accept(listener.accept())
                    listener.close()
                    serverTask(server)
                    clientJob.join()
                } else {
                    val (c, s) = memoryStreamPair()
                    val clientJob = launch { clientTask(client("ws://localhost:3012/socket", c).first) }
                    serverTask(accept(s))
                    clientJob.join()
                }
            }
        }
    }

    // ---- T/tests/connection_reset.rs ----

    private fun testServerClose(tcp: Boolean) = doTest(
        tcp,
        clientTask = { cliSock ->
            cliSock.send(Message.text("Hello WebSocket"))
            assertTrue(cliSock.receive()!!.isClose) // receive close from server
            assertNull(cliSock.receive()) // now we should get ConnectionClosed
        },
        serverTask = { srvSock ->
            assertEquals("Hello WebSocket", srvSock.receive()!!.intoData().toByteArray().decodeToString())
            srvSock.close() // send close to client
            assertTrue(srvSock.receive()!!.isClose) // receive acknowledgement
            assertNull(srvSock.receive()) // now we should get ConnectionClosed
        },
    )

    @Test fun testServerClose() = testServerClose(tcp = false)
    @Test fun testServerCloseTcp() = testServerClose(tcp = true)

    /**
     * Over TCP the server drops the connection with SO_LINGER 0 (a reset) right after the closing
     * handshake; over a memory stream, which has no reset, it just closes it.
     */
    private fun testEvilServerClose(tcp: Boolean) = doTest(
        tcp,
        serverOptions = SocketOptions(lingerSeconds = 0),
        clientTask = { cliSock ->
            cliSock.send(Message.text("Hello WebSocket"))
            delay(500)
            assertTrue(cliSock.receive()!!.isClose) // receive close from server
            assertNull(cliSock.receive()) // now we should get ConnectionClosed
        },
        serverTask = { srvSock ->
            assertEquals("Hello WebSocket", srvSock.receive()!!.intoData().toByteArray().decodeToString())
            srvSock.close() // send close to client
            assertTrue(srvSock.receive()!!.isClose) // receive acknowledgement
            // and now just drop the connection without waiting for `ConnectionClosed`
            srvSock.abort()
        },
    )

    @Test fun testEvilServerClose() = testEvilServerClose(tcp = false)
    @Test fun testEvilServerCloseTcp() = testEvilServerClose(tcp = true)

    private fun testClientClose(tcp: Boolean) = doTest(
        tcp,
        clientTask = { cliSock ->
            cliSock.send(Message.text("Hello WebSocket"))
            assertEquals("From Server", cliSock.receive()!!.toText()) // receive answer from server
            cliSock.close() // send close to server
            assertTrue(cliSock.receive()!!.isClose) // receive acknowledgement from server
            assertNull(cliSock.receive()) // now we should get ConnectionClosed
        },
        serverTask = { srvSock ->
            assertEquals("Hello WebSocket", srvSock.receive()!!.toText())
            srvSock.send(Message.text("From Server"))
            assertTrue(srvSock.receive()!!.isClose) // receive close from client
            assertNull(srvSock.receive()) // now we should get ConnectionClosed
        },
    )

    @Test fun testClientClose() = testClientClose(tcp = false)
    @Test fun testClientCloseTcp() = testClientClose(tcp = true)

    // ---- T/tests/no_send_after_close.rs ----

    private fun testNoSendAfterClose(tcp: Boolean) = doTest(
        tcp,
        clientTask = { client ->
            assertTrue(client.receive()!!.isClose) // receive close from server
            assertNull(client.receive()) // now we should get ConnectionClosed
        },
        serverTask = { clientHandler ->
            clientHandler.close() // send close to client
            val e = assertFailsWith<WebSocketException.Protocol> { clientHandler.send(Message.text("Hello WebSocket")) }
            assertEquals(ProtocolError.SendAfterClosing, e.error)
            clientHandler.abort() // drop(client_handler)
        },
    )

    @Test fun testNoSendAfterClose() = testNoSendAfterClose(tcp = false)
    @Test fun testNoSendAfterCloseTcp() = testNoSendAfterClose(tcp = true)

    // ---- T/tests/receive_after_init_close.rs ----

    private fun testReceiveAfterInitClose(tcp: Boolean) = doTest(
        tcp,
        clientTask = { client ->
            client.send(Message.text("Hello WebSocket"))
            assertTrue(client.receive()!!.isClose) // receive close from server
            assertNull(client.receive()) // now we should get ConnectionClosed
        },
        serverTask = { clientHandler ->
            clientHandler.close() // send close to client
            // This read should succeed even though we already initiated a close
            assertEquals("Hello WebSocket", clientHandler.receive()!!.intoData().toByteArray().decodeToString())
            assertTrue(clientHandler.receive()!!.isClose) // receive acknowledgement
            assertNull(clientHandler.receive()) // now we should get ConnectionClosed
        },
    )

    @Test fun testReceiveAfterInitClose() = testReceiveAfterInitClose(tcp = false)
    @Test fun testReceiveAfterInitCloseTcp() = testReceiveAfterInitClose(tcp = true)

    // ---- T/tests/write.rs ----

    /** Records call stats and drops the data; reads never complete. */
    private class RecordingStream : IoStream {
        var writtenBytes = 0
        var writeCount = 0
        var flushCount = 0
        override suspend fun read(dst: Buffer): Int = awaitCancellation()
        override suspend fun write(src: Buffer): Int {
            val n = src.readableBytes
            src.skip(n)
            writtenBytes += n
            writeCount++
            return n
        }
        override suspend fun flush() { flushCount++ }
        override fun close() {}
    }

    /** Write buffering and flushing behaviour. */
    @Test fun writeFlushBehaviour() = runReactor {
        val sendMeLen = 10
        val batchMeLen = 11
        val writeBufferSize = 600

        val mock = RecordingStream()
        val ws = WebSocket.fromRawStream(mock, Role.Server, WebSocketConfig(writeBufferSize = writeBufferSize))

        assertEquals(0, mock.writtenBytes)
        assertEquals(0, mock.writeCount)
        assertEquals(0, mock.flushCount)

        // `send` writes & flushes immediately.
        ws.send(Message.text("Send me!"))
        assertEquals(sendMeLen, mock.writtenBytes)
        assertEquals(1, mock.writeCount)
        assertEquals(1, mock.flushCount)

        // A batch of messages: after 55 feeds the output exceeds writeBufferSize = 600 and is
        // written once (not flushed): the driver takes it before the 56th is queued.
        repeat(100) { ws.feed(Message.text("Batch me!")) }
        assertEquals(55 * batchMeLen + sendMeLen, mock.writtenBytes)
        assertEquals(2, mock.writeCount)
        assertEquals(1, mock.flushCount)

        // Flushing writes the rest once and flushes.
        ws.flush()
        assertEquals(100 * batchMeLen + sendMeLen, mock.writtenBytes)
        assertEquals(3, mock.writeCount)
        assertEquals(2, mock.flushCount)
        ws.abort()
    }

    // ---- T/tests/auto_pong_flush.rs ----

    /** Reads a single ping, then nothing ever again; writes work; a flush waits for [flushGate]. */
    private class PingOnceStream : IoStream {
        var writtenData = ByteArray(0)
        var flushedData = ByteArray(0)
        var writeCalls = 0
        var flushCalls = 0
        var readCalls = 0
        val flushGate = CompletableDeferred<Unit>()

        override suspend fun read(dst: Buffer): Int {
            readCalls++
            if (readCalls == 1) {
                val b = Buffer()
                Frame.ping(neton.io.bytes.Bytes.EMPTY).format(b)
                val n = b.readableBytes
                dst.writeBytes(b.readAll())
                return n
            }
            awaitCancellation()
        }

        override suspend fun write(src: Buffer): Int {
            writeCalls++
            val n = src.readableBytes
            writtenData += src.readAll()
            return n
        }

        override suspend fun flush() {
            flushCalls++
            flushGate.await() // the flush "would block" until the gate opens
            flushedData = writtenData
            writtenData = ByteArray(0)
        }

        override fun close() {}
    }

    /**
     * In read-only usage auto pong responses are written and flushed even if flushes would block.
     *
     * ⚖️ A flush that cannot complete suspends (in the driver) instead of failing with `WouldBlock`
     * and being retried on the next read, so the pong is flushed by one flush call, not three, and
     * the reads do not retry anything.
     */
    @Test fun readUsageAutoPongFlush() = runReactor {
        val mock = PingOnceStream()
        val ws = WebSocket.fromRawStream(mock, Role.Client)

        // Receiving a ping schedules a pong (not written yet).
        val msg = ws.receive()
        assertTrue(msg is Message.Ping, "Unexpected msg $msg")
        assertEquals(1, mock.readCalls)
        assertTrue(mock.writtenData.isEmpty())
        assertTrue(mock.flushedData.isEmpty())

        // The next receive waits for more to read; meanwhile the driver writes the pong and its
        // flush blocks. The reader is not held up by that.
        val reader = launch { ws.receive() }
        repeat(3) { yield() }
        assertEquals(2, mock.readCalls)
        assertTrue(mock.writtenData.isNotEmpty(), "Should have written a pong frame")
        assertEquals(1, mock.writeCalls)
        val pongHeader = assertNotNull(FrameHeader.parse(Buffer().apply { writeBytes(mock.writtenData) })).header
        assertEquals(OpCode.Control.Pong, pongHeader.opcode)
        val writtenData = mock.writtenData.copyOf()
        assertEquals(1, mock.flushCalls)
        assertTrue(mock.flushedData.isEmpty())

        // The flush can complete now.
        mock.flushGate.complete(Unit)
        repeat(3) { yield() }
        assertEquals(1, mock.writeCalls)
        assertEquals(1, mock.flushCalls)
        assertContentEquals(writtenData, mock.flushedData)

        // No additional writes or flushes are necessary.
        repeat(3) { yield() }
        assertEquals(2, mock.readCalls)
        assertEquals(1, mock.writeCalls)
        assertEquals(1, mock.flushCalls)
        reader.cancel()
        ws.abort()
    }
}
