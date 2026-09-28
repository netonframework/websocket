package neton.websocket

import neton.websocket.frame.CloseCode
import neton.websocket.frame.CloseFrame
import neton.websocket.frame.Frame
import neton.websocket.frame.FrameHeader
import neton.websocket.frame.OpCode
import neton.io.bytes.Bytes
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** The sans-I/O core: parsing rules (SPEC §4.1, §4.2), the close state machine and replies (SPEC §5). */
class WebSocketCoreTest {
    private fun server(config: WebSocketConfig = WebSocketConfig()) = WebSocketCore(Role.Server, config)
    private fun client(config: WebSocketConfig = WebSocketConfig()) = WebSocketCore(Role.Client, config)

    // ---- parsing ----

    @Test fun incompleteDataIsNotConsumed() {
        val all = fromClient(0x81, "hello".encodeToByteArray())
        val core = server()
        for (b in all) {
            assertNull(core.read())
            core.input.writeByte(b)
        }
        assertEquals(Message.text("hello"), core.read())
        assertNull(core.read())
    }

    @Test fun manyFramesInOneRead() {
        val core = client()
        core.feed(fromServer(0x81, "a".encodeToByteArray()) + fromServer(0x82, bytesOf(1)) + fromServer(0x8a) + fromServer(0x81))
        assertEquals(Message.text("a"), core.read())
        assertEquals(Message.binary(bytesOf(1)), core.read())
        assertEquals(Message.Pong(Bytes.EMPTY), core.read())
        assertEquals(Message.text(""), core.read())
        assertNull(core.read())
    }

    @Test fun serverUnmasksLargeFrames() {
        val payload = ByteArray(70000) { (it % 251).toByte() }
        val core = server()
        core.feed(fromClient(0x82, payload))
        val m = assertIs<Message.Binary>(core.read())
        assertContentEquals(payload, m.data.toByteArray())
    }

    @Test fun partiallyReadPrefix() {
        val all = fromServer(0x81, "prefix".encodeToByteArray())
        val core = WebSocketCore(Role.Client, WebSocketConfig(), Bytes.copyOf(all, 0, 3))
        assertNull(core.read())
        core.feed(all.copyOfRange(3, all.size))
        assertEquals(Message.text("prefix"), core.read())
    }

    @Test fun validationOrderAndErrors() {
        assertProtocolError<ProtocolError.NonZeroReservedBits> { client().feed(fromServer(0xc1, bytesOf(0x61))).read() }
        assertProtocolError<ProtocolError.MaskedFrameFromServer> { client().feed(fromClient(0x81)).read() }
        assertProtocolError<ProtocolError.UnmaskedFrameFromClient> { server().feed(fromServer(0x81)).read() }
        // Unmasked beats RSV, like the reference (the codec checks it first).
        assertProtocolError<ProtocolError.UnmaskedFrameFromClient> { server().feed(fromServer(0xc1)).read() }
        assertEquals(Message.text("ok"), server(WebSocketConfig(acceptUnmaskedFrames = true)).feed(fromServer(0x81, "ok".encodeToByteArray())).read())
        assertProtocolError<ProtocolError.FragmentedControlFrame> { client().feed(fromServer(0x09)).read() }
        assertProtocolError<ProtocolError.UnexpectedContinueFrame> { client().feed(fromServer(0x80)).read() }
        val e = assertProtocolError<ProtocolError.ExpectedFragment> { client().feed(fromServer(0x01) + fromServer(0x82)).read() }
        assertEquals(OpCode.Data.Binary, e.data)
        assertProtocolError<ProtocolError.InvalidOpcode> { client().feed(bytesOf(0x83, 0x00)).read() }
        assertProtocolError<ProtocolError.InvalidOpcode> { client().feed(bytesOf(0x8b, 0x00)).read() }
    }

    @Test fun receivedAfterClosing() {
        val core = client()
        core.feed(fromServer(0x88) + fromServer(0x81))
        assertEquals(Message.Close(null), core.read())
        assertProtocolError<ProtocolError.ReceivedAfterClosing> { core.read() }
    }

    @Test fun frameSizeCheckedBeforePayload() {
        val core = client(WebSocketConfig(maxFrameSize = 10))
        core.feed(bytesOf(0x82, 11)) // header only
        val e = assertFailsWith<WebSocketException.Capacity> { core.read() }
        assertEquals(CapacityError.MessageTooLong(11, 10), e.error)
    }

    // ---- reassembly and UTF-8 (SPEC §4.2) ----

    @Test fun fragmentedTextWithCodePointSplitAcrossFrames() {
        val euro = "€".encodeToByteArray() // e2 82 ac
        val core = client()
        core.feed(fromServer(0x01, bytesOf(0x61, 0xe2)) + fromServer(0x00, bytesOf(0x82)) + fromServer(0x80, bytesOf(0xac, 0x62)))
        assertEquals(Message.text("a€b"), core.read())
        assertEquals(3, euro.size)
    }

    @Test fun fragmentedTextInvalidIsUtf8ErrorNotProtocol() {
        assertFailsWith<WebSocketException.Utf8> {
            client().feed(fromServer(0x01, bytesOf(0x61, 0xe2)) + fromServer(0x80, bytesOf(0x28))).read()
        }
        // Ends inside a code point.
        assertFailsWith<WebSocketException.Utf8> {
            client().feed(fromServer(0x01, bytesOf(0x61)) + fromServer(0x80, bytesOf(0xe2, 0x82))).read()
        }
        // Single frame.
        assertFailsWith<WebSocketException.Utf8> { client().feed(fromServer(0x81, bytesOf(0xc0, 0x80))).read() }
        // Invalid in the first fragment is reported at that fragment.
        assertFailsWith<WebSocketException.Utf8> { client().feed(fromServer(0x01, bytesOf(0xff))).read() }
    }

    @Test fun controlFramesBetweenFragments() {
        val core = client()
        core.feed(fromServer(0x02, bytesOf(1, 2)) + fromServer(0x89, bytesOf(9)) + fromServer(0x00, bytesOf(3)) + fromServer(0x80, bytesOf(4)))
        assertEquals(Message.Ping(bytes(9)), core.read())
        assertEquals(Message.binary(bytesOf(1, 2, 3, 4)), core.read())
    }

    @Test fun fragmentedSizeLimitCountsAllFragments() {
        val core = client(WebSocketConfig(maxMessageSize = 4))
        core.feed(fromServer(0x02, bytesOf(1, 2)) + fromServer(0x00, bytesOf(3, 4)) + fromServer(0x80, bytesOf(5)))
        val e = assertFailsWith<WebSocketException.Capacity> { core.read() }
        assertEquals(CapacityError.MessageTooLong(5, 4), e.error)
    }

    // ---- close state machine (SPEC §5) ----

    @Test fun serverTerminatesOnceReplyIsWrittenOut() {
        val core = server()
        core.feed(fromClient(0x88, closePayload(1000, "bye")))
        assertEquals(Message.Close(CloseFrame(CloseCode.Normal, "bye")), core.read())
        assertEquals(WebSocketState.ClosedByPeer, core.state)
        assertFalse(core.canRead)
        assertFalse(core.canWrite)
        assertTrue(core.hasPendingReply)
        assertNull(core.read()) // reply not written yet: keep reading
        assertTrue(core.bufferReply())
        val echo = core.drainFrames().single()
        assertEquals(OpCode.Control.Close, echo.header.opcode)
        assertContentEquals(closePayload(1000, "bye"), echo.payload.toByteArray())
        assertFailsWith<WebSocketException.ConnectionClosed> { core.flushed() }
        assertEquals(WebSocketState.Terminated, core.state)
        assertFailsWith<WebSocketException.AlreadyClosed> { core.read() }
        assertFailsWith<WebSocketException.AlreadyClosed> { core.write(Message.text("x")) }
    }

    @Test fun serverReadReportsClosedWhenOutputAlreadyDrained() {
        val core = server()
        core.close()
        core.bufferReply(); core.drainFrames()
        core.flushed() // still waiting for the client's answer
        core.feed(fromClient(0x88))
        assertEquals(Message.Close(null), core.read())
        assertEquals(WebSocketState.CloseAcknowledged, core.state)
        assertFailsWith<WebSocketException.ConnectionClosed> { core.read() }
    }

    @Test fun clientWaitsForEof() {
        val core = client()
        core.feed(fromServer(0x88, closePayload(1001)))
        assertEquals(Message.Close(CloseFrame(CloseCode.Away)), core.read())
        assertTrue(core.bufferReply())
        assertEquals(1, core.drainFrames().size)
        core.flushed() // a client does not terminate here
        assertNull(core.read())
        core.receivedEof()
        assertFailsWith<WebSocketException.ConnectionClosed> { core.read() }
        assertFailsWith<WebSocketException.AlreadyClosed> { core.read() }
    }

    @Test fun eofWithoutClosingHandshake() {
        val core = client()
        core.receivedEof()
        assertProtocolError<ProtocolError.ResetWithoutClosingHandshake> { core.read() }
        assertEquals(WebSocketState.Terminated, core.state)
        // EOF in the middle of a frame is the same.
        val c2 = client().feed(bytesOf(0x82, 5, 1))
        c2.receivedEof()
        assertProtocolError<ProtocolError.ResetWithoutClosingHandshake> { c2.read() }
    }

    @Test fun resetAfterPeerCloseIsNormal() {
        val core = client()
        assertIs<WebSocketException.Io>(core.mapIoError(RuntimeException("reset"), isConnectionReset = true))
        core.feed(fromServer(0x88))
        core.read()
        assertIs<WebSocketException.ConnectionClosed>(core.mapIoError(RuntimeException("reset"), isConnectionReset = true))
        assertIs<WebSocketException.Io>(core.mapIoError(RuntimeException("other"), isConnectionReset = false))
    }

    @Test fun disallowedCloseCodeEchoedAsProtocolViolation() {
        for (code in listOf(1005, 1006, 1015, 999, 1016, 5000)) {
            val core = client()
            core.feed(fromServer(0x88, closePayload(code, "x")))
            val expected = CloseFrame(CloseCode.Protocol, "Protocol violation")
            assertEquals(Message.Close(expected), core.read())
            core.bufferReply()
            assertContentEquals(closePayload(1002, "Protocol violation"), core.drainFrames().single().payload.toByteArray())
        }
    }

    @Test fun closePayloadRules() {
        assertProtocolError<ProtocolError.InvalidCloseSequence> { client().feed(fromServer(0x88, bytesOf(3))).read() }
        assertFailsWith<WebSocketException.Utf8> { client().feed(fromServer(0x88, bytesOf(3, 0xe8, 0xff))).read() }
        assertEquals(Message.Close(null), client().feed(fromServer(0x88)).read())
    }

    @Test fun sendAfterClosingAndCloseIsIdempotent() {
        val core = client()
        core.close(CloseFrame(CloseCode.Normal, "done"))
        assertEquals(WebSocketState.ClosedByUs, core.state)
        assertProtocolError<ProtocolError.SendAfterClosing> { core.write(Message.text("x")) }
        core.close() // no-op
        core.bufferReply()
        assertEquals(1, core.drainFrames().size)
        assertFalse(core.hasPendingReply)
    }

    @Test fun readsContinueAfterOurCloseUntilPeerCloses() {
        val core = client()
        core.close()
        core.feed(fromServer(0x81, "late".encodeToByteArray()) + fromServer(0x88))
        assertEquals(Message.text("late"), core.read())
        assertEquals(Message.Close(null), core.read())
        assertEquals(WebSocketState.CloseAcknowledged, core.state)
    }

    // ---- ping / pong and the reply slot (SPEC §5) ----

    @Test fun autoPongSharesPingPayload() {
        val core = client()
        core.feed(fromServer(0x89, bytesOf(1, 2, 3)))
        val ping = assertIs<Message.Ping>(core.read())
        assertTrue(core.hasPendingReply)
        assertTrue(core.bufferReply())
        val pong = core.drainFrames().single()
        assertEquals(OpCode.Control.Pong, pong.header.opcode)
        assertEquals(ping.data, pong.payload)
    }

    /** SPEC §5 backpressure: the peer keeps pinging and never reads; reading goes on, one reply at most. */
    @Test fun pingFloodKeepsOnlyLatestPong() {
        val core = client()
        repeat(1000) { i ->
            core.feed(fromServer(0x89, bytesOf(i and 0xFF, i ushr 8)))
            assertEquals(Message.Ping(bytes(i and 0xFF, i ushr 8)), core.read())
            assertTrue(core.output.isEmpty) // nothing piles up in the output
        }
        assertTrue(core.bufferReply())
        val pong = core.drainFrames().single()
        assertEquals(bytes(999 and 0xFF, 999 ushr 8), pong.payload)
    }

    /** SPEC §5: after our close no pong is sent; the close keeps its slot while reading goes on. */
    @Test fun closeHasPriorityOverPongsWhilePeerDoesNotRead() {
        val core = client()
        core.feed(fromServer(0x89, bytesOf(1)))
        core.read()
        core.close() // replaces the pending pong
        repeat(10) {
            core.feed(fromServer(0x89, bytesOf(2)))
            assertIs<Message.Ping>(core.read())
        }
        core.feed(fromServer(0x88))
        assertEquals(Message.Close(null), core.read())
        assertTrue(core.bufferReply())
        val frames = core.drainFrames()
        assertEquals(listOf<OpCode>(OpCode.Control.Close), frames.map { it.header.opcode })
    }

    @Test fun userPongReplacesAutoPong() {
        val core = client()
        core.feed(fromServer(0x89, bytesOf(1)))
        core.read()
        core.write(Message.Pong(bytes(2)))
        core.bufferReply()
        assertEquals(bytes(2), core.drainFrames().single().payload)
    }

    @Test fun closeReplyIsNotReplacedByPong() {
        val core = client()
        core.feed(fromServer(0x88) + fromServer(0x89))
        core.read()
        assertProtocolError<ProtocolError.ReceivedAfterClosing> { core.read() }
        core.bufferReply()
        assertEquals(OpCode.Control.Close, core.drainFrames().single().header.opcode)
    }

    // ---- write path (SPEC §4.3) ----

    @Test fun clientMasksEveryFrameServerDoesNot() {
        val c = client()
        c.write(Message.text("hello"))
        c.write(Message.text("hello"))
        val raw = c.output.peekAll()
        assertEquals(0x80, raw[1].toInt() and 0x80)
        val frames = c.drainFrames()
        assertEquals(listOf("hello", "hello"), frames.map { it.toText() })
        // Fresh keys per frame: the two masked encodings differ (fails with probability 2^-32).
        assertNotEquals(raw.copyOfRange(2, 11).toList(), raw.copyOfRange(13, 22).toList())

        val s = server()
        s.write(Message.binary(bytesOf(1, 2)))
        assertContentEquals(bytesOf(0x82, 0x02, 1, 2), s.output.readAll())
    }

    @Test fun rawFramesForFragmentation() {
        val s = server()
        s.write(Message.Frame(Frame.message(bytes(0x61), OpCode.Data.Text, false)))
        s.write(Message.Frame(Frame.message(bytes(0x62), OpCode.Data.Continue, true)))
        assertContentEquals(bytesOf(0x01, 0x01, 0x61, 0x80, 0x01, 0x62), s.output.readAll())
        // A client re-masks raw frames with a fresh key.
        val c = client()
        c.write(Message.Frame(Frame(FrameHeader(opcode = OpCode.Data.Binary), bytes(7))))
        assertEquals("[7]", c.drainFrames().single().payload.toByteArray().toList().toString())
    }

    @Test fun wantsWriteAfterWriteBufferSize() {
        val s = server(WebSocketConfig(writeBufferSize = 10))
        s.write(Message.binary(ByteArray(8)))
        assertFalse(s.wantsWrite)
        s.write(Message.binary(ByteArray(1)))
        assertTrue(s.wantsWrite)
    }

    @Test fun setConfigReappliesLimits() {
        val s = server()
        s.setConfig { it.copy(writeBufferSize = 0, maxWriteBufferSize = 5) }
        s.write(Message.binary(ByteArray(10))) // empty output: accepted
        assertFailsWith<WebSocketException.WriteBufferFull> { s.write(Message.binary(ByteArray(1))) }
        assertFailsWith<IllegalArgumentException> { s.setConfig { it.copy(maxWriteBufferSize = 0) } }
    }
}
