package neton.websocket

import neton.websocket.frame.CloseCode
import neton.websocket.frame.CloseFrame
import neton.websocket.frame.OpCode
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

/** One test (or more) per ⚖️ of SPEC §4–§5. */
class DeviationTest {
    private fun server(config: WebSocketConfig = WebSocketConfig()) = WebSocketCore(Role.Server, config)
    private fun client(config: WebSocketConfig = WebSocketConfig()) = WebSocketCore(Role.Client, config)

    /** §4.1: 64-bit length with the top bit set → protocol error (the reference: then a size error or a huge reserve). */
    @Test fun lengthTopBitRejected() {
        val core = client(WebSocketConfig(maxFrameSize = null))
        core.feed(bytesOf(0x82, 0x7f, 0x80, 0, 0, 0, 0, 0, 0, 0))
        assertProtocolError<ProtocolError.InvalidPayloadLength> { core.read() }
    }

    /** §4.1: an oversized control frame fails on its header, before its payload is read; same error type. */
    @Test fun oversizedControlFrameRejectedBeforePayload() {
        // A ping announcing 126 bytes; no payload has arrived.
        assertProtocolError<ProtocolError.ControlFrameTooBig> { client().feed(bytesOf(0x89, 0x7e, 0x00, 0x7e)).read() }
        assertProtocolError<ProtocolError.ControlFrameTooBig> { client().feed(bytesOf(0x88, 0x7f, 0, 0, 0, 0, 0, 1, 0, 0)).read() }
        // The error is the one the reference reports after reading the payload, in its order:
        assertProtocolError<ProtocolError.UnmaskedFrameFromClient> { server().feed(bytesOf(0x89, 0x7e, 0x00, 0x7e)).read() }
        assertProtocolError<ProtocolError.NonZeroReservedBits> { client().feed(bytesOf(0xc9, 0x7e, 0x00, 0x7e)).read() }
        assertProtocolError<ProtocolError.MaskedFrameFromServer> { client().feed(bytesOf(0x89, 0xfe, 0x00, 0x7e, 1, 2, 3, 4)).read() }
        assertProtocolError<ProtocolError.FragmentedControlFrame> { client().feed(bytesOf(0x09, 0x7e, 0x00, 0x7e)).read() }
        // maxFrameSize is still checked first, like the reference.
        assertFailsWith<WebSocketException.Capacity> { client(WebSocketConfig(maxFrameSize = 100)).feed(bytesOf(0x89, 0x7e, 0x00, 0x7e)).read() }
        // 125 bytes is fine.
        val ok = client().feed(bytesOf(0x89, 125) + ByteArray(125))
        assertEquals(125, (ok.read() as Message.Ping).data.size)
    }

    /** §4.3: send-side limits the reference documents but does not enforce. */
    @Test fun sendSideLimits() {
        val c = client(WebSocketConfig(maxMessageSize = 100))
        assertProtocolError<ProtocolError.ControlFrameTooBig> { c.write(Message.Ping(neton.io.bytes.Bytes.wrap(ByteArray(126)))) }
        assertProtocolError<ProtocolError.ControlFrameTooBig> { c.write(Message.Pong(neton.io.bytes.Bytes.wrap(ByteArray(126)))) }
        assertProtocolError<ProtocolError.ControlFrameTooBig> { c.close(CloseFrame(CloseCode.Normal, "x".repeat(124))) }
        assertEquals(WebSocketState.Active, c.state) // a refused close changes nothing
        val e = assertFailsWith<WebSocketException.Capacity> { c.write(Message.binary(ByteArray(101))) }
        assertEquals(CapacityError.MessageTooLong(101, 100), e.error)
        assertFailsWith<WebSocketException.Capacity> { c.write(Message.text("y".repeat(101))) }
        assertTrue(c.output.isEmpty)
        c.write(Message.Ping(neton.io.bytes.Bytes.wrap(ByteArray(125))))
        c.write(Message.binary(ByteArray(100)))
        c.close(CloseFrame(CloseCode.Normal, "x".repeat(123)))
        c.bufferReply()
        assertEquals(listOf<OpCode>(OpCode.Control.Ping, OpCode.Data.Binary, OpCode.Control.Close), c.drainFrames().map { it.header.opcode })
    }

    /** §4.4: maxWriteBufferSize defaults to 4 × writeBufferSize; invalid configs throw. */
    @Test fun maxWriteBufferSizeDefault() {
        assertEquals(4 * 128 * 1024, WebSocketConfig().maxWriteBufferSize)
        assertEquals(2400, WebSocketConfig(writeBufferSize = 600).maxWriteBufferSize)
        assertEquals(4 * 128 * 1024, WebSocketConfig(writeBufferSize = 0).maxWriteBufferSize)
        assertEquals(Int.MAX_VALUE, WebSocketConfig(writeBufferSize = Int.MAX_VALUE / 2).maxWriteBufferSize)
        assertFailsWith<IllegalArgumentException> { WebSocketConfig(writeBufferSize = 10, maxWriteBufferSize = 10) }
        // Other defaults (the reference's).
        val d = WebSocketConfig()
        assertEquals(128 * 1024, d.readBufferSize)
        assertEquals(128 * 1024, d.writeBufferSize)
        assertEquals(64 shl 20, d.maxMessageSize)
        assertEquals(16 shl 20, d.maxFrameSize)
        assertFalse(d.acceptUnmaskedFrames)
        assertFalse(d.sendCloseOnProtocolError)
    }

    /**
     * WriteBufferFull hands the message back and queues nothing; a frame larger than the bound is
     * accepted into an empty output (with a bounded default the reference's check would make such
     * messages unsendable).
     */
    @Test fun writeBufferFullAndLargeMessages() {
        val s = server(WebSocketConfig(writeBufferSize = 10, maxWriteBufferSize = 100))
        s.write(Message.binary(ByteArray(60)))
        val m = Message.binary(ByteArray(60))
        val e = assertFailsWith<WebSocketException.WriteBufferFull> { s.write(m) }
        assertSame(m, e.rejected)
        assertEquals(62, s.output.readableBytes)
        s.output.readAll()
        s.write(Message.binary(ByteArray(500)))
        assertEquals(504, s.output.readableBytes)
        // The close frame is never refused: it waits in the reply slot until there is room.
        s.close()
        assertFalse(s.bufferReply())
        assertTrue(s.hasPendingReply)
        s.output.readAll()
        assertTrue(s.bufferReply())
    }

    /** §5: sendCloseOnProtocolError off (default) → nothing queued; on → 1002 / 1007 / 1009. */
    @Test fun sendCloseOnProtocolError() {
        val off = client()
        assertProtocolError<ProtocolError.NonZeroReservedBits> { off.feed(fromServer(0xc1)).read() }
        assertFalse(off.hasPendingReply)
        assertEquals(WebSocketState.Active, off.state)

        fun codeAfter(frame: ByteArray, config: WebSocketConfig = WebSocketConfig(sendCloseOnProtocolError = true)): Int {
            val c = client(config)
            assertFailsWith<WebSocketException> { c.feed(frame).read() }
            assertEquals(WebSocketState.ClosedByUs, c.state)
            assertTrue(c.bufferReply())
            val p = c.drainFrames().single().payload
            return ((p[0].toInt() and 0xFF) shl 8) or (p[1].toInt() and 0xFF)
        }
        assertEquals(1002, codeAfter(fromServer(0xc1)))
        assertEquals(1007, codeAfter(fromServer(0x81, bytesOf(0xff))))
        assertEquals(1009, codeAfter(fromServer(0x82, ByteArray(20)), WebSocketConfig(sendCloseOnProtocolError = true, maxMessageSize = 10)))

        // Not after the close handshake started, and not for a plain EOF.
        val c = client(WebSocketConfig(sendCloseOnProtocolError = true))
        c.receivedEof()
        assertProtocolError<ProtocolError.ResetWithoutClosingHandshake> { c.read() }
        assertFalse(c.hasPendingReply)
    }

    /** §5: our close goes through the reply slot: behind queued frames, ahead of (replacing) a pending pong. */
    @Test fun closeThroughReplySlot() {
        val c = client()
        c.feed(fromServer(0x89, bytesOf(5)))
        c.read()
        c.write(Message.text("before"))
        c.close(CloseFrame(CloseCode.Away, "bye"))
        assertTrue(c.bufferReply())
        val frames = c.drainFrames()
        assertEquals(listOf<OpCode>(OpCode.Data.Text, OpCode.Control.Close), frames.map { it.header.opcode })
        assertNull(c.read())
    }
}
