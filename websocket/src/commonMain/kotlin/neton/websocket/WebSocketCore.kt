package neton.websocket

import neton.io.bytes.Buffer
import neton.io.bytes.Bytes
import neton.websocket.frame.CloseCode
import neton.websocket.frame.CloseFrame
import neton.websocket.frame.Frame
import neton.websocket.frame.FrameCodec
import neton.websocket.frame.MaskSource
import neton.websocket.frame.OP_BINARY
import neton.websocket.frame.OP_CLOSE
import neton.websocket.frame.OP_CONTINUE
import neton.websocket.frame.OP_PING
import neton.websocket.frame.OP_PONG
import neton.websocket.frame.OP_TEXT
import neton.websocket.frame.OpCode
import neton.websocket.frame.applyMask
import neton.websocket.frame.headerSize
import neton.websocket.frame.packMask
import neton.websocket.frame.writeFrame

/** Connection state (tungstenite `WebSocketState`, `T/src/protocol/mod.rs:793-828`). */
enum class WebSocketState {
    /** The connection is open. */
    Active,
    /** We sent a close frame; the peer has not answered yet. */
    ClosedByUs,
    /** The peer sent a close frame (our reply is queued). */
    ClosedByPeer,
    /** The peer answered our close frame. */
    CloseAcknowledged,
    /** The connection is over. */
    Terminated;

    /** Normal messages may be processed (the peer may still send until it answers our close). */
    val canRead: Boolean get() = this == Active || this == ClosedByUs

    val isActive: Boolean get() = this == Active
}

/**
 * The WebSocket protocol without I/O (tungstenite `WebSocketContext`, `T/src/protocol/mod.rs:360-782`;
 * SPEC §2, §4, §5): frame coding, masking, message reassembly, UTF-8 validation, the close state
 * machine and automatic replies. It never reads a clock or a socket; a driver moves the bytes.
 *
 * ## Driving it
 * Reading:
 * 1. Append received bytes to [input] (e.g. `stream.read(core.input)`, at most [maxReadSize] at a
 *    time); at end of stream call [receivedEof].
 * 2. Call [read]: it returns the next [Message], or null when [input] holds no complete message
 *    yet (go to 1). It throws [WebSocketException.ConnectionClosed] when the connection has ended
 *    normally, and other [WebSocketException]s when it failed.
 *
 * Writing:
 * - [write] / [close] only queue: data frames go into [output] (bounded by
 *   [WebSocketConfig.maxWriteBufferSize], else [WebSocketException.WriteBufferFull]); automatic
 *   replies (pong, close) and our own close frame wait in a one-frame reply slot ([hasPendingReply]).
 * - Whoever writes to the network calls [bufferReply] to move the reply behind the queued frames,
 *   writes out [output] (and consumes what was written), flushes the stream, then calls [flushed].
 *   [wantsWrite] tells when [output] passed [WebSocketConfig.writeBufferSize] and should be written
 *   even without a flush. A driver that writes while frames keep being queued uses [takeOutput] /
 *   [outputWritten] instead of writing [output] in place.
 * - Reading never waits for writing: a reply only sits in the slot, and a newer pong replaces an
 *   older one, so the slot never piles up while the peer does not read (SPEC §5).
 *
 * Termination (RFC 6455 §7.1.1, TIME_WAIT on the server): a server reports
 * [WebSocketException.ConnectionClosed] from [read] or [flushed] once the close handshake is over
 * and everything is written, and should then close the TCP connection; a client reports it when
 * the server closes the connection ([receivedEof], or a reset: [mapIoError]).
 *
 * Not thread-safe: use it from one thread (one reactor) at a time.
 *
 * @param prefix bytes read past the handshake, parsed first (`from_partially_read`).
 * @param deflate ⚖️ permessage-deflate as negotiated by the handshake (RFC 7692; [PerMessageDeflate]); null for none.
 *   Its native compression state is released when the connection terminates.
 */
class WebSocketCore(
    val role: Role,
    config: WebSocketConfig = WebSocketConfig(),
    prefix: Bytes? = null,
    val deflate: PerMessageDeflate? = null,
) {
    /** Current configuration; change it with [setConfig]. */
    var config: WebSocketConfig = config
        private set

    private val codec = FrameCodec(config.readBufferSize, prefix)
    private val masks: MaskSource? = if (role == Role.Client) MaskSource() else null
    private var deflater: RawDeflater? = null
    private var inflater: RawInflater? = null

    /** Connection state. */
    var state: WebSocketState = WebSocketState.Active
        private set

    private var incomplete: IncompleteMessage? = null
    private var inputEof = false
    private var blocked = false

    /** Nonblocking admission before reserving a data frame payload. Unsupported with compression. */
    fun setInboundAdmission(beforePayload: (Int) -> Unit) {
        check(deflate == null) { "Compressed admission requires a decompressed-byte budget" }
        codec.beforeDataPayload = beforePayload
    }

    /** Closing mode: drain data payloads without materializing messages, retaining control handling. */
    fun discardData() {
        incomplete = null
        codec.discardData = true
    }

    // The reply slot (tungstenite `additional_send`): opcode 0 when empty, else OP_PONG / OP_CLOSE.
    private var replyOp = 0
    private var replyPayload: Bytes = Bytes.EMPTY
    private var replyQueued = false

    init { applyLimits() }

    /** Received bytes not yet decoded; the driver appends to it. */
    val input: Buffer get() = codec.input

    /** Encoded frames to send; the driver writes them out and consumes what was written. */
    val output: Buffer get() = codec.output

    /** The most one read should ask for: `max(readBufferSize, 14)`. */
    val maxReadSize: Int get() = codec.maxReadSize

    /** Reading is possible: not after receiving a close frame (tungstenite `can_read`). */
    val canRead: Boolean get() = state.canRead

    /** Writing is possible: only while [WebSocketState.Active] (tungstenite `can_write`). */
    val canWrite: Boolean get() = state.isActive

    /** An automatic reply or our close frame waits in the reply slot. */
    val hasPendingReply: Boolean get() = replyOp != 0

    /** [output] holds more than [WebSocketConfig.writeBufferSize] bytes: write it out now. */
    val wantsWrite: Boolean get() = codec.wantsWrite

    /**
     * Replace the configuration (tungstenite `set_config`): `setConfig { it.copy(maxMessageSize = 1 shl 20) }`.
     * The new value is validated by [WebSocketConfig]'s constructor and the write limits re-applied.
     * The read buffer keeps its size.
     */
    fun setConfig(transform: (WebSocketConfig) -> WebSocketConfig) {
        config = transform(config)
        applyLimits()
    }

    private fun applyLimits() {
        codec.maxOutBufferLen = config.maxWriteBufferSize
        codec.outBufferWriteLen = config.writeBufferSize
    }

    /** The input stream ended: pending reads resolve by the close state machine. */
    fun receivedEof() { inputEof = true }

    // ------------------------------------------------------------------------------------------
    // Reading
    // ------------------------------------------------------------------------------------------

    /**
     * Decode the next message from [input] (tungstenite `WebSocketContext::read`, `mod.rs:449-479`).
     *
     * Pings queue a pong and are returned; a close frame from the peer queues the echo and is
     * returned as [Message.Close]; our close being answered is returned as [Message.Close] too.
     *
     * @return the message, or null when more input is needed.
     * @throws WebSocketException.ConnectionClosed the connection ended normally.
     * @throws WebSocketException.AlreadyClosed called after the connection was terminated.
     * @throws WebSocketException for protocol, UTF-8 and size errors; the connection is then failed
     *   and should be dropped (with [WebSocketConfig.sendCloseOnProtocolError], after sending the
     *   close frame queued in the reply slot).
     */
    fun read(): Message? {
        checkNotTerminated()
        try {
            while (true) {
                // Server: once the close handshake is done and everything went out, it is over.
                if (replyOp == 0 && role == Role.Server && !state.canRead && allWritten) {
                    terminated()
                    throw WebSocketException.ConnectionClosed()
                }
                val m = readMessageFrame()
                if (m != null) return m
                if (blocked) { blocked = false; return null }
            }
        } catch (e: WebSocketException) {
            if (config.sendCloseOnProtocolError && state == WebSocketState.Active) queueFailureClose(e)
            throw e
        }
    }

    private fun queueFailureClose(e: WebSocketException) {
        val code = when (e) {
            is WebSocketException.Protocol -> CloseCode.Protocol
            is WebSocketException.Utf8 -> CloseCode.Invalid
            is WebSocketException.Capacity -> CloseCode.Size
            else -> return
        }
        state = WebSocketState.ClosedByUs
        replyOp = OP_CLOSE
        replyPayload = Frame.closePayload(CloseFrame(code))
    }

    /** No complete frame in [input]: at end of stream the close state decides, else wait. */
    private fun needMore(): Message? {
        if (inputEof) {
            val prev = state
            terminated()
            if (prev == WebSocketState.ClosedByPeer || prev == WebSocketState.CloseAcknowledged) {
                throw WebSocketException.ConnectionClosed()
            }
            protocolError(ProtocolError.ResetWithoutClosingHandshake)
        }
        blocked = true
        return null
    }

    /** Drop the current frame's payload and fail. */
    private fun fail(len: Int, e: ProtocolError): Nothing {
        input.skip(len)
        protocolError(e)
    }

    /**
     * The checks the reference makes after reading a whole frame (`read_message_frame`, `mod.rs:610-662`),
     * in its order, for a control frame whose header already shows more than 125 bytes: ⚖️ run
     * before its payload is read (SPEC §4.1), so the error is the one the reference would report.
     */
    private fun failOversizedControl(): Nothing {
        val h = codec.header
        if (role == Role.Server && !h.masked && !config.acceptUnmaskedFrames) protocolError(ProtocolError.UnmaskedFrameFromClient)
        if (!state.canRead) protocolError(ProtocolError.ReceivedAfterClosing)
        if (h.rsvBits != 0) protocolError(ProtocolError.NonZeroReservedBits)
        if (role == Role.Client && h.masked) protocolError(ProtocolError.MaskedFrameFromServer)
        if (!h.isFinal) protocolError(ProtocolError.FragmentedControlFrame)
        protocolError(ProtocolError.ControlFrameTooBig)
    }

    /**
     * Decode one frame (tungstenite `read_message_frame`, `mod.rs:610-714`). Returns a message, or
     * null either because the frame did not complete a message or (with [blocked]) because the
     * input holds no whole frame.
     */
    private fun readMessageFrame(): Message? {
        val c = codec
        if (!c.pollHeader(config.maxFrameSize)) return needMore()
        val h = c.header
        if (h.isControl && h.length > 125) failOversizedControl()
        if (c.discardData && !h.isControl) {
            val skip = minOf(c.input.readableBytes.toLong(), h.length).toInt()
            c.input.skip(skip)
            h.length -= skip
            if (h.length > 0) return needMore()
            c.hasHeader = false
            return null
        }
        if (!c.payloadReady()) return needMore()
        c.hasHeader = false

        val len = h.length.toInt()
        val input = c.input
        val a = input.backingArray()
        val off = input.readerIndex()

        var masked = h.masked
        if (role == Role.Server) {
            if (masked) {
                // A server MUST remove masking for data frames received from a client (RFC 6455 §5.3).
                applyMask(a, off, off + len, h.mask)
                masked = false
            } else if (!config.acceptUnmaskedFrames) {
                fail(len, ProtocolError.UnmaskedFrameFromClient)
            }
        }
        if (!state.canRead) fail(len, ProtocolError.ReceivedAfterClosing)
        // ⚖️ RSV1 marks a compressed message: only on its first frame, only with permessage-deflate (RFC 7692 §6).
        val compressed = h.rsvBits == 0x40 && deflate != null && (h.opcode == OP_TEXT || h.opcode == OP_BINARY)
        if (h.rsvBits != 0 && !compressed) fail(len, ProtocolError.NonZeroReservedBits)
        // A client MUST close a connection if it detects a masked frame (RFC 6455 §5.1).
        if (role == Role.Client && masked) fail(len, ProtocolError.MaskedFrameFromServer)

        val op = h.opcode
        if (h.isControl) {
            if (!h.isFinal) fail(len, ProtocolError.FragmentedControlFrame)
            if (len > 125) fail(len, ProtocolError.ControlFrameTooBig)
            return when (op) {
                OP_CLOSE -> doClose(readClose(a, off, len))
                OP_PING -> {
                    val data = takePayload(input, len)
                    // No pong once we sent a close frame.
                    if (state.isActive) setReply(OP_PONG, data)
                    Message.Ping(data)
                }
                OP_PONG -> Message.Pong(takePayload(input, len))
                else -> fail(len, ProtocolError.UnknownControlFrameType(op))
            }
        }

        val fin = h.isFinal
        val inc = incomplete
        when (op) {
            OP_CONTINUE -> {
                if (inc == null) fail(len, ProtocolError.UnexpectedContinueFrame)
                try {
                    if (inc.compressed) inflateInto(inc, a, off, len, fin) else inc.extend(a, off, len, config.maxMessageSize)
                } finally { input.skip(len) }
                if (!fin) return null
                incomplete = null
                return inc.complete()
            }
            OP_TEXT, OP_BINARY -> {
                if (inc != null) fail(len, ProtocolError.ExpectedFragment(OpCode.from(op) as OpCode.Data))
                if (compressed) {
                    // Inflated as it arrives, through the message's size limit and UTF-8 check (a decompression
                    // bomb stops at the limit).
                    val m = IncompleteMessage(op == OP_TEXT).also { it.compressed = true }
                    try { inflateInto(m, a, off, len, fin) } finally { input.skip(len) }
                    if (!fin) { incomplete = m; return null }
                    return m.complete()
                }
                if (!fin) {
                    val m = IncompleteMessage(op == OP_TEXT)
                    try { m.extend(a, off, len, config.maxMessageSize) } finally { input.skip(len) }
                    incomplete = m
                    return null
                }
                val max = config.maxMessageSize
                if (max != null && len > max) {
                    input.skip(len)
                    throw WebSocketException.Capacity(CapacityError.MessageTooLong(len.toLong(), max.toLong()))
                }
                if (op == OP_BINARY) return Message.Binary(takePayload(input, len))
                // Single-frame text: validated in place, handed out without a copy.
                Utf8Validator.check(a, off, off + len)?.let { input.skip(len); throw WebSocketException.Utf8(it) }
                return Message.Text(Utf8Bytes.unchecked(takePayload(input, len)))
            }
            else -> fail(len, ProtocolError.UnknownDataFrameType(op))
        }
    }

    /**
     * Inflate one frame of a compressed message into [m]; after its last frame the sync flush's `00 00 ff ff` the
     * sender removed (RFC 7692 §7.2.2), then a reset when the peer uses no context takeover.
     */
    private fun inflateInto(m: IncompleteMessage, a: ByteArray, off: Int, len: Int, last: Boolean) {
        val d = deflate!!
        val inf = inflater ?: rawInflater(d.decompressWindowBits).also { inflater = it }
        val max = config.maxMessageSize
        val sink: (ByteArray, Int, Int) -> Unit = { b, o, n -> m.extend(b, o, n, max) }
        inf.inflate(a, off, len, sink)
        if (last) {
            inf.inflate(DEFLATE_TAIL, 0, DEFLATE_TAIL.size, sink)
            if (d.decompressNoContextTakeover) inf.reset()
        }
    }

    /** Close payload (tungstenite `Frame::into_close`): none, or a code and a UTF-8 reason. */
    private fun readClose(a: ByteArray, off: Int, len: Int): CloseFrame? {
        if (len == 0) return null
        if (len == 1) fail(len, ProtocolError.InvalidCloseSequence)
        val code = ((a[off].toInt() and 0xFF) shl 8) or (a[off + 1].toInt() and 0xFF)
        Utf8Validator.check(a, off + 2, off + len)?.let { input.skip(len); throw WebSocketException.Utf8(it) }
        input.skip(2)
        return CloseFrame(CloseCode.from(code), Utf8Bytes.unchecked(takePayload(input, len - 2)))
    }

    /**
     * A close frame arrived (tungstenite `do_close`, `mod.rs:716-752`). Returns the message for the
     * caller, or null when the frame is ignored.
     */
    private fun doClose(close: CloseFrame?): Message? = when (state) {
        WebSocketState.Active -> {
            state = WebSocketState.ClosedByPeer
            val echoed = if (close != null && !close.code.isAllowed) {
                CloseFrame(CloseCode.Protocol, PROTOCOL_VIOLATION)
            } else close
            setReply(OP_CLOSE, Frame.closePayload(echoed))
            Message.Close(echoed)
        }
        WebSocketState.ClosedByPeer, WebSocketState.CloseAcknowledged -> null
        WebSocketState.ClosedByUs -> {
            state = WebSocketState.CloseAcknowledged
            Message.Close(close)
        }
        WebSocketState.Terminated -> error("Bug: close frame read after termination")
    }

    // ------------------------------------------------------------------------------------------
    // Writing
    // ------------------------------------------------------------------------------------------

    /**
     * Queue [message] (tungstenite `WebSocketContext::write`, `mod.rs:491-521`, without the I/O).
     * Text, binary, ping and raw frames go into [output]; a pong goes to the reply slot (replacing a
     * pending pong); a close is [close].
     *
     * @throws WebSocketException.AlreadyClosed after termination.
     * @throws WebSocketException.Protocol [ProtocolError.SendAfterClosing] once not active;
     *   ⚖️ [ProtocolError.ControlFrameTooBig] for a ping / pong over 125 bytes or a close reason
     *   over 123 bytes (SPEC §4.3).
     * @throws WebSocketException.Capacity ⚖️ a message over [WebSocketConfig.maxMessageSize].
     * @throws WebSocketException.WriteBufferFull [output] would exceed
     *   [WebSocketConfig.maxWriteBufferSize]; nothing was queued.
     */
    fun write(message: Message) {
        if (!tryWrite(message)) throw WebSocketException.WriteBufferFull(message)
    }

    /**
     * [write] that reports a full [output] by returning false instead of throwing
     * [WebSocketException.WriteBufferFull] (nothing was queued; the caller keeps [message]). A
     * driver that waits for room calls this, so waiting costs no exception. Other errors as [write].
     */
    fun tryWrite(message: Message): Boolean {
        checkNotTerminated()
        if (!state.isActive) protocolError(ProtocolError.SendAfterClosing)
        return when (message) {
            is Message.Text -> bufferData(OP_TEXT, message.text.bytes)
            is Message.Binary -> bufferData(OP_BINARY, message.data)
            is Message.Ping -> {
                checkControlSize(message.data.size)
                bufferFrame(0x80 or OP_PING, message.data, null)
            }
            is Message.Pong -> {
                checkControlSize(message.data.size)
                setReply(OP_PONG, message.data)
                true
            }
            is Message.Close -> { close(message.frame); true }
            is Message.Frame -> {
                val f = message.frame
                val size = f.payload.size
                if (f.header.opcode is OpCode.Control) checkControlSize(size) else checkMessageSize(size)
                bufferFrame(f.header.firstByte(), f.payload, f.header.mask?.let { packMask(it) })
            }
        }
    }

    /**
     * Start the close handshake (tungstenite `WebSocketContext::close`, `mod.rs:597-607`, without
     * the flush): only while active, enter [WebSocketState.ClosedByUs] and queue the close frame.
     * Otherwise nothing happens. The caller then flushes (the reference always does).
     *
     * ⚖️ The close frame goes to the reply slot, ahead of any pending pong (dropped: no pong after
     * our close) and never refused for a full [output]; [bufferReply] puts it behind the frames
     * already queued (SPEC §5).
     *
     * @throws WebSocketException.Protocol ⚖️ [ProtocolError.ControlFrameTooBig] for a reason over 123 bytes.
     */
    fun close(frame: CloseFrame? = null) {
        if (!state.isActive) return
        if (frame != null && frame.reason.size > 123) protocolError(ProtocolError.ControlFrameTooBig)
        state = WebSocketState.ClosedByUs
        replyOp = OP_CLOSE
        replyPayload = Frame.closePayload(frame)
    }

    /**
     * Move the pending reply (pong or close) behind the frames in [output] (tungstenite `_write`
     * with no new frame, `mod.rs:544-574`). Returns true when a reply was queued, which the
     * reference flushes at once; false when there is none or it does not fit yet (it stays).
     */
    fun bufferReply(): Boolean {
        val op = replyOp
        if (op == 0) return false
        val payload = replyPayload
        if (!codec.fits(headerSize(payload.size.toLong(), masks != null) + payload.size)) return false
        writeFrame(output, 0x80 or op, payload, masks != null, masks?.next() ?: 0)
        replyOp = 0
        replyPayload = Bytes.EMPTY
        replyQueued = true
        return true
    }

    /**
     * A reply was moved into [output] since the last call (by the reader or the writer): the writer flushes it at once,
     * as the reference does. Clears the flag.
     */
    fun takeReplyQueued(): Boolean = replyQueued.also { replyQueued = false }

    /** Whether a reply waits in the slot or was queued and not yet flushed. */
    val hasReplyToFlush: Boolean get() = replyOp != 0 || replyQueued

    /**
     * The driver wrote out all of [output] and flushed the stream. A server whose close handshake
     * is complete then terminates (tungstenite `_write`, `mod.rs:576-589`).
     * @throws WebSocketException.ConnectionClosed the server should now close the connection.
     */
    fun flushed() {
        if (role == Role.Server && !state.canRead && replyOp == 0 && allWritten) {
            terminated()
            throw WebSocketException.ConnectionClosed()
        }
    }

    /**
     * Hand the queued frames to the writer without copying them (the write driver, SPEC §5): the
     * returned buffer holds everything [output] held, and [empty] becomes the new [output], so
     * frames can be queued while the returned bytes are being written. Until [outputWritten], those
     * bytes still count against [WebSocketConfig.maxWriteBufferSize] and the connection is not
     * considered fully written.
     */
    fun takeOutput(empty: Buffer): Buffer = codec.takeOutput(empty)

    /** The buffer returned by [takeOutput] has been written out completely. */
    fun outputWritten() { codec.inFlight = 0 }

    /** Nothing queued and nothing being written. */
    private val allWritten: Boolean get() = output.isEmpty && codec.inFlight == 0

    /**
     * The exception for an I/O error of the stream (tungstenite `check_connection_reset`,
     * `mod.rs:830-848`): a connection reset after the peer's close is a normal end
     * ([WebSocketException.ConnectionClosed]); anything else is [WebSocketException.Io].
     */
    fun mapIoError(cause: Throwable, isConnectionReset: Boolean): WebSocketException =
        if (isConnectionReset && !state.canRead) WebSocketException.ConnectionClosed() else WebSocketException.Io(cause)

    /** The driver gave up on the connection (stream error, cancelled write): later calls fail with AlreadyClosed. */
    fun terminate() = terminated()

    /** The connection is over: no more reads or writes, and the compression state goes. */
    private fun terminated() {
        state = WebSocketState.Terminated
        deflater?.release(); deflater = null
        inflater?.release(); inflater = null
    }

    private fun checkNotTerminated() {
        if (state == WebSocketState.Terminated) throw WebSocketException.AlreadyClosed()
    }

    /** Replace the reply slot only if it is empty or holds a pong (tungstenite `set_additional`). */
    private fun setReply(op: Int, payload: Bytes) {
        if (replyOp == 0 || replyOp == OP_PONG) {
            replyOp = op
            replyPayload = payload
        }
    }

    private fun checkControlSize(size: Int) {
        if (size > 125) protocolError(ProtocolError.ControlFrameTooBig)
    }

    private fun checkMessageSize(size: Int) {
        val max = config.maxMessageSize ?: return
        if (size > max) throw WebSocketException.Capacity(CapacityError.MessageTooLong(size.toLong(), max.toLong()))
    }

    private fun bufferData(op: Int, payload: Bytes): Boolean {
        checkMessageSize(payload.size)
        val d = deflate ?: return bufferFrame(0x80 or op, payload, null)
        // ⚖️ permessage-deflate: one compressed frame with RSV1 (RFC 7692 §6). Checked against the compressed size's
        // bound first: a message that does not fit must leave the compressor untouched, as it is queued again later.
        val bound = deflateBound(payload.size)
        if (!codec.fits(headerSize(bound.toLong(), masks != null) + bound)) return false
        val def = deflater ?: rawDeflater(d.level, d.compressWindowBits).also { deflater = it }
        val out = ByteArray(bound)
        val n = def.compress(payload.toByteArray(), 0, payload.size, out, 0)
        if (d.compressNoContextTakeover) def.reset()
        return bufferFrame(0x80 or 0x40 or op, Bytes.copyOf(out, 0, n), null)
    }

    /**
     * Encode one frame into [output] (tungstenite `buffer_frame`, `mod.rs:755-770` and
     * `mod.rs` FrameCodec): a client masks with a fresh random key; a server sends unmasked unless
     * a raw frame carries its own key ([ownMask]). Returns false, queueing nothing, when it does not fit.
     */
    private fun bufferFrame(first: Int, payload: Bytes, ownMask: Int?): Boolean {
        val masked = masks != null || ownMask != null
        if (!codec.fits(headerSize(payload.size.toLong(), masked) + payload.size)) return false
        val mask = masks?.next() ?: ownMask ?: 0
        writeFrame(output, first, payload, masked, mask)
        return true
    }

    private companion object {
        val PROTOCOL_VIOLATION = Utf8Bytes.from("Protocol violation")
    }
}

/** Payloads up to this size are copied out of the read buffer; larger ones are handed out as slices of it. */
internal const val PAYLOAD_COPY_LIMIT = 32 * 1024

/**
 * Take a message payload out of [input]. A slice would make the buffer share its array with the message, so the next
 * read has to move to a fresh array of the whole read-buffer size (128 KiB by default) — per message. A small payload
 * is copied instead (one right-sized array) and the read buffer keeps its array; a large one is still a zero-copy slice.
 */
internal fun takePayload(input: neton.io.bytes.Buffer, len: Int): Bytes {
    if (len > PAYLOAD_COPY_LIMIT) return input.readSlice(len)
    if (len == 0) return Bytes.EMPTY
    val at = input.readerIndex()
    val out = Bytes.copyOf(input.backingArray(), at, at + len)
    input.skip(len)
    return out
}
