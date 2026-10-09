package neton.websocket.frame

import neton.io.bytes.Buffer
import neton.io.bytes.Bytes
import neton.websocket.CapacityError
import neton.websocket.Message
import neton.websocket.ProtocolError
import neton.websocket.WebSocketException
import neton.websocket.protocolError

/**
 * Frame encoder / decoder over two buffers (tungstenite `FrameCodec`, `T/src/protocol/frame/mod.rs:92-291`),
 * without I/O: a driver appends received bytes to [input] and writes out what is in [output].
 *
 * Reading keeps a parsed header in [header] until its payload has arrived; incomplete input is
 * never consumed. Nothing is allocated per frame except the payload slice (zero-copy out of
 * [input]).
 */
internal class FrameCodec(readBufferSize: Int, prefix: Bytes?) {
    /** Received bytes not yet decoded. Pooled: the array goes back to the pool when idle. */
    val input: Buffer = Buffer(maxOf(readBufferSize, FrameHeader.MAX_SIZE), pooled = true)

    /** Encoded frames not yet written out; [takeOutput] swaps it for an empty buffer. */
    var output: Buffer = Buffer(pooled = true)
        private set

    /** Bytes handed to the writer by [takeOutput] and not yet written; they count against [maxOutBufferLen]. */
    var inFlight: Int = 0

    /** The most one read should ask for (`in_buf_max_read`, `mod.rs:124`). */
    val maxReadSize: Int = maxOf(readBufferSize, FrameHeader.MAX_SIZE)

    /** Bound on [output] (`max_out_buffer_len`). */
    var maxOutBufferLen: Int = Int.MAX_VALUE

    /** [output] should be written out once longer than this (`out_buffer_write_len`). */
    var outBufferWriteLen: Int = 0

    /** The header of the frame being read, valid while [hasHeader]. */
    val header = RawHeader()
    var hasHeader = false
    var beforeDataPayload: ((Int) -> Unit)? = null
    var discardData = false

    init { if (prefix != null) input.writeBytes(prefix) }

    /**
     * Make sure a header is pending, parsing one if needed (`read_frame`, `mod.rs:162-190`).
     * Returns false when the input does not yet hold a whole header.
     *
     * The payload length is checked against [maxSize] as a 64-bit value before anything is
     * reserved; then room for the whole payload is reserved once.
     */
    fun pollHeader(maxSize: Int?): Boolean {
        if (hasHeader) return true
        if (!header.parse(input.backingArray(), input.readerIndex(), input.readableBytes)) return false
        input.skip(header.size)
        hasHeader = true
        val max = maxSize ?: Int.MAX_VALUE
        if (header.length > max) {
            throw WebSocketException.Capacity(CapacityError.MessageTooLong(header.length, max.toLong()))
        }
        if (!header.isControl && !discardData) beforeDataPayload?.invoke(header.length.toInt())
        val missing = header.length.toInt() - input.readableBytes
        if (missing > 0 && (!discardData || header.isControl)) input.reserve(missing)
        return true
    }

    /** Whether the pending header's payload is complete in [input]. */
    fun payloadReady(): Boolean = input.readableBytes >= header.length

    /**
     * Read one frame as a [Frame] (`FrameCodec::read_frame`); null when [input] does not hold a
     * whole frame. With [unmask], a masked payload is unmasked in place and an unmasked one is
     * rejected unless [acceptUnmasked].
     */
    fun readFrame(maxSize: Int?, unmask: Boolean, acceptUnmasked: Boolean): Frame? {
        if (!pollHeader(maxSize) || !payloadReady()) return null
        hasHeader = false
        val len = header.length.toInt()
        val h = header.toHeader()
        if (unmask) {
            if (header.masked) {
                val off = input.readerIndex()
                applyMask(input.backingArray(), off, off + len, header.mask)
                h.mask = null
            } else if (!acceptUnmasked) {
                input.skip(len)
                protocolError(ProtocolError.UnmaskedFrameFromClient)
            }
        }
        return Frame(h, neton.websocket.takePayload(input, len))
    }

    /** Whether a frame of [frameLen] bytes may be queued now (bytes in flight count too). */
    fun fits(frameLen: Int): Boolean =
        (output.isEmpty && inFlight == 0) || output.readableBytes.toLong() + inFlight + frameLen <= maxOutBufferLen

    /** Hand the queued bytes to a writer: [empty] becomes [output], the old one is returned and counted in [inFlight]. */
    fun takeOutput(empty: Buffer): Buffer {
        require(empty.isEmpty) { "the replacement output buffer must be empty" }
        check(inFlight == 0) { "the previous output is still being written" }
        val out = output
        output = empty
        inFlight = out.readableBytes
        return out
    }

    /**
     * Queue [frame] into [output] (`FrameCodec::buffer_frame`, `mod.rs:250-270`).
     * @throws WebSocketException.WriteBufferFull if it does not fit.
     */
    fun bufferFrame(frame: Frame) {
        if (!fits(frame.length)) throw WebSocketException.WriteBufferFull(Message.Frame(frame))
        frame.formatIntoBuf(output)
    }

    /** Whether [output] has grown past [outBufferWriteLen] and should be written out now. */
    val wantsWrite: Boolean get() = output.readableBytes > outBufferWriteLen
}

/**
 * Frame-level reading and writing (tungstenite `FrameSocket`, `T/src/protocol/frame/mod.rs:27-90`),
 * sans I/O: the driver appends received bytes to [input], calls [receivedEof] at end of stream, and
 * writes out and flushes [output]. Reading always unmasks and accepts unmasked frames; no limit on
 * [output]. The read buffer is 128 KiB like the reference.
 *
 * @param prefix bytes already read past the handshake (`from_partially_read`).
 */
class FrameSocket(prefix: Bytes? = null) {
    private val codec = FrameCodec(READ_BUF_LEN, prefix)

    /** Received, not yet decoded bytes; the driver appends to it. */
    val input: Buffer get() = codec.input

    /** Encoded frames to send; the driver writes them out. */
    val output: Buffer get() = codec.output

    /** The most one read should ask for. */
    val maxReadSize: Int get() = codec.maxReadSize

    /** True after [receivedEof]. */
    var isEof: Boolean = false
        private set

    /** The stream reached its end. */
    fun receivedEof() { isEof = true }

    /**
     * The next frame, or null when [input] does not hold a whole one: read more, or, when [isEof],
     * the stream is over (the reference's `Ok(None)`).
     * @throws WebSocketException.Capacity when a payload is longer than [maxSize].
     */
    fun read(maxSize: Int? = null): Frame? = codec.readFrame(maxSize, unmask = true, acceptUnmasked = true)

    /** Queue [frame] in [output] (the reference's `write`); the driver writes and flushes. */
    fun write(frame: Frame) = codec.bufferFrame(frame)

    /** Bytes still in [input]: the reference's `into_inner().1`. */
    fun remaining(): ByteArray = codec.input.peekAll()

    private companion object {
        const val READ_BUF_LEN = 128 * 1024
    }
}
