package neton.websocket.frame

import neton.io.bytes.Buffer
import neton.io.bytes.Bytes
import neton.websocket.ProtocolError
import neton.websocket.Utf8Bytes
import neton.websocket.Utf8Validator
import neton.websocket.WebSocketException
import neton.websocket.protocolError

/**
 * The close command: a status code and a reason (tungstenite `CloseFrame`, `frame.rs:23-36`).
 * `toString()` is the reference's `Display`: `"<reason> (<code>)"`.
 */
data class CloseFrame(val code: CloseCode, val reason: Utf8Bytes = Utf8Bytes.EMPTY) {
    constructor(code: CloseCode, reason: String) : this(code, Utf8Bytes.from(reason))

    override fun toString(): String = "$reason ($code)"
}

/**
 * A frame header (tungstenite `FrameHeader`, `frame.rs:38-203`). Mutable like the reference's
 * `header_mut()`. [mask] is the 4-byte key in wire order, or null for an unmasked frame.
 */
class FrameHeader(
    var isFinal: Boolean = true,
    var rsv1: Boolean = false,
    var rsv2: Boolean = false,
    var rsv3: Boolean = false,
    var opcode: OpCode = OpCode.Control.Close,
    var mask: ByteArray? = null,
) {
    init { mask?.let { require(it.size == 4) { "mask must be 4 bytes" } } }

    /** Size of this header when formatted for a payload of [length] bytes. */
    fun len(length: Long): Int = headerSize(length, mask != null)

    /** Write this header for a payload of [length] bytes (tungstenite `FrameHeader::format`). */
    fun format(length: Long, output: Buffer) {
        writeHeader(output, firstByte(), length, mask?.let { packMask(it) }, mask != null)
    }

    /** Give the header a fresh random mask (the payload is masked when formatted). */
    internal fun setRandomMask() { mask = generateMask() }

    internal fun firstByte(): Int = opcode.code or (if (isFinal) 0x80 else 0) or
        (if (rsv1) 0x40 else 0) or (if (rsv2) 0x20 else 0) or (if (rsv3) 0x10 else 0)

    override fun equals(other: Any?): Boolean =
        other is FrameHeader && isFinal == other.isFinal && rsv1 == other.rsv1 && rsv2 == other.rsv2 &&
            rsv3 == other.rsv3 && opcode == other.opcode && (mask?.contentEquals(other.mask) ?: (other.mask == null))

    override fun hashCode(): Int = firstByte() * 31 + (mask?.contentHashCode() ?: 0)

    override fun toString(): String =
        "FrameHeader(isFinal=$isFinal, rsv1=$rsv1, rsv2=$rsv2, rsv3=$rsv3, opcode=$opcode, mask=${mask?.contentToString()})"

    /** A parsed header and the payload length that follows it. */
    data class Parsed(val header: FrameHeader, val length: Long)

    companion object {
        /** The longest possible header: 2 + 8 (64-bit length) + 4 (mask). */
        const val MAX_SIZE = 14

        /**
         * Parse a header from the readable bytes of [input] (tungstenite `FrameHeader::parse`,
         * `frame.rs:73-88`). Returns null if the header is incomplete, consuming nothing; else
         * consumes the header and returns it with its payload length.
         *
         * @throws WebSocketException.Protocol [ProtocolError.InvalidOpcode] for a reserved opcode;
         *   ⚖️ [ProtocolError.InvalidPayloadLength] for a 64-bit length with the top bit set.
         */
        fun parse(input: Buffer): Parsed? {
            val raw = RawHeader()
            if (!raw.parse(input.backingArray(), input.readerIndex(), input.readableBytes)) return null
            input.skip(raw.size)
            return Parsed(raw.toHeader(), raw.length)
        }
    }
}

/**
 * The header fields as primitives: what the codec parses into on its hot path, so reading a frame
 * allocates nothing but the payload slice (the reference builds a `FrameHeader` per frame).
 */
internal class RawHeader {
    var first = 0
    var masked = false
    var mask = 0
    var length = 0L
    /** Encoded header size in bytes. */
    var size = 0

    val isFinal: Boolean get() = first and 0x80 != 0
    val rsvBits: Int get() = first and 0x70
    val opcode: Int get() = first and 0x0F
    val isControl: Boolean get() = first and 0x08 != 0

    /**
     * Parse from `a[off, off + avail)` (tungstenite `FrameHeader::parse_internal`, `frame.rs:138-203`).
     * Returns false when the header is incomplete; the fields are then unspecified.
     */
    fun parse(a: ByteArray, off: Int, avail: Int): Boolean {
        if (avail < 2) return false
        val b0 = a[off].toInt() and 0xFF
        val b1 = a[off + 1].toInt() and 0xFF
        val lenByte = b1 and 0x7F
        val extra = when (lenByte) { 126 -> 2; 127 -> 8; else -> 0 }
        val isMasked = b1 and 0x80 != 0
        val total = 2 + extra + (if (isMasked) 4 else 0)
        if (avail < total) return false
        var p = off + 2
        val len: Long = when (extra) {
            0 -> lenByte.toLong()
            2 -> (((a[p].toInt() and 0xFF) shl 8) or (a[p + 1].toInt() and 0xFF)).toLong()
            else -> {
                var v = 0L
                for (k in 0 until 8) v = (v shl 8) or (a[p + k].toLong() and 0xFF)
                v
            }
        }
        p += extra
        val m = if (isMasked) {
            ((a[p].toInt() and 0xFF) shl 24) or ((a[p + 1].toInt() and 0xFF) shl 16) or
                ((a[p + 2].toInt() and 0xFF) shl 8) or (a[p + 3].toInt() and 0xFF)
        } else 0
        // Disallow bad opcodes, after the whole header is known (like the reference).
        val op = b0 and 0x0F
        if (op in 3..7 || op >= 11) protocolError(ProtocolError.InvalidOpcode(op))
        // ⚖️ RFC 6455 §5.2: the most significant bit of a 64-bit length MUST be 0.
        if (len < 0) protocolError(ProtocolError.InvalidPayloadLength)
        first = b0; masked = isMasked; mask = m; length = len; size = total
        return true
    }

    fun toHeader(): FrameHeader = FrameHeader(
        isFinal = isFinal, rsv1 = first and 0x40 != 0, rsv2 = first and 0x20 != 0, rsv3 = first and 0x10 != 0,
        opcode = OpCode.from(opcode), mask = if (masked) unpackMask(mask) else null,
    )
}

/** Size of a header for [length] payload bytes (tungstenite `LengthFormat`, `frame.rs:432-481`). */
internal fun headerSize(length: Long, masked: Boolean): Int =
    2 + (if (length < 126) 0 else if (length < 65536) 2 else 8) + (if (masked) 4 else 0)

/** Write a header: [first] is FIN | RSV | opcode; [mask] is written when [masked]. */
internal fun writeHeader(out: Buffer, first: Int, length: Long, mask: Int?, masked: Boolean) {
    val maskBit = if (masked) 0x80 else 0
    when {
        length < 126 -> out.writeShort((first shl 8) or maskBit or length.toInt())
        length < 65536 -> { out.writeShort((first shl 8) or maskBit or 126); out.writeShort(length.toInt()) }
        else -> { out.writeShort((first shl 8) or maskBit or 127); out.writeLong(length) }
    }
    if (masked) out.writeInt(mask ?: 0)
}

/**
 * Write a whole frame into [out]: header, then the payload copied once and masked in place when
 * [masked] (tungstenite `Frame::format_into_buf`, `frame.rs:362-373`).
 */
internal fun writeFrame(out: Buffer, first: Int, payload: Bytes, masked: Boolean, mask: Int) {
    writeHeader(out, first, payload.size.toLong(), mask, masked)
    if (payload.size == 0) return
    out.writeBytes(payload)
    if (masked) {
        val end = out.writerIndex()
        applyMask(out.backingArray(), end - payload.size, end, mask)
    }
}

/**
 * A WebSocket frame: header and payload (tungstenite `Frame`, `frame.rs:205-429`).
 * `toString()` is the reference's `Display`.
 */
class Frame(
    /** The header (mutable, like the reference's `header_mut()`). */
    val header: FrameHeader,
    /** The payload, unmasked. */
    val payload: Bytes,
) {
    /** Header length plus payload length. */
    val length: Int get() = header.len(payload.size.toLong()) + payload.size

    val isEmpty: Boolean get() = length == 0

    internal val isMasked: Boolean get() = header.mask != null

    internal fun setRandomMask() = header.setRandomMask()

    /** The payload as text; @throws WebSocketException.Utf8 if not UTF-8. */
    fun intoText(): Utf8Bytes = Utf8Bytes.tryFrom(payload)

    /** The payload. */
    fun intoPayload(): Bytes = payload

    /** The payload decoded as a [String]; @throws WebSocketException.Utf8 if not UTF-8. */
    fun toText(): String = intoText().asString()

    /**
     * The close frame this frame carries (tungstenite `Frame::into_close`, `frame.rs:290-301`):
     * empty payload → null; one byte → [ProtocolError.InvalidCloseSequence]; else code + UTF-8 reason.
     */
    internal fun intoClose(): CloseFrame? = when (payload.size) {
        0 -> null
        1 -> protocolError(ProtocolError.InvalidCloseSequence)
        else -> CloseFrame(
            CloseCode.from(((payload[0].toInt() and 0xFF) shl 8) or (payload[1].toInt() and 0xFF)),
            Utf8Bytes.tryFrom(payload.slice(2)),
        )
    }

    /** Write the frame (header, then the payload masked if the header has a mask). */
    fun format(output: Buffer) {
        val m = header.mask
        writeFrame(output, header.firstByte(), payload, m != null, if (m != null) packMask(m) else 0)
    }

    /** Same as [format]; the reference keeps both (`format` to a writer, `format_into_buf` to a Vec). */
    internal fun formatIntoBuf(output: Buffer) = format(output)

    override fun equals(other: Any?): Boolean = other is Frame && header == other.header && payload == other.payload

    override fun hashCode(): Int = header.hashCode() * 31 + payload.hashCode()

    override fun toString(): String {
        val sb = StringBuilder()
        for (i in 0 until payload.size) {
            val b = payload[i].toInt() and 0xFF
            sb.append(HEX[b ushr 4]).append(HEX[b and 15])
        }
        return """
<FRAME>
final: ${header.isFinal}
reserved: ${header.rsv1} ${header.rsv2} ${header.rsv3}
opcode: ${header.opcode}
length: $length
payload length: ${payload.size}
payload: 0x$sb
            """
    }

    companion object {
        private const val HEX = "0123456789abcdef"

        /** A data frame (tungstenite `Frame::message`). */
        fun message(data: Bytes, opcode: OpCode.Data, isFinal: Boolean): Frame =
            Frame(FrameHeader(isFinal = isFinal, opcode = opcode), data)

        /** A pong frame. */
        fun pong(data: Bytes): Frame = Frame(FrameHeader(opcode = OpCode.Control.Pong), data)

        /** A ping frame. */
        fun ping(data: Bytes): Frame = Frame(FrameHeader(opcode = OpCode.Control.Ping), data)

        /** A close frame; null gives an empty payload. */
        fun close(msg: CloseFrame?): Frame = Frame(FrameHeader(), closePayload(msg))

        /** A frame from a header and a payload. */
        fun fromPayload(header: FrameHeader, payload: Bytes): Frame = Frame(header, payload)

        internal fun closePayload(msg: CloseFrame?): Bytes {
            if (msg == null) return Bytes.EMPTY
            val reason = msg.reason.bytes
            val p = ByteArray(2 + reason.size)
            p[0] = (msg.code.code ushr 8).toByte()
            p[1] = msg.code.code.toByte()
            reason.copyInto(p, 2)
            return Bytes.wrap(p)
        }
    }
}
