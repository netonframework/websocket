@file:OptIn(kotlin.experimental.ExperimentalNativeApi::class)

package neton.websocket

import neton.io.bytes.Bytes
import kotlin.native.getLongAt

/**
 * Incremental UTF-8 validation (RFC 3629) across any number of chunks, without decoding or
 * allocating (the role of tungstenite `T/src/utf8.rs` plus `StringCollector`, `message.rs:8-76`).
 *
 * The state remembers where a code point split across chunks stands (at most 3 bytes seen), which
 * is what the reference's 4-byte `Incomplete` buffer holds. Runs of ASCII are skipped 8 bytes at a
 * time.
 */
internal class Utf8Validator {
    /** [ACCEPT] between code points, [REJECT] after an error, anything else mid-sequence. */
    private var state = ACCEPT

    /** True when the input so far ends on a code point boundary with no error. */
    val isComplete: Boolean get() = state == ACCEPT

    fun reset() { state = ACCEPT }

    /**
     * Feed `a[from, to)`. Returns -1 when valid so far (possibly ending inside a code point),
     * otherwise the index in [a] of the byte that makes the input invalid.
     */
    fun feed(a: ByteArray, from: Int, to: Int): Int {
        var s = state
        if (s == REJECT) return from
        var i = from
        while (i < to) {
            if (s == ACCEPT) {
                // ASCII fast path, one word at a time.
                while (i + 8 <= to && (a.getLongAt(i) and HIGH_BITS) == 0L) i += 8
                if (i >= to) break
                val b = a[i].toInt()
                if (b >= 0) { i++; continue }
            }
            s = step(s, a[i].toInt() and 0xFF)
            if (s == REJECT) { state = REJECT; return i }
            i++
        }
        state = s
        return -1
    }

    /** Feed one byte; false once the input is invalid. */
    fun feedByte(b: Int): Boolean {
        if (state == REJECT) return false
        state = step(state, b and 0xFF)
        return state != REJECT
    }

    companion object {
        private const val HIGH_BITS = -0x7f7f7f7f7f7f7f80L // 0x8080808080808080
        const val ACCEPT = 0
        const val REJECT = -1
        // 1..3: that many continuation bytes (80..BF) still expected.
        private const val AFTER_E0 = 4 // next A0..BF, then 1 more
        private const val AFTER_ED = 5 // next 80..9F (no surrogates), then 1 more
        private const val AFTER_F0 = 6 // next 90..BF (no overlongs), then 2 more
        private const val AFTER_F4 = 7 // next 80..8F (<= U+10FFFF), then 2 more

        private fun step(s: Int, b: Int): Int = when (s) {
            ACCEPT -> when {
                b < 0x80 -> ACCEPT
                b < 0xC2 -> REJECT
                b < 0xE0 -> 1
                b == 0xE0 -> AFTER_E0
                b == 0xED -> AFTER_ED
                b < 0xF0 -> 2
                b == 0xF0 -> AFTER_F0
                b < 0xF4 -> 3
                b == 0xF4 -> AFTER_F4
                else -> REJECT
            }
            1, 2, 3 -> if (b and 0xC0 == 0x80) s - 1 else REJECT
            AFTER_E0 -> if (b in 0xA0..0xBF) 1 else REJECT
            AFTER_ED -> if (b in 0x80..0x9F) 1 else REJECT
            AFTER_F0 -> if (b in 0x90..0xBF) 2 else REJECT
            AFTER_F4 -> if (b in 0x80..0x8F) 2 else REJECT
            else -> REJECT
        }

        /**
         * Validate `a[from, to)` as a complete text; null when valid, else the error detail
         * (the wording of Rust's `Utf8Error`, which the reference reports).
         */
        fun check(a: ByteArray, from: Int, to: Int): String? {
            val v = Utf8Validator()
            val bad = v.feed(a, from, to)
            if (bad >= 0) return "invalid utf-8 sequence from index ${bad - from}"
            if (!v.isComplete) return "incomplete utf-8 byte sequence at the end of ${to - from} bytes"
            return null
        }

        /** [check] for a [Bytes] slice, reading it in place. */
        fun check(b: Bytes): String? {
            val v = Utf8Validator()
            for (i in 0 until b.size) {
                if (!v.feedByte(b[i].toInt())) return "invalid utf-8 sequence from index $i"
            }
            if (!v.isComplete) return "incomplete utf-8 byte sequence at the end of ${b.size} bytes"
            return null
        }
    }
}

/**
 * Bytes guaranteed to be valid UTF-8 (tungstenite `Utf8Bytes`, `T/src/protocol/frame/utf8.rs`).
 *
 * Wraps a neton-io [Bytes] slice: validation reads it in place, and the [String] is decoded only
 * when asked for ([asString] / [toString]) and then kept. Equality and ordering are by content
 * (byte order equals code point order); [hashCode] equals that of the [String], like the
 * reference's `Hash` equals that of `&str`.
 */
class Utf8Bytes private constructor(
    /** The UTF-8 bytes. */
    val bytes: Bytes,
    private var string: String?,
) : Comparable<Utf8Bytes> {

    /** Length in bytes (the reference derefs to `str`, whose `len` is in bytes). */
    val size: Int get() = bytes.size

    val isEmpty: Boolean get() = bytes.size == 0

    /** The text (decoded once, then cached). */
    fun asString(): String = string ?: bytes.decodeToString().also { string = it }

    override fun toString(): String = asString()

    override fun equals(other: Any?): Boolean =
        this === other || (other is Utf8Bytes && bytes == other.bytes)

    override fun hashCode(): Int = asString().hashCode()

    override fun compareTo(other: Utf8Bytes): Int {
        val n = minOf(size, other.size)
        for (i in 0 until n) {
            val c = (bytes[i].toInt() and 0xFF) - (other.bytes[i].toInt() and 0xFF)
            if (c != 0) return c
        }
        return size - other.size
    }

    companion object {
        val EMPTY: Utf8Bytes = Utf8Bytes(Bytes.EMPTY, "")

        /** From a [String] (encoded once; the reference's `From<String>` / `From<&str>`). */
        fun from(text: String): Utf8Bytes =
            if (text.isEmpty()) EMPTY else Utf8Bytes(Bytes.wrap(text.encodeToByteArray()), text)

        /**
         * Validate [bytes] in place (the reference's `TryFrom<Bytes>`).
         * @throws WebSocketException.Utf8 if not valid UTF-8.
         */
        fun tryFrom(bytes: Bytes): Utf8Bytes {
            Utf8Validator.check(bytes)?.let { throw WebSocketException.Utf8(it) }
            return Utf8Bytes(bytes, null)
        }

        /** Validate a copy of [data] (the reference's `TryFrom<Vec<u8>>`). */
        fun tryFrom(data: ByteArray): Utf8Bytes {
            Utf8Validator.check(data, 0, data.size)?.let { throw WebSocketException.Utf8(it) }
            return Utf8Bytes(Bytes.copyOf(data), null)
        }

        /** Wrap bytes already validated (the reference's `from_bytes_unchecked`). */
        internal fun unchecked(bytes: Bytes): Utf8Bytes = if (bytes.size == 0) EMPTY else Utf8Bytes(bytes, null)
    }
}
