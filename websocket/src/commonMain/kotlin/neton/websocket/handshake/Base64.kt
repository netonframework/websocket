package neton.websocket.handshake

/**
 * Standard base64 with padding (RFC 4648 §4), as the reference's `data_encoding::BASE64`: used for
 * `Sec-WebSocket-Key` and `Sec-WebSocket-Accept` (SPEC §3.1, §9).
 */
internal object Base64 {
    private const val ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"

    private val DECODE = IntArray(128) { -1 }.also { t -> for (i in ALPHABET.indices) t[ALPHABET[i].code] = i }

    fun encode(src: ByteArray): String {
        val sb = StringBuilder((src.size + 2) / 3 * 4)
        var i = 0
        while (i + 3 <= src.size) {
            val v = ((src[i].toInt() and 0xFF) shl 16) or ((src[i + 1].toInt() and 0xFF) shl 8) or (src[i + 2].toInt() and 0xFF)
            sb.append(ALPHABET[v ushr 18]).append(ALPHABET[(v ushr 12) and 63]).append(ALPHABET[(v ushr 6) and 63]).append(ALPHABET[v and 63])
            i += 3
        }
        when (src.size - i) {
            1 -> {
                val v = (src[i].toInt() and 0xFF) shl 16
                sb.append(ALPHABET[v ushr 18]).append(ALPHABET[(v ushr 12) and 63]).append("==")
            }
            2 -> {
                val v = ((src[i].toInt() and 0xFF) shl 16) or ((src[i + 1].toInt() and 0xFF) shl 8)
                sb.append(ALPHABET[v ushr 18]).append(ALPHABET[(v ushr 12) and 63]).append(ALPHABET[(v ushr 6) and 63]).append('=')
            }
        }
        return sb.toString()
    }

    /**
     * Decode `src[offset, offset + length)`, or null if it is not canonical padded base64: the
     * length must be a multiple of 4, `=` only as the last one or two characters, and the unused
     * bits of the last symbol zero (`data_encoding::BASE64` checks trailing bits).
     */
    fun decode(src: ByteArray, offset: Int = 0, length: Int = src.size - offset): ByteArray? {
        if (length % 4 != 0) return null
        if (length == 0) return ByteArray(0)
        val end = offset + length
        val pad = when {
            src[end - 2] == '='.code.toByte() -> 2
            src[end - 1] == '='.code.toByte() -> 1
            else -> 0
        }
        if (pad == 2 && src[end - 1] != '='.code.toByte()) return null
        val out = ByteArray(length / 4 * 3 - pad)
        var o = 0
        var i = offset
        while (i < end) {
            var v = 0
            val symbols = if (i + 4 == end) 4 - pad else 4
            for (k in 0 until 4) {
                val d = if (k < symbols) symbol(src[i + k]) else 0
                if (d < 0) return null
                v = (v shl 6) or d
            }
            out[o++] = (v ushr 16).toByte()
            if (symbols > 2) out[o++] = (v ushr 8).toByte()
            if (symbols > 3) out[o++] = v.toByte()
            if (symbols == 2 && (v and 0xFFFF) != 0) return null
            if (symbols == 3 && (v and 0xFF) != 0) return null
            i += 4
        }
        return out
    }

    private fun symbol(b: Byte): Int {
        val c = b.toInt() and 0xFF
        return if (c < 128) DECODE[c] else -1
    }
}
