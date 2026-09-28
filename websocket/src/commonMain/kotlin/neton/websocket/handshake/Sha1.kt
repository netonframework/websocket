package neton.websocket.handshake

/**
 * SHA-1 (RFC 3174, FIPS 180-4), for `Sec-WebSocket-Accept` only (SPEC §3.1, §9: the reference uses
 * the `sha1` crate). SHA-1 is not collision resistant; RFC 6455 uses it as a fixed transform,
 * not for security.
 */
internal class Sha1 {
    private val h = intArrayOf(0x67452301, 0xEFCDAB89.toInt(), 0x98BADCFE.toInt(), 0x10325476, 0xC3D2E1F0.toInt())
    private val block = ByteArray(64)
    private var blockLen = 0
    private var total = 0L
    private val w = IntArray(80)

    fun update(src: ByteArray, offset: Int = 0, length: Int = src.size - offset): Sha1 {
        var i = offset
        val end = offset + length
        total += length
        while (i < end) {
            val n = minOf(64 - blockLen, end - i)
            src.copyInto(block, blockLen, i, i + n)
            blockLen += n
            i += n
            if (blockLen == 64) { compress(); blockLen = 0 }
        }
        return this
    }

    /** The 20-byte digest; the instance must not be used afterwards. */
    fun digest(): ByteArray {
        val bits = total * 8
        block[blockLen++] = 0x80.toByte()
        if (blockLen > 56) {
            block.fill(0, blockLen, 64)
            compress()
            blockLen = 0
        }
        block.fill(0, blockLen, 56)
        for (k in 0 until 8) block[56 + k] = (bits ushr (56 - 8 * k)).toByte()
        compress()
        val out = ByteArray(20)
        for (k in 0 until 5) {
            val v = h[k]
            out[4 * k] = (v ushr 24).toByte()
            out[4 * k + 1] = (v ushr 16).toByte()
            out[4 * k + 2] = (v ushr 8).toByte()
            out[4 * k + 3] = v.toByte()
        }
        return out
    }

    private fun compress() {
        for (t in 0 until 16) {
            val p = 4 * t
            w[t] = ((block[p].toInt() and 0xFF) shl 24) or ((block[p + 1].toInt() and 0xFF) shl 16) or
                ((block[p + 2].toInt() and 0xFF) shl 8) or (block[p + 3].toInt() and 0xFF)
        }
        for (t in 16 until 80) w[t] = (w[t - 3] xor w[t - 8] xor w[t - 14] xor w[t - 16]).rotateLeft(1)
        var a = h[0]; var b = h[1]; var c = h[2]; var d = h[3]; var e = h[4]
        for (t in 0 until 80) {
            val f: Int
            val k: Int
            when {
                t < 20 -> { f = (b and c) or (b.inv() and d); k = 0x5A827999 }
                t < 40 -> { f = b xor c xor d; k = 0x6ED9EBA1 }
                t < 60 -> { f = (b and c) or (b and d) or (c and d); k = 0x8F1BBCDC.toInt() }
                else -> { f = b xor c xor d; k = 0xCA62C1D6.toInt() }
            }
            val temp = a.rotateLeft(5) + f + e + k + w[t]
            e = d; d = c; c = b.rotateLeft(30); b = a; a = temp
        }
        h[0] += a; h[1] += b; h[2] += c; h[3] += d; h[4] += e
    }
}

/** SHA-1 of [data]. */
internal fun sha1(data: ByteArray): ByteArray = Sha1().update(data).digest()
