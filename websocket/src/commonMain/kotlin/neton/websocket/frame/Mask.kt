@file:OptIn(kotlin.experimental.ExperimentalNativeApi::class)

package neton.websocket.frame

import neton.io.core.secureRandom
import kotlin.experimental.xor
import kotlin.native.getLongAt
import kotlin.native.setLongAt

// Masking (RFC 6455 §5.3; tungstenite `T/src/protocol/frame/mask.rs`).
//
// A mask is carried as an Int holding the four key bytes in wire order, big-endian: key byte 0 is
// the most significant byte. Masking is in place, allocates nothing, and is its own inverse.

/** Key byte `k` (0..3) of [mask]. */
@Suppress("NOTHING_TO_INLINE")
internal inline fun maskByte(mask: Int, k: Int): Byte = (mask ushr (24 - 8 * k)).toByte()

/** Byte-at-a-time reference (tungstenite `apply_mask_fallback`, `mask.rs:14-19`). */
internal fun applyMaskFallback(a: ByteArray, from: Int, to: Int, mask: Int) {
    for (i in from until to) a[i] = a[i] xor maskByte(mask, (i - from) and 3)
}

/**
 * Mask or unmask `a[from, to)` in place with the key [mask], as if the key started at [from]
 * (tungstenite `apply_mask_fast32`, `mask.rs:22-44`).
 *
 * ⚖️ Works on 8-byte words (the reference: 4-byte words, SPEC §4.3): bytes up to the next index
 * that is a multiple of 8 are done one at a time (Kotlin/Native aligns array data to 8 bytes, so
 * this aligns the words), then whole words with the key rotated to the phase reached, then the
 * tail. Equivalence with [applyMaskFallback] is tested at every offset 0–7 and length 0–32.
 */
internal fun applyMask(a: ByteArray, from: Int, to: Int, mask: Int) {
    if (to - from < 16 || !Platform.isLittleEndian) { applyMaskFallback(a, from, to, mask); return }
    var i = from
    while ((i and 7) != 0) { a[i] = a[i] xor maskByte(mask, (i - from) and 3); i++ }
    // Key byte for index i is ((i - from) & 3); in a little-endian word the lowest byte comes first.
    val r = mask.rotateLeft(8 * ((i - from) and 3))
    val le = ((r ushr 24) and 0xFF) or ((r ushr 8) and 0xFF00) or ((r shl 8) and 0xFF0000) or (r shl 24)
    val w = (le.toLong() and 0xFFFFFFFFL) or (le.toLong() shl 32)
    val end = to - 8
    while (i <= end) { a.setLongAt(i, a.getLongAt(i) xor w); i += 8 }
    while (i < to) { a[i] = a[i] xor maskByte(mask, (i - from) and 3); i++ }
}

/** Pack four key bytes (wire order) into the Int form used by [applyMask]. */
internal fun packMask(k: ByteArray): Int =
    ((k[0].toInt() and 0xFF) shl 24) or ((k[1].toInt() and 0xFF) shl 16) or ((k[2].toInt() and 0xFF) shl 8) or (k[3].toInt() and 0xFF)

/** Unpack the Int form into four key bytes (wire order). */
internal fun unpackMask(mask: Int): ByteArray = byteArrayOf(maskByte(mask, 0), maskByte(mask, 1), maskByte(mask, 2), maskByte(mask, 3))

/** A fresh random key (tungstenite `generate_mask`, `mask.rs:1-5`), from the OS CSPRNG. */
fun generateMask(): ByteArray = ByteArray(4).also { secureRandom(it) }

/**
 * Random mask keys for one connection. Keys come from the OS CSPRNG ([secureRandom]), fetched 16 at
 * a time so a client does not make one system call per frame; each key is still independent and
 * unpredictable before it is sent (RFC 6455 §5.3).
 */
internal class MaskSource {
    private val pool = ByteArray(64)
    private var pos = pool.size

    fun next(): Int {
        if (pos == pool.size) { secureRandom(pool); pos = 0 }
        val p = pos
        pos = p + 4
        return ((pool[p].toInt() and 0xFF) shl 24) or ((pool[p + 1].toInt() and 0xFF) shl 16) or
            ((pool[p + 2].toInt() and 0xFF) shl 8) or (pool[p + 3].toInt() and 0xFF)
    }
}
