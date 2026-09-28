package neton.websocket.frame

import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertNotEquals

/** Ported from `T/src/protocol/frame/mask.rs` tests (1). */
class MaskTest {
    @Test fun testApplyMask() {
        val mask = packMask(byteArrayOf(0x6d, 0xb6.toByte(), 0xb2.toByte(), 0x80.toByte()))
        val unmasked = intArrayOf(0xf3, 0x00, 0x01, 0x02, 0x03, 0x80, 0x81, 0x82, 0xff, 0xfe, 0x00, 0x17, 0x74, 0xf9, 0x12, 0x03)
            .map { it.toByte() }.toByteArray()
        for (dataLen in 0..unmasked.size) {
            // Check masking with different alignment.
            for (off in 0..3) {
                if (dataLen < off) continue
                val masked = unmasked.copyOf(dataLen)
                applyMaskFallback(masked, off, dataLen, mask)
                val maskedFast = unmasked.copyOf(dataLen)
                applyMask(maskedFast, off, dataLen, mask)
                assertContentEquals(masked, maskedFast)
            }
        }
    }

    /** ⚖️ SPEC §4.3: 8-byte words; equal to the byte-wise reference at every offset 0–7, length 0–32 (and longer). */
    @Test fun wordMaskMatchesFallbackAtEveryOffsetAndLength() {
        val mask = packMask(byteArrayOf(0x12, 0x34, 0x56, 0x78.toByte()))
        val src = ByteArray(200) { (it * 37 + 11).toByte() }
        for (off in 0..7) for (len in (0..32) + listOf(63, 64, 65, 150)) {
            val a = src.copyOf()
            val b = src.copyOf()
            applyMaskFallback(a, off, off + len, mask)
            applyMask(b, off, off + len, mask)
            assertContentEquals(a, b, "off=$off len=$len")
        }
    }

    @Test fun maskIsItsOwnInverse() {
        val mask = MaskSource().next()
        val src = ByteArray(100) { it.toByte() }
        val a = src.copyOf()
        applyMask(a, 3, 97, mask)
        applyMask(a, 3, 97, mask)
        assertContentEquals(src, a)
    }

    @Test fun maskSourceGivesDifferentKeys() {
        val s = MaskSource()
        val keys = List(40) { s.next() }.toSet()
        assertNotEquals(1, keys.size)
    }
}
