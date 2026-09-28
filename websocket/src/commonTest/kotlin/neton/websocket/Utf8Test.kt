package neton.websocket

import neton.io.bytes.Bytes
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** Ported from `T/src/protocol/frame/utf8.rs` tests (1), plus the validator (`T/src/utf8.rs` has none). */
class Utf8Test {
    @Test fun hashConsistency() {
        val bytes = Utf8Bytes.from("hash_consistency")
        assertEquals("hash_consistency".hashCode(), bytes.hashCode())
        assertEquals(Utf8Bytes.tryFrom(Bytes.wrap("hash_consistency".encodeToByteArray())).hashCode(), bytes.hashCode())
    }

    private val valid = listOf(
        "", "a", "héllo", "€", "😀", "日本語テキスト", "mixed ascii and ünïcödé and 🎉 in one long line!!",
    ).map { it.encodeToByteArray() } + listOf(
        bytesOf(0x7f), bytesOf(0xc2, 0x80), bytesOf(0xdf, 0xbf), bytesOf(0xe0, 0xa0, 0x80), bytesOf(0xed, 0x9f, 0xbf),
        bytesOf(0xee, 0x80, 0x80), bytesOf(0xef, 0xbf, 0xbf), bytesOf(0xf0, 0x90, 0x80, 0x80), bytesOf(0xf4, 0x8f, 0xbf, 0xbf),
    )

    private val invalid = listOf(
        bytesOf(0x80), bytesOf(0xbf), bytesOf(0xc0, 0x80), bytesOf(0xc1, 0xbf), // lone continuation, overlong
        bytesOf(0xe0, 0x80, 0x80), bytesOf(0xe0, 0x9f, 0xbf), // overlong 3-byte
        bytesOf(0xed, 0xa0, 0x80), bytesOf(0xed, 0xbf, 0xbf), // surrogates
        bytesOf(0xf0, 0x80, 0x80, 0x80), bytesOf(0xf0, 0x8f, 0xbf, 0xbf), // overlong 4-byte
        bytesOf(0xf4, 0x90, 0x80, 0x80), bytesOf(0xf5, 0x80, 0x80, 0x80), bytesOf(0xff), // > U+10FFFF
        bytesOf(0x61, 0xc2, 0x61), bytesOf(0xe2, 0x82, 0x61),
    )

    @Test fun validatorAcceptsValidRejectsInvalid() {
        for (v in valid) assertNull(Utf8Validator.check(v, 0, v.size), v.contentToString())
        for (v in invalid) assertNotNull(Utf8Validator.check(v, 0, v.size), v.contentToString())
        // Truncated sequences are incomplete, not valid.
        for (v in listOf(bytesOf(0xc2), bytesOf(0xe2, 0x82), bytesOf(0xf0, 0x9f, 0x98))) {
            val z = Utf8Validator()
            assertEquals(-1, z.feed(v, 0, v.size))
            assertFalse(z.isComplete)
            assertNotNull(Utf8Validator.check(v, 0, v.size))
        }
    }

    /** Incremental: every valid text split at every point (a code point may span chunks). */
    @Test fun validatorAcrossEverySplit() {
        val text = "a€😀é日本".encodeToByteArray() + ByteArray(20) { 'x'.code.toByte() }
        for (i in 0..text.size) for (j in i..text.size) {
            val v = Utf8Validator()
            assertEquals(-1, v.feed(text, 0, i))
            assertEquals(-1, v.feed(text, i, j))
            assertEquals(-1, v.feed(text, j, text.size))
            assertTrue(v.isComplete)
        }
        for (bad in invalid) for (i in 0..bad.size) {
            val v = Utf8Validator()
            val ok = v.feed(bad, 0, i) < 0 && v.feed(bad, i, bad.size) < 0 && v.isComplete
            assertFalse(ok, bad.contentToString())
        }
    }

    /** The ASCII word path must still find a bad byte anywhere in a long input. */
    @Test fun validatorFindsBadByteAtEveryPosition() {
        for (pos in 0 until 40) {
            val a = ByteArray(40) { 'a'.code.toByte() }
            a[pos] = 0xff.toByte()
            assertEquals(pos, Utf8Validator().feed(a, 0, a.size))
        }
    }

    @Test fun utf8BytesTryFromAndOrdering() {
        assertFailsWith<WebSocketException.Utf8> { Utf8Bytes.tryFrom(bytesOf(0xc3)) }
        assertEquals("é", Utf8Bytes.tryFrom(bytesOf(0xc3, 0xa9)).asString())
        assertTrue(Utf8Bytes.from("a") < Utf8Bytes.from("b"))
        assertTrue(Utf8Bytes.from("￿") < Utf8Bytes.from("😀")) // code point order, not UTF-16
        assertEquals(Utf8Bytes.from("x"), Utf8Bytes.tryFrom(Bytes.wrap(bytesOf(0x78))))
    }
}
