package neton.websocket.handshake

import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertNull

/** Ported from `T/src/handshake/mod.rs` (1), plus the SHA-1 and base64 vectors (SPEC §3.1). */
class HandshakeTest {
    @Test fun keyConversion() {
        // example from RFC 6455
        assertEquals("s3pPLMBiTxaQ9kYGzzhZRbK+xOo=", deriveAcceptKey("dGhlIHNhbXBsZSBub25jZQ==".encodeToByteArray()))
    }

    private fun hex(b: ByteArray) = b.joinToString("") { (it.toInt() and 0xFF).toString(16).padStart(2, '0') }

    /** RFC 3174 §7.3 test vectors (and the empty message). */
    @Test fun sha1Vectors() {
        assertEquals("da39a3ee5e6b4b0d3255bfef95601890afd80709", hex(sha1(ByteArray(0))))
        assertEquals("a9993e364706816aba3e25717850c26c9cd0d89d", hex(sha1("abc".encodeToByteArray())))
        assertEquals(
            "84983e441c3bd26ebaae4aa1f95129e5e54670f1",
            hex(sha1("abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq".encodeToByteArray())),
        )
        val million = Sha1()
        val chunk = ByteArray(1000) { 'a'.code.toByte() }
        repeat(1000) { million.update(chunk) }
        assertEquals("34aa973cd4c4daa4f61eeb2bdbad27316534016f", hex(million.digest()))
        assertEquals("dea356a2cddd90c7a7ecedc5ebb563934f460452", hex(sha1("0123456701234567012345670123456701234567012345670123456701234567".repeat(10).encodeToByteArray())))
    }

    /** Messages around the 55 / 56 / 64-byte padding boundaries, fed at once and byte by byte. */
    @Test fun sha1PaddingBoundaries() {
        val expected = mapOf(
            55 to "c1c8bbdc22796e28c0e15163d20899b65621d65a",
            56 to "c2db330f6083854c99d4b5bfb6e8f29f201be699",
            64 to "0098ba824b5c16427bd7a1122a5a442a25ec644d",
        )
        for ((len, digest) in expected) {
            val data = ByteArray(len) { 'a'.code.toByte() }
            assertEquals(digest, hex(sha1(data)), "length $len")
            val h = Sha1()
            for (b in data) h.update(byteArrayOf(b))
            assertEquals(digest, hex(h.digest()), "length $len, byte by byte")
        }
    }

    /** RFC 4648 §10 test vectors. */
    @Test fun base64Vectors() {
        val vectors = listOf("" to "", "f" to "Zg==", "fo" to "Zm8=", "foo" to "Zm9v", "foob" to "Zm9vYg==", "fooba" to "Zm9vYmE=", "foobar" to "Zm9vYmFy")
        for ((plain, encoded) in vectors) {
            assertEquals(encoded, Base64.encode(plain.encodeToByteArray()))
            assertContentEquals(plain.encodeToByteArray(), Base64.decode(encoded.encodeToByteArray()))
        }
    }

    /** Decoding is strict, like `data_encoding::BASE64`: padding, alphabet, canonical trailing bits. */
    @Test fun base64RejectsNonCanonical() {
        for (bad in listOf("Zg", "Zg=", "Zh==", "Zm9=", "Z===", "Zg==Zg==", "Zm9v!A==", "=Zg=", "Zg=a")) {
            assertNull(Base64.decode(bad.encodeToByteArray()), bad)
        }
        val all = ByteArray(256) { it.toByte() }
        assertContentEquals(all, Base64.decode(Base64.encode(all).encodeToByteArray()))
    }
}
