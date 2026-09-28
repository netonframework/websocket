package neton.websocket.handshake

import neton.http.h1.parse.HttpParseError
import neton.websocket.ProtocolError
import neton.websocket.WebSocketException
import neton.websocket.assertProtocolError
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The reading side of `T/src/handshake/machine.rs` (the reference has no tests for it): the
 * `AttackCheck` limits and [HeadReader], fed as a driver would.
 */
class MachineTest {
    private val request = ("GET /chat HTTP/1.1\r\nHost: h\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n" +
        "Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n").encodeToByteArray()

    private fun reader(limits: HandshakeLimits = HandshakeLimits()) = HeadReader(limits, TryParse(::tryParseRequest))

    /** Feed [data] in chunks of [chunk] bytes until the head completes. */
    private fun <T> HeadReader<T>.feed(data: ByteArray, chunk: Int): Pair<T, neton.io.bytes.Bytes>? {
        var i = 0
        while (i < data.size) {
            val n = minOf(chunk, data.size - i)
            prepareRead()
            input.writeBytes(data, i, n)
            i += n
            received(n)?.let { return it }
        }
        return null
    }

    @Test fun defaultLimitsAreTheReferences() {
        assertEquals(HandshakeLimits(maxBytes = 65536, maxPackets = 512, minPacketSize = 128, minPacketCheckThreshold = 64), HandshakeLimits())
    }

    @Test fun attackCheckMaxBytes() {
        val c = AttackCheck(HandshakeLimits())
        c.checkIncomingPacketSize(65536)
        assertFailsWith<WebSocketException.AttackAttempt> { c.checkIncomingPacketSize(1) }
    }

    @Test fun attackCheckMaxPackets() {
        val c = AttackCheck(HandshakeLimits(minPacketCheckThreshold = 10_000))
        repeat(512) { c.checkIncomingPacketSize(1) }
        assertFailsWith<WebSocketException.AttackAttempt> { c.checkIncomingPacketSize(1) }
    }

    @Test fun attackCheckAverageAfterThreshold() {
        // 64 tiny reads are fine; the 65th must bring the average to 128 bytes.
        val c = AttackCheck(HandshakeLimits())
        repeat(64) { c.checkIncomingPacketSize(1) }
        c.checkIncomingPacketSize(65 * 128 - 64)
        assertFailsWith<WebSocketException.AttackAttempt> { AttackCheck(HandshakeLimits()).also { a -> repeat(65) { a.checkIncomingPacketSize(1) } } }
    }

    @Test fun completeInOneRead() {
        val (req, tail) = reader().feed(request + byteArrayOf(9, 8), request.size + 2)!!
        assertEquals("/chat", req.uri.path)
        assertEquals(2, tail.size)
    }

    @Test fun eofBeforeHead() {
        val r = reader()
        assertNull(r.feed(request.copyOf(20), 20))
        assertProtocolError<ProtocolError.HandshakeIncomplete> { r.receivedEof() }
        assertProtocolError<ProtocolError.HandshakeIncomplete> { reader().receivedEof() }
    }

    @Test fun slowlorisIsAnAttack() {
        // One byte per read: the 65th read averages 1 byte (the reference's limit).
        val r = reader()
        assertFailsWith<WebSocketException.AttackAttempt> { r.feed(request, 1) }
    }

    @Test fun oversizedHeadIsAnAttack() {
        val big = "GET / HTTP/1.1\r\nX: ${"a".repeat(70_000)}\r\n\r\n".encodeToByteArray()
        assertFailsWith<WebSocketException.AttackAttempt> { reader().feed(big, 4096) }
    }

    @Test fun earlierErrorWinsOverTheLimit() {
        // The reference would have failed on the first read (a bad request line) before any limit.
        val bad = "GET / HTTP/9.9\r\nX: ${"a".repeat(70_000)}".encodeToByteArray()
        val e = assertProtocolError<ProtocolError.HttparseError> { reader().feed(bad, 4096) }
        assertEquals(HttpParseError.Version, e.error)
    }
}
