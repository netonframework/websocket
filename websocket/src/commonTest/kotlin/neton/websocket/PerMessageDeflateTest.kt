package neton.websocket

import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.withTimeout
import neton.http.Request
import neton.http.Response
import neton.http.header.HeaderValue
import neton.io.core.memoryStreamPair
import neton.io.net.runReactor
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * ⚖️ permessage-deflate (RFC 7692; tungstenite has none, SPEC §11.7): the RFC's examples byte for byte (§7.2.3), the
 * frame rules (§6), size limits on inflated data, context takeover, negotiation (§5, §7.1) and the handshake.
 */
class PerMessageDeflateTest {
    private fun params(
        compressNoContextTakeover: Boolean = false,
        decompressNoContextTakeover: Boolean = false,
        compressBits: Int = 15,
        decompressBits: Int = 15,
    ) = PerMessageDeflate(compressNoContextTakeover, decompressNoContextTakeover, compressBits, decompressBits, 6)

    private fun server(deflate: PerMessageDeflate? = params(), config: WebSocketConfig = WebSocketConfig()) =
        WebSocketCore(Role.Server, config, null, deflate)

    private fun client(deflate: PerMessageDeflate? = params(), config: WebSocketConfig = WebSocketConfig()) =
        WebSocketCore(Role.Client, config, null, deflate)

    private fun hello() = "Hello".encodeToByteArray()

    /** The frames a server core queued, as (first byte, payload). */
    private fun WebSocketCore.sentFrames(): List<Pair<Int, ByteArray>> {
        val out = output.readAll()
        val frames = mutableListOf<Pair<Int, ByteArray>>()
        var i = 0
        while (i < out.size) {
            val first = out[i].toInt() and 0xFF
            var len = out[i + 1].toInt() and 0x7F
            var at = i + 2
            if (len == 126) { len = ((out[at].toInt() and 0xFF) shl 8) or (out[at + 1].toInt() and 0xFF); at += 2 }
            frames += first to out.copyOfRange(at, at + len)
            i = at + len
        }
        return frames
    }

    // ---- RFC 7692 §7.2.3 examples ----

    @Test fun aCompressedHelloIsTheRfcBytes() {
        val s = server()
        s.write(Message.text("Hello"))
        val (first, payload) = s.sentFrames().single()
        assertEquals(0xC1, first, "FIN, RSV1, text")
        assertContentEquals(bytesOf(0xf2, 0x48, 0xcd, 0xc9, 0xc9, 0x07, 0x00), payload)
    }

    @Test fun theSecondHelloUsesTheFirstAsContext() {
        val s = server()
        s.write(Message.text("Hello"))
        s.write(Message.text("Hello"))
        val frames = s.sentFrames()
        assertContentEquals(bytesOf(0xf2, 0x00, 0x11, 0x00, 0x00), frames[1].second, "§7.2.3.2: a back-reference to the first")
    }

    @Test fun withoutContextTakeoverEachMessageStandsAlone() {
        val s = server(params(compressNoContextTakeover = true))
        s.write(Message.text("Hello"))
        s.write(Message.text("Hello"))
        val frames = s.sentFrames()
        assertContentEquals(frames[0].second, frames[1].second)
    }

    @Test fun aCompressedHelloIsRead() {
        val c = client()
        c.feed(bytesOf(0xc1, 0x07, 0xf2, 0x48, 0xcd, 0xc9, 0xc9, 0x07, 0x00))
        assertEquals(Message.text("Hello"), c.read())
    }

    @Test fun twoHellosSharingContextAreRead() {
        val c = client()
        c.feed(bytesOf(0xc1, 0x07, 0xf2, 0x48, 0xcd, 0xc9, 0xc9, 0x07, 0x00))
        c.feed(bytesOf(0xc1, 0x05, 0xf2, 0x00, 0x11, 0x00, 0x00))
        assertEquals(Message.text("Hello"), c.read())
        assertEquals(Message.text("Hello"), c.read())
    }

    @Test fun aFragmentedCompressedMessageIsRead() {
        // §7.2.3.1: RSV1 on the first frame only.
        val c = client()
        c.feed(bytesOf(0x41, 0x03, 0xf2, 0x48, 0xcd) + bytesOf(0x80, 0x04, 0xc9, 0xc9, 0x07, 0x00))
        assertEquals(Message.text("Hello"), c.read())
    }

    @Test fun aBlockWithBfinalIsRead() {
        // §7.2.3.4: a DEFLATE block with BFINAL set, then a padding byte.
        val c = client()
        c.feed(bytesOf(0xc1, 0x08, 0xf3, 0x48, 0xcd, 0xc9, 0xc9, 0x07, 0x00, 0x00))
        assertEquals(Message.text("Hello"), c.read())
        // The next message starts a new stream.
        c.feed(bytesOf(0xc1, 0x07, 0xf2, 0x48, 0xcd, 0xc9, 0xc9, 0x07, 0x00))
        assertEquals(Message.text("Hello"), c.read())
    }

    @Test fun anEmptyMessageIsOneZeroByte() {
        // §7.2.3.6.
        val s = server()
        s.write(Message.binary(ByteArray(0)))
        assertContentEquals(bytesOf(0x00), s.sentFrames().single().second)
        val c = client()
        c.feed(bytesOf(0xc2, 0x01, 0x00))
        assertEquals(Message.binary(ByteArray(0)), c.read())
    }

    @Test fun anUncompressedMessageOnACompressingConnectionIsRead() {
        val c = client()
        c.feed(fromServer(0x81, hello()))
        assertEquals(Message.text("Hello"), c.read())
    }

    // ---- round trips ----

    @Test fun largeAndRepeatedMessagesRoundTrip() {
        for (noCtx in listOf(false, true)) {
            val s = server(params(compressNoContextTakeover = noCtx))
            val c = client(params(decompressNoContextTakeover = noCtx))
            val messages = listOf(
                ByteArray(200_000) { (it % 7).toByte() },       // compressible
                ByteArray(70_000) { ((it * 2654435761L) ushr 13).toByte() },   // barely
                ByteArray(1) { 9 },
                ByteArray(0),
            )
            for (m in messages) s.write(Message.binary(m))
            c.feed(s.output.readAll())
            for (m in messages) assertContentEquals(m, assertIs<Message.Binary>(c.read()).data.toByteArray(), "no context takeover $noCtx")
        }
    }

    @Test fun clientFramesAreMaskedAndRead() {
        val c = client()
        val s = server()
        c.write(Message.text("ünïcödé ".repeat(1000)))
        s.feed(c.output.readAll())
        assertEquals(Message.text("ünïcödé ".repeat(1000)), s.read())
    }

    // ---- frame rules (§6) ----

    @Test fun rsv1OnAControlFrameIsAnError() {
        val c = client()
        c.feed(bytesOf(0xc9, 0x00))                          // FIN, RSV1, ping
        assertEquals(ProtocolError.NonZeroReservedBits, assertFailsWith<WebSocketException.Protocol> { c.read() }.error)
    }

    @Test fun rsv1OnAContinuationIsAnError() {
        val c = client()
        c.feed(bytesOf(0x01, 0x01, 0x61) + bytesOf(0xc0, 0x01, 0x62))
        assertEquals(ProtocolError.NonZeroReservedBits, assertFailsWith<WebSocketException.Protocol> { c.read() }.error)
    }

    @Test fun rsv1WithoutTheExtensionIsAnError() {
        val c = client(deflate = null)
        c.feed(bytesOf(0xc1, 0x07, 0xf2, 0x48, 0xcd, 0xc9, 0xc9, 0x07, 0x00))
        assertEquals(ProtocolError.NonZeroReservedBits, assertFailsWith<WebSocketException.Protocol> { c.read() }.error)
    }

    @Test fun invalidCompressedDataIsAnError() {
        val c = client()
        c.feed(bytesOf(0xc1, 0x04, 0xff, 0xff, 0xff, 0xff))
        assertIs<ProtocolError.InvalidCompressedData>(assertFailsWith<WebSocketException.Protocol> { c.read() }.error)
    }

    @Test fun inflatedTextIsCheckedForUtf8() {
        val s = server()
        s.write(Message.binary(bytesOf(0xff, 0xfe)))
        val wire = s.output.readAll()
        wire[0] = (0xC1).toByte()                            // the same bytes, now claiming to be text
        val c = client()
        c.feed(wire)
        assertFailsWith<WebSocketException.Utf8> { c.read() }
    }

    @Test fun aDecompressionBombStopsAtTheMessageLimit() {
        // 64 MiB of zeros compress to about 64 KiB; the reader's limit is 1 MiB.
        val s = server()
        s.write(Message.binary(ByteArray(64 shl 20)))
        val wire = s.output.readAll()
        assertTrue(wire.size < 200_000, "compressed to ${wire.size} bytes")
        val c = client(config = WebSocketConfig(maxMessageSize = 1 shl 20))
        c.feed(wire)
        val e = assertFailsWith<WebSocketException.Capacity> { c.read() }
        assertIs<CapacityError.MessageTooLong>(e.error)
    }

    @Test fun aMessageThatDoesNotFitLeavesTheCompressorAsItWas() {
        // A full output must not consume compressor state: the same message queued later decodes.
        val s = server(config = WebSocketConfig(writeBufferSize = 0, maxWriteBufferSize = 300))
        assertTrue(s.tryWrite(Message.text("Hello")))
        assertFalse(s.tryWrite(Message.binary(ByteArray(1000) { it.toByte() })))
        val c = client()
        c.feed(s.output.readAll())
        assertEquals(Message.text("Hello"), c.read())
        assertTrue(s.tryWrite(Message.text("Hello")))
        c.feed(s.output.readAll())
        assertEquals(Message.text("Hello"), c.read())
    }

    // ---- negotiation (§5, §7.1) ----

    private fun request(vararg offers: String): Request<Unit> =
        Request(Unit).also { r -> for (o in offers) r.headers.append("Sec-WebSocket-Extensions", HeaderValue.fromStatic(o)) }

    private fun answer(config: PerMessageDeflateConfig, vararg offers: String): String? {
        val res = Response(Unit)
        return if (negotiatePerMessageDeflate(request(*offers), res, config)) res.headers["Sec-WebSocket-Extensions"]!!.toStr() else null
    }

    @Test fun theServerAcceptsAPlainOffer() {
        assertEquals("permessage-deflate", answer(PerMessageDeflateConfig(), "permessage-deflate"))
    }

    @Test fun theServerAnswersParameters() {
        assertEquals(
            "permessage-deflate; server_no_context_takeover; server_max_window_bits=10; client_max_window_bits=12",
            answer(PerMessageDeflateConfig(), "permessage-deflate; server_no_context_takeover; server_max_window_bits=10; client_max_window_bits=12"),
        )
        // The server's own limits are added when allowed.
        assertEquals(
            "permessage-deflate; client_no_context_takeover; server_max_window_bits=11; client_max_window_bits=9",
            answer(PerMessageDeflateConfig(clientNoContextTakeover = true, serverMaxWindowBits = 11, clientMaxWindowBits = 9), "permessage-deflate; client_max_window_bits"),
        )
    }

    @Test fun theServerDeclinesWhatItCannotDo() {
        assertNull(answer(PerMessageDeflateConfig(), "permessage-deflate; server_max_window_bits=8"), "zlib cannot compress with 8")
        assertNull(answer(PerMessageDeflateConfig(clientMaxWindowBits = 10), "permessage-deflate"), "a client limit not allowed by the offer")
        for (bad in listOf("permessage-deflate; foo", "permessage-deflate; server_no_context_takeover; server_no_context_takeover",
            "permessage-deflate; server_max_window_bits=16", "permessage-deflate; server_max_window_bits=09",
            "permessage-deflate; server_max_window_bits", "permessage-deflate; client_no_context_takeover=1")) {
            assertNull(answer(PerMessageDeflateConfig(), bad), bad)
        }
        assertNull(answer(PerMessageDeflateConfig(), "x-webkit-deflate-frame"))
    }

    @Test fun theServerTakesTheFirstAcceptableOffer() {
        assertEquals(
            "permessage-deflate; server_max_window_bits=12",
            answer(PerMessageDeflateConfig(), "permessage-deflate; server_max_window_bits=8, permessage-deflate; server_max_window_bits=12"),
        )
    }

    private fun response(value: String) = Response<Unit>(Unit).also { it.headers.append("Sec-WebSocket-Extensions", HeaderValue.fromStatic(value)) }

    @Test fun bothSidesReadTheResponseTheSameWay() {
        val r = response("permessage-deflate; server_no_context_takeover; client_max_window_bits=10")
        val s = assertNotNull(PerMessageDeflate.fromResponse(r, Role.Server))
        val c = assertNotNull(PerMessageDeflate.fromResponse(r, Role.Client))
        assertTrue(s.compressNoContextTakeover); assertFalse(s.decompressNoContextTakeover)
        assertTrue(c.decompressNoContextTakeover); assertFalse(c.compressNoContextTakeover)
        assertEquals(10, s.decompressWindowBits); assertEquals(10, c.compressWindowBits)
        assertEquals(15, s.compressWindowBits); assertEquals(15, c.decompressWindowBits)
        assertNull(PerMessageDeflate.fromResponse(Response<Unit>(Unit), Role.Client))
    }

    @Test fun theClientRejectsAnAnswerOutsideItsOffer() {
        val offer = PerMessageDeflateConfig(serverNoContextTakeover = true, serverMaxWindowBits = 10, clientMaxWindowBits = 12)
        for (bad in listOf("permessage-deflate; server_max_window_bits=10", "permessage-deflate; server_no_context_takeover",
            "permessage-deflate; server_no_context_takeover; server_max_window_bits=10; client_max_window_bits=13",
            "permessage-deflate; server_no_context_takeover; server_max_window_bits=10; client_max_window_bits",
            "permessage-deflate; server_no_context_takeover; server_max_window_bits=10; client_max_window_bits=8",
            "permessage-deflate; bogus", "permessage-deflate, permessage-deflate")) {
            assertIs<ProtocolError.InvalidExtensionParameters>(
                assertFailsWith<WebSocketException.Protocol>(bad) { PerMessageDeflate.fromResponse(response(bad), Role.Client, offer) }.error,
            )
        }
    }

    @Test fun theOfferNamesItsParameters() {
        assertEquals("permessage-deflate; client_max_window_bits", PerMessageDeflateConfig().offer())
        assertEquals(
            "permessage-deflate; server_no_context_takeover; client_no_context_takeover; server_max_window_bits=10; client_max_window_bits=11",
            PerMessageDeflateConfig(true, true, 10, 11).offer(),
        )
    }

    // ---- handshake ----

    private fun exchange(clientConfig: WebSocketConfig, serverConfig: WebSocketConfig, check: (WebSocket, WebSocket) -> Unit = { _, _ -> }) = runReactor {
        withTimeout(5_000) {
            coroutineScope {
                val (c, s) = memoryStreamPair()
                val server = async { accept(s, serverConfig) }
                val (client, _) = client("ws://localhost/", c, clientConfig)
                val srv = server.await()
                check(client, srv)
                val big = "compress me ".repeat(5000)
                client.send(Message.text(big))
                assertEquals(Message.text(big), srv.receive())
                srv.send(Message.binary(ByteArray(10_000) { 3 }))
                assertContentEquals(ByteArray(10_000) { 3 }, assertIs<Message.Binary>(client.receive()).data.toByteArray())
                client.close()
                while (srv.receive() != null) { }
                while (client.receive() != null) { }
            }
        }
    }

    @Test fun bothSidesWithCompressionNegotiateIt() {
        val on = WebSocketConfig(compression = PerMessageDeflateConfig(clientNoContextTakeover = true))
        exchange(on, on)
    }

    @Test fun aServerWithoutCompressionLeavesItOff() {
        exchange(WebSocketConfig(compression = PerMessageDeflateConfig()), WebSocketConfig())
    }

    @Test fun aClientWithoutCompressionLeavesItOff() {
        exchange(WebSocketConfig(), WebSocketConfig(compression = PerMessageDeflateConfig()))
    }
}
