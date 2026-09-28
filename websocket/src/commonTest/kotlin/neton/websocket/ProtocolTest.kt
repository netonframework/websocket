package neton.websocket

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

/** Ported from `T/src/protocol/mod.rs` tests (3). */
class ProtocolTest {
    @Test fun receiveMessages() {
        val incoming = CursorStream(bytesOf(
            0x89, 0x02, 0x01, 0x02, 0x8a, 0x01, 0x03, 0x01, 0x07, 0x48, 0x65, 0x6c, 0x6c, 0x6f,
            0x2c, 0x20, 0x80, 0x06, 0x57, 0x6f, 0x72, 0x6c, 0x64, 0x21, 0x82, 0x03, 0x01, 0x02,
            0x03,
        ))
        val socket = SyncWebSocket(incoming, Role.Client)
        assertEquals(Message.Ping(bytes(1, 2)), socket.read())
        assertEquals(Message.Pong(bytes(3)), socket.read())
        assertEquals(Message.text("Hello, World!"), socket.read())
        assertEquals(Message.binary(bytesOf(0x01, 0x02, 0x03)), socket.read())
    }

    @Test fun sizeLimitingTextFragmented() {
        val incoming = CursorStream(bytesOf(
            0x01, 0x07, 0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x2c, 0x20, 0x80, 0x06, 0x57, 0x6f, 0x72,
            0x6c, 0x64, 0x21,
        ))
        val limit = WebSocketConfig(maxMessageSize = 10)
        val socket = SyncWebSocket(incoming, Role.Client, limit)
        val e = assertFailsWith<WebSocketException.Capacity> { socket.read() }
        assertEquals(CapacityError.MessageTooLong(size = 13, maxSize = 10), e.error)
    }

    @Test fun sizeLimitingBinary() {
        val incoming = CursorStream(bytesOf(0x82, 0x03, 0x01, 0x02, 0x03))
        val limit = WebSocketConfig(maxMessageSize = 2)
        val socket = SyncWebSocket(incoming, Role.Client, limit)
        val e = assertFailsWith<WebSocketException.Capacity> { socket.read() }
        assertEquals(CapacityError.MessageTooLong(size = 3, maxSize = 2), e.error)
    }
}
