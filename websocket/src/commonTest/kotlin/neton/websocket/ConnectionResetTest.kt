package neton.websocket

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * Ported from `T/tests/connection_reset.rs` (3), `no_send_after_close.rs` (1) and
 * `receive_after_init_close.rs` (1), at protocol level: the reference runs client and server on
 * two threads over TCP after a handshake; here both are [SyncWebSocket]s over an in-memory,
 * non-blocking connection, and the steps run in one of the orders the threads can take (a read
 * with nothing to read throws [WouldBlock] where the reference's thread would block). The TCP /
 * memoryStreamPair versions with the handshake belong to the coroutine driver (SPEC §6, §7).
 */
class ConnectionResetTest {
    private fun pair(): Triple<SyncWebSocket, SyncWebSocket, Pair<PipeEnd, PipeEnd>> {
        val (c, s) = pipePair()
        return Triple(SyncWebSocket(c, Role.Client), SyncWebSocket(s, Role.Server), c to s)
    }

    @Test fun testServerClose() {
        val (cli, srv, ends) = pair()
        cli.send(Message.text("Hello WebSocket"))

        assertEquals("Hello WebSocket", srv.read().toText())
        srv.close() // send close to client

        assertTrue(cli.read().isClose) // receive close from server
        assertFailsWith<WouldBlock> { cli.read() } // replies; then waits for the server to drop TCP

        assertTrue(srv.read().isClose) // receive acknowledgement
        assertFailsWith<WebSocketException.ConnectionClosed> { srv.read() }
        ends.second.drop()

        assertFailsWith<WebSocketException.ConnectionClosed> { cli.read() }
    }

    @Test fun testEvilServerClose() {
        val (cli, srv, ends) = pair()
        cli.send(Message.text("Hello WebSocket"))

        assertEquals("Hello WebSocket", srv.read().toText())
        srv.close()

        assertTrue(cli.read().isClose)
        assertFailsWith<WouldBlock> { cli.read() }

        assertTrue(srv.read().isClose) // receive acknowledgement
        // and now just drop the connection with a reset, without waiting for ConnectionClosed
        ends.second.dropWithReset()

        assertFailsWith<WebSocketException.ConnectionClosed> { cli.read() }
    }

    @Test fun testClientClose() {
        val (cli, srv, ends) = pair()
        cli.send(Message.text("Hello WebSocket"))

        assertEquals("Hello WebSocket", srv.read().toText())
        srv.send(Message.text("From Server"))

        assertEquals("From Server", cli.read().toText())
        cli.close() // send close to server

        assertTrue(srv.read().isClose) // receive close from client
        assertFailsWith<WebSocketException.ConnectionClosed> { srv.read() } // reply written, server done
        ends.second.drop()

        assertTrue(cli.read().isClose) // receive acknowledgement from server
        assertFailsWith<WebSocketException.ConnectionClosed> { cli.read() }
    }

    /** `T/tests/no_send_after_close.rs`. */
    @Test fun testNoSendAfterClose() {
        val (cli, srv, ends) = pair()
        srv.close() // send close to client

        val e = assertFailsWith<WebSocketException.Protocol> { srv.send(Message.text("Hello WebSocket")) }
        assertEquals(ProtocolError.SendAfterClosing, e.error)

        assertTrue(cli.read().isClose) // receive close from server
        assertFailsWith<WouldBlock> { cli.read() }
        ends.second.drop()
        assertFailsWith<WebSocketException.ConnectionClosed> { cli.read() }
    }

    /** `T/tests/receive_after_init_close.rs`: data can be read after we initiated the close. */
    @Test fun testReceiveAfterInitClose() {
        val (cli, srv, ends) = pair()
        cli.send(Message.text("Hello WebSocket"))

        srv.close() // send close to client

        // This read succeeds even though we already initiated a close.
        assertEquals("Hello WebSocket", srv.read().intoData().decodeToString())

        assertTrue(cli.read().isClose) // receive close from server
        assertFailsWith<WouldBlock> { cli.read() }

        assertTrue(srv.read().isClose) // receive acknowledgement
        assertFailsWith<WebSocketException.ConnectionClosed> { srv.read() }
        ends.second.drop()

        assertFailsWith<WebSocketException.ConnectionClosed> { cli.read() }
    }
}
