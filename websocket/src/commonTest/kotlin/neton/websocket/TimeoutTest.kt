package neton.websocket

import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import neton.io.bytes.Buffer
import neton.io.core.IoException
import neton.io.core.IoStream
import neton.io.core.memoryStreamPair
import neton.io.net.runReactor
import neton.websocket.handshake.HandshakeLimits
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

/**
 * ⚖️ The time limits the reference does not have (SPEC §11.8): the handshake ([HandshakeLimits.timeoutMillis]), the
 * closing handshake ([WebSocketConfig.closeTimeoutMillis]) and an idle connection ([WebSocketConfig.idleTimeoutMillis]).
 * Each ends with [WebSocketException.Timeout] when the peer stays silent.
 */
class TimeoutTest {
    private fun test(block: suspend () -> Unit) = runReactor { withTimeout(20.seconds) { block() } }

    /** Reads what [s] receives until it ends or fails. */
    private suspend fun drain(s: IoStream) {
        try { while (s.read(Buffer()) >= 0) {} } catch (_: IoException) {}
    }

    @Test
    fun aServerGivesUpOnAClientThatSendsNoRequest() = test {
        val (silent, server) = memoryStreamPair()
        val start = TimeSource.Monotonic.markNow()
        val e = assertFailsWith<WebSocketException.Timeout> { acceptWithConfig(server, null, HandshakeLimits(timeoutMillis = 200)) }
        assertEquals("handshake", e.what)
        assertTrue(start.elapsedNow().inWholeMilliseconds in 150..3000, "gave up after ${start.elapsedNow()}")
        server.close(); silent.close()
    }

    @Test
    fun aClientGivesUpOnAServerThatNeverAnswers() = test {
        val (client, mute) = memoryStreamPair()
        coroutineScope {
            launch { drain(mute) }
            val e = assertFailsWith<WebSocketException.Timeout> {
                clientHandshake(client, "ws://localhost/", null, HandshakeLimits(timeoutMillis = 200))
            }
            assertEquals("handshake", e.what)
            client.close(); mute.close()
        }
    }

    /** The server closes; the client never reads, so it never answers: the server ends after the close limit. */
    @Test
    fun theClosingHandshakeIsBounded() = test {
        val (a, b) = memoryStreamPair()
        coroutineScope {
            val accepted = async { acceptWithConfig(b, WebSocketConfig(closeTimeoutMillis = 200)) }
            val (client, _) = client("ws://localhost/", a)
            val server = accepted.await()
            server.close()
            val start = TimeSource.Monotonic.markNow()
            val e = assertFailsWith<WebSocketException.Timeout> { while (server.receive() != null) {} }
            assertEquals("closing handshake", e.what)
            assertTrue(start.elapsedNow().inWholeMilliseconds in 150..3000, "ended after ${start.elapsedNow()}")
            client.abort()
        }
    }

    /** A peer that answers the close ends the connection normally, well within the limit. */
    @Test
    fun aClosingHandshakeThatCompletesIsNotCutShort() = test {
        val (a, b) = memoryStreamPair()
        coroutineScope {
            val accepted = async { acceptWithConfig(b, WebSocketConfig(closeTimeoutMillis = 5_000)) }
            val (client, _) = client("ws://localhost/", a)
            val server = accepted.await()
            val echo = launch { while (client.receive() != null) {} }
            server.close()
            while (server.receive() != null) {}
            echo.join()
        }
    }

    @Test
    fun anIdleConnectionEndsAndActivityKeepsItOpen() = test {
        val (a, b) = memoryStreamPair()
        coroutineScope {
            val accepted = async { acceptWithConfig(b, WebSocketConfig(idleTimeoutMillis = 300)) }
            val (client, _) = client("ws://localhost/", a)
            val server = accepted.await()
            // Messages every 100 ms keep it open for a second...
            val reader = async {
                var n = 0
                try { while (server.receive() != null) n++ } catch (e: WebSocketException.Timeout) { return@async n to e.what }
                n to "ended"
            }
            repeat(10) { client.send(Message.Text("tick $it")); delay(100) }
            // ...then silence: the idle limit ends it.
            val (received, how) = reader.await()
            assertEquals(10, received)
            assertEquals("idle", how)
            client.abort()
        }
    }

    @Test
    fun defaults() {
        assertEquals(10_000L, HandshakeLimits().timeoutMillis)
        assertEquals(10_000L, WebSocketConfig().closeTimeoutMillis)
        assertEquals(0L, WebSocketConfig().idleTimeoutMillis)
    }
}
