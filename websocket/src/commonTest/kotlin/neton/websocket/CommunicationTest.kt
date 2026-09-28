package neton.websocket

import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.withTimeout
import neton.io.core.IoStream
import neton.io.core.memoryStreamPair
import neton.io.net.runReactor
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Ported from `TT/tests/communication.rs` (2): a client sends nine messages and closes; the server
 * collects everything until the connection ends. Each runs over loopback TCP (the `...Tcp` twin,
 * as the reference) and over a neton-io `memoryStreamPair`.
 */
class CommunicationTest {
    /** `run_connection`: every message until the stream ends. */
    private suspend fun runConnection(connection: WebSocket): List<Message> {
        val messages = mutableListOf<Message>()
        while (true) messages += connection.receive() ?: break
        return messages
    }

    /** Accept on a server, run [clientSide] with a connected client stream, return what the server got. */
    private fun withServer(tcp: Boolean, clientSide: suspend (IoStream, String) -> Unit): List<Message> {
        var messages: List<Message> = emptyList()
        runReactor {
            withTimeout(5_000) {
                coroutineScope {
                    if (tcp) {
                        val (listener, port) = listenLoopback()
                        val server = async {
                            val stream = accept(listener.accept()) // "Failed to handshake with connection"
                            listener.close()
                            runConnection(stream)
                        }
                        clientSide(neton.io.net.connect("127.0.0.1", port), "ws://localhost:$port/")
                        messages = server.await()
                    } else {
                        val (c, s) = memoryStreamPair()
                        val server = async { runConnection(accept(s)) }
                        clientSide(c, "ws://localhost:12345/")
                        messages = server.await()
                    }
                }
            }
        }
        return messages
    }

    private fun communication(tcp: Boolean) {
        val messages = withServer(tcp) { tcp, url ->
            val (stream, _) = client(url, tcp) // client_async
            for (i in 1 until 10) stream.send(Message.text("$i"))
            stream.close()
            stream.abort() // the reference drops the client here
        }
        assertEquals(10, messages.size)
        assertTrue(messages.last().isClose)
    }

    private fun splitCommunication(tcp: Boolean) {
        val messages = withServer(tcp) { tcp, url ->
            val (stream, _) = client(url, tcp)
            val (_, tx) = stream.split()
            for (i in 1 until 10) tx.send(Message.text("$i"))
            tx.close()
            stream.abort()
        }
        assertEquals(10, messages.size)
        assertEquals((1 until 10).map { "$it" }, messages.take(9).map { it.toText() })
    }

    @Test fun communication() = communication(tcp = false)
    @Test fun communicationTcp() = communication(tcp = true)
    @Test fun splitCommunication() = splitCommunication(tcp = false)
    @Test fun splitCommunicationTcp() = splitCommunication(tcp = true)
}
