package neton.websocket

import neton.io.core.IoStream
import neton.io.net.TcpListener
import neton.io.net.listen
import neton.websocket.frame.CloseFrame
import kotlin.random.Random

// Helpers for the handshake flow tests (SPEC §7): a minimal suspend driver over a WebSocketCore, so
// the reference's integration tests can exchange a few messages after the handshake. The real
// coroutine driver (write driver, split, cancellation) is SPEC §6, step 3.

/** `WebSocket<Stream>` reduced to what the ported tests call: read, send, close. */
class CoreConnection(val stream: IoStream, val core: WebSocketCore) {
    /** `read`: flush a pending reply first, then decode, reading more as needed. */
    suspend fun read(): Message {
        while (true) {
            if (core.hasPendingReply) flush()
            core.read()?.let { return it }
            val n = stream.read(core.input)
            if (n < 0) core.receivedEof()
        }
    }

    suspend fun send(message: Message) {
        core.write(message)
        flush()
    }

    suspend fun close(frame: CloseFrame? = null) {
        core.close(frame)
        flush()
    }

    suspend fun flush() {
        do {
            core.bufferReply()
            if (!core.output.isEmpty) stream.write(core.output)
        } while (core.hasPendingReply)
        stream.flush()
        core.flushed()
    }
}

fun ClientHandshakeResult.connection() = CoreConnection(stream, core)

/** Listen on 127.0.0.1 at a free port picked at random (TcpListener does not report its port). */
suspend fun listenLoopback(): Pair<TcpListener, Int> {
    var last: Throwable? = null
    repeat(50) {
        val port = Random.nextInt(20000, 60000)
        try {
            return listen("127.0.0.1", port) to port
        } catch (e: Throwable) {
            last = e
        }
    }
    throw IllegalStateException("no free port", last)
}
