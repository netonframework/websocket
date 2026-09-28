package neton.websocket

import neton.io.net.SocketOptions
import neton.io.net.TcpListener
import neton.io.net.listen
import kotlin.random.Random

// Helpers for the tests over real streams (SPEC §7).

/** Listen on 127.0.0.1 at a free port picked at random (TcpListener does not report its port). */
suspend fun listenLoopback(options: SocketOptions = SocketOptions.Default): Pair<TcpListener, Int> {
    var last: Throwable? = null
    repeat(50) {
        val port = Random.nextInt(20000, 60000)
        try {
            return listen("127.0.0.1", port, options) to port
        } catch (e: Throwable) {
            last = e
        }
    }
    throw IllegalStateException("no free port", last)
}
