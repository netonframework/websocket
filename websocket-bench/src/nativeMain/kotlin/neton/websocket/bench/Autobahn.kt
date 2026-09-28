package neton.websocket.bench

import neton.io.net.runReactor
import neton.io.net.serveTcp
import neton.websocket.Message
import neton.websocket.WebSocket
import neton.websocket.WebSocketException
import neton.websocket.accept
import neton.websocket.connect

/**
 * Echo until the connection ends, as tungstenite's Autobahn examples do: data messages are sent back, control
 * messages are handled by the connection. A normal close, a protocol or a UTF-8 error ends the case quietly.
 */
private suspend fun echo(ws: WebSocket) {
    try {
        while (true) {
            when (val msg = ws.receive() ?: break) {
                is Message.Text, is Message.Binary -> ws.send(msg)
                else -> {}
            }
        }
    } catch (e: WebSocketException) {
        when (e) {
            is WebSocketException.ConnectionClosed, is WebSocketException.Protocol, is WebSocketException.Utf8 -> {}
            else -> println("test: $e")
        }
    }
}

/** tungstenite `examples/autobahn-server.rs`: an echo server for the fuzzing client. Arguments: host port. */
fun autobahnServerMain(args: Array<String>) {
    val host = args.getOrElse(0) { "127.0.0.1" }
    val port = args.getOrElse(1) { "9002" }.toInt()
    println("autobahnServer on $host:$port")
    serveTcp(host, port, reactors = 1, shutdownOnSignals = true) { stream ->
        val ws = try { accept(stream) } catch (e: Exception) { println("handshake: $e"); stream.close(); return@serveTcp }
        echo(ws)
    }
}

/** tungstenite `examples/autobahn-client.rs`: runs every case of the fuzzing server. Arguments: base URL (ws://host:9001). */
fun autobahnClientMain(args: Array<String>) = runReactor {
    val base = args.getOrElse(0) { "ws://127.0.0.1:9001" }
    val agent = "neton-websocket"
    val (counter, _) = connect("$base/getCaseCount")
    val total = (counter.receive() as Message.Text).toString().trim().toInt()
    runCatching { counter.close() }
    println("autobahnClient: $total cases")
    for (case in 1..total) {
        try {
            val (ws, _) = connect("$base/runCase?case=$case&agent=$agent")
            echo(ws)
        } catch (e: Exception) {
            println("case $case: $e")
        }
    }
    val (reports, _) = connect("$base/updateReports?agent=$agent")
    runCatching { reports.close() }
    println("autobahnClient: done")
}
