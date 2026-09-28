package neton.websocket

import neton.io.bytes.Buffer
import neton.io.bytes.Bytes
import neton.websocket.frame.CloseFrame

// A synchronous driver with the semantics of tungstenite's `WebSocket<Stream: Read + Write>`
// (`T/src/protocol/mod.rs:245-358`, `mod.rs:449-607`), built on the sans-I/O [WebSocketCore].
// It exists so the reference's stream-level tests can be ported as they are; the coroutine driver
// is SPEC §6.

/** A non-blocking stream would block (the reference's `io::ErrorKind::WouldBlock`). */
class WouldBlock : Exception("would block")

/** The peer reset the connection (the reference's `io::ErrorKind::ConnectionReset`). */
class ConnectionReset : Exception("connection reset")

/** `Read + Write`: [read] returns 0 at EOF and may throw [WouldBlock] / [ConnectionReset]. */
interface MockStream {
    fun read(dst: ByteArray, off: Int, len: Int): Int
    fun write(src: ByteArray, off: Int, len: Int): Int
    fun flush()
}

class SyncWebSocket(
    val stream: MockStream,
    role: Role,
    config: WebSocketConfig? = null,
    prefix: ByteArray? = null,
) {
    val core = WebSocketCore(role, config ?: WebSocketConfig(), prefix?.let { Bytes.copyOf(it) })

    /** A reply was written but its flush would block (the reference's `unflushed_additional`). */
    private var unflushed = false

    val canRead: Boolean get() = core.canRead
    val canWrite: Boolean get() = core.canWrite

    /** `WebSocketContext::read`: reply first (a blocked flush does not stop reading), then decode. */
    fun read(): Message {
        while (true) {
            if (core.hasPendingReply || unflushed) {
                try { flush() } catch (_: WouldBlock) { unflushed = true }
            }
            val m = core.read()
            if (m != null) return m
            readIn()
        }
    }

    private fun readIn() {
        val input = core.input
        val room = minOf(input.reserve(minOf(4096, core.maxReadSize)), core.maxReadSize)
        val n = try {
            stream.read(input.backingArray(), input.writerIndex(), room)
        } catch (e: ConnectionReset) {
            throw core.mapIoError(e, isConnectionReset = true)
        }
        if (n == 0) core.receivedEof() else input.commitWrite(n)
    }

    /** `WebSocketContext::write`: queue, write out past `writeBufferSize`, flush if a reply went out. */
    fun write(message: Message) {
        if (message is Message.Close) { close(message.frame); return }
        core.write(message)
        val replied = core.bufferReply()
        if (core.wantsWrite) writeOut()
        if (message !is Message.Pong && replied) flush()
    }

    fun send(message: Message) { write(message); flush() }

    /** `WebSocketContext::flush`: queue the reply, write everything, flush the stream. */
    fun flush() {
        do {
            core.bufferReply()
            writeOut()
        } while (core.hasPendingReply)
        stream.flush()
        unflushed = false
        core.flushed()
    }

    /** `WebSocketContext::close`. */
    fun close(frame: CloseFrame? = null) {
        core.close(frame)
        flush()
    }

    private fun writeOut() {
        val out: Buffer = core.output
        while (!out.isEmpty) {
            val n = stream.write(out.backingArray(), out.readerIndex(), out.readableBytes)
            if (n == 0) throw WebSocketException.Io(ConnectionReset())
            out.consume(n)
        }
    }
}

/** Reads from a fixed array (then EOF); writes are accepted and dropped (the reference's `WriteMoc<Cursor>`). */
class CursorStream(private val data: ByteArray) : MockStream {
    private var pos = 0
    override fun read(dst: ByteArray, off: Int, len: Int): Int {
        val n = minOf(len, data.size - pos)
        data.copyInto(dst, off, pos, pos + n)
        pos += n
        return n
    }
    override fun write(src: ByteArray, off: Int, len: Int): Int = len
    override fun flush() {}
}

/** One direction of an in-memory connection. */
class Pipe {
    val data = ArrayDeque<Byte>()
    var closed = false
    var reset = false
}

/** One end of an in-memory connection: reads [WouldBlock] when idle, like a non-blocking socket. */
class PipeEnd(private val inbound: Pipe, private val outbound: Pipe) : MockStream {
    override fun read(dst: ByteArray, off: Int, len: Int): Int {
        if (inbound.data.isEmpty()) {
            if (inbound.reset) throw ConnectionReset()
            if (inbound.closed) return 0
            throw WouldBlock()
        }
        var n = 0
        while (n < len && inbound.data.isNotEmpty()) dst[off + n++] = inbound.data.removeFirst()
        return n
    }

    override fun write(src: ByteArray, off: Int, len: Int): Int {
        if (outbound.reset || outbound.closed) throw ConnectionReset()
        for (i in off until off + len) outbound.data.addLast(src[i])
        return len
    }

    override fun flush() {}

    /** Drop the socket: the peer reads EOF. */
    fun drop() { outbound.closed = true }

    /** Drop with SO_LINGER 0: the peer gets a reset. */
    fun dropWithReset() { outbound.reset = true; inbound.reset = true }
}

fun pipePair(): Pair<PipeEnd, PipeEnd> {
    val ab = Pipe()
    val ba = Pipe()
    return PipeEnd(inbound = ba, outbound = ab) to PipeEnd(inbound = ab, outbound = ba)
}

fun bytesOf(vararg b: Int): ByteArray = ByteArray(b.size) { b[it].toByte() }

fun bytes(vararg b: Int): Bytes = Bytes.wrap(bytesOf(*b))

/** Feed [data] to [core] and read one message. */
fun WebSocketCore.feed(data: ByteArray): WebSocketCore = also { input.writeBytes(data) }

inline fun <reified T : ProtocolError> assertProtocolError(block: () -> Unit): T {
    val e = kotlin.test.assertFailsWith<WebSocketException.Protocol> { block() }
    kotlin.test.assertTrue(e.error is T, "expected ${T::class.simpleName}, got ${e.error}")
    return e.error as T
}
