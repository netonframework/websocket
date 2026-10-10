package neton.websocket

import kotlinx.coroutines.CancellableContinuation
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.launch
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.yield
import neton.http.Upgraded
import neton.io.bytes.Buffer
import neton.io.bytes.Bytes
import neton.io.core.IoException
import neton.io.core.IoStream
import neton.websocket.frame.CloseFrame
import kotlin.coroutines.ContinuationInterceptor
import kotlin.coroutines.resume

/**
 * A WebSocket connection over an [IoStream] (tungstenite `WebSocket<Stream>`, tokio-tungstenite
 * `WebSocketStream`; SPEC §5, §6): the protocol core ([WebSocketCore]) driven by coroutines.
 *
 * ## Reading and writing
 * - [receive] returns the next message, answering pings and close frames on its own, and null once
 *   the connection has ended normally.
 * - [feed] queues a message, [flush] writes out and flushes everything queued, [send] does both;
 *   [trySend] queues without suspending; [close] starts (or answers) the closing handshake.
 * - Reading never waits for writing. One long-lived coroutine per connection, the write driver,
 *   owns the write direction of the stream: [feed] only queues frames into the connection's output
 *   buffer (bounded by [WebSocketConfig.maxWriteBufferSize]), [receive] only puts automatic replies
 *   into the core's one-frame reply slot (a newer pong replaces an older one, a close frame wins),
 *   and the driver writes them out. A peer that sends but does not read therefore never stops
 *   [receive]; the pending output stays bounded.
 * - At most one [receive] and one write operation ([feed], [send], [flush], [close]) at a time, as
 *   for an [IoStream]; a second concurrent one throws [IllegalStateException]. Reading and writing
 *   may run in two coroutines (see [split]).
 *
 * ## Cancellation
 * - Cancelling [receive] loses nothing: bytes already read stay in the read buffer and the next
 *   [receive] goes on parsing (the reference's cancel-safe `poll_next`). A stream without
 *   [neton.io.core.StreamCapability.ResumableAfterCancel] closes itself when a read is cancelled;
 *   the next [receive] then fails with [WebSocketException.Io].
 * - A frame is *accepted* once it has been encoded into the output buffer, which happens in one
 *   step. Cancelled before that ([feed] waiting for room), nothing of the message was queued and it
 *   still belongs to the caller. Cancelled after that (including while [flush] waits), the frame
 *   still goes out: the driver owns it and writes it before any later frame.
 * - If the driver cannot finish (the stream failed), the connection ends: no frame ever follows a
 *   partly written one.
 *
 * ## End of the connection
 * The connection owns the stream from here on and closes it when the connection ends: after the
 * closing handshake (a server closes as soon as its reply is written, a client when the server
 * closed the TCP connection), after a failure, or on [abort]. Then every operation throws
 * [WebSocketException.AlreadyClosed] (the first [receive] after a normal end returns null, the
 * first after a failure throws the failure).
 *
 * ## Threads
 * Use a connection on the thread of the reactor it was created on (neton-io SPEC §28.6): the
 * driver runs on the dispatcher of the coroutine that created the connection, and nothing is
 * locked.
 */
class WebSocket private constructor(
    private val stream: IoStream,
    private val core: WebSocketCore,
) {
    /** Whether this side is the server or the client. */
    val role: Role get() = core.role

    /** The configuration (tungstenite `get_config`). */
    val config: WebSocketConfig get() = core.config

    /** Executor-confined, nonblocking callback; throw to reject before payload allocation. */
    fun setInboundAdmission(beforePayload: (Int) -> Unit) = core.setInboundAdmission(beforePayload)

    /** Switch permanently to bounded data draining while completing a closing handshake. */
    fun discardData() = core.discardData()

    fun setInboundDataPolicy(policy: InboundDataPolicy) = core.setInboundDataPolicy(policy)

    /** Replace the configuration (tungstenite `set_config`); see [WebSocketCore.setConfig]. */
    fun setConfig(transform: (WebSocketConfig) -> WebSocketConfig) {
        core.setConfig(transform)
        writerSignal.signal() // a larger bound may make room for a waiting feed
        driverSignal.signal() // a smaller writeBufferSize may call for a write
    }

    /** Messages can still be received (tungstenite `can_read`): not after the peer's close frame. */
    val canRead: Boolean get() = !ended && core.canRead

    /** Messages can still be sent (tungstenite `can_write`): only before any close frame. */
    val canWrite: Boolean get() = !ended && core.canWrite

    private var reading = false
    private var writing = false

    /** Wakes the driver: something to write or flush. */
    private val driverSignal = Signal()

    /** Wakes the one waiting write operation: room in the output buffer, a flush done, or the end. */
    private val writerSignal = Signal()

    /** The driver's buffer: swapped with the core's output buffer to write without copying. */
    private var writeBuffer = Buffer(pooled = true)

    /** Flush requests made (a counter) and the highest one the driver has completed. */
    private var flushRequested = 0L
    private var flushedUpTo = 0L

    /** A read failure to end the connection with once the failure close frame is flushed. */
    private var failAfterFlush: WebSocketException? = null

    private var ended = false
    private var failure: Throwable? = null
    private var endSeen = false
    private var driver: Job? = null

    // ------------------------------------------------------------------------------------------
    // Reading
    // ------------------------------------------------------------------------------------------

    /**
     * The next message (tokio-tungstenite `StreamExt::next`, tungstenite `read`). Pings are answered
     * and a close frame from the peer is echoed automatically; both are returned too.
     *
     * @return the message, or null when the connection has ended normally (the reference's
     *   `ConnectionClosed`, or `None` from the stream).
     * @throws WebSocketException for a protocol, UTF-8 or size error, or an I/O error: the
     *   connection is then over (with [WebSocketConfig.sendCloseOnProtocolError], once the close
     *   frame for the error has been sent).
     * @throws WebSocketException.AlreadyClosed after the end was reported.
     */
    suspend fun receive(): Message? {
        check(!reading) { "another receive is in progress on this WebSocket" }
        reading = true
        try {
            return nextMessage()
        } finally {
            reading = false
        }
    }

    private suspend fun nextMessage(): Message? {
        while (true) {
            if (endSeen) throw alreadyClosed()
            if (ended) return seeEnd()
            // tungstenite `read` first writes a pending reply (`_write(None)`): the pong of a ping goes out before the
            // next message is read, so before any answer to it (Autobahn 5.6). Buffered only, never waited for.
            if (core.hasPendingReply && core.bufferReply()) driverSignal.signal()
            val message = try {
                core.read()
            } catch (_: WebSocketException.ConnectionClosed) {
                end(null)
                return seeEnd()
            } catch (e: WebSocketException) {
                endSeen = true
                failRead(e)
                // A reply due before the failure (the echo of the peer's close) is written before the error is reported, as
                // the reference's read wrote it first: a caller that closes the socket on the error would otherwise cut it
                // off. Bounded, in case the peer does not read.
                if (failAfterFlush != null && !ended) {
                    kotlinx.coroutines.withTimeoutOrNull(FINAL_FLUSH_MILLIS) { driver?.join() }
                    if (!ended) end(e)
                }
                throw e
            }
            // An automatic reply was queued: the driver writes it; this side never waits for that.
            if (core.hasReplyToFlush) driverSignal.signal()
            if (message != null) return message
            if (core.needsReadYield) { kotlinx.coroutines.yield(); continue }
            val n = try {
                stream.read(core.input)
            } catch (e: IoException) {
                // Closed under us because the connection ended (the driver, abort): report that.
                if (!ended) {
                    val mapped = core.mapIoError(e, isConnectionReset(e))
                    end(mapped.takeUnless { it is WebSocketException.ConnectionClosed })
                }
                continue
            }
            if (n < 0) core.receivedEof()
        }
    }

    /** The reader learns that the connection is over: null for a normal end, else the failure. */
    private fun seeEnd(): Message? {
        endSeen = true
        core.input.clear()
        core.input.releaseIfIdle()
        failure?.let { throw it }
        return null
    }

    /**
     * ⚖️ A protocol, UTF-8 or size error fails the connection (RFC 6455 §7.1.7): the stream is
     * closed, after the close frame of [WebSocketConfig.sendCloseOnProtocolError] if one was queued.
     * The reference returns the error and leaves dropping the connection to the caller.
     */
    private fun failRead(e: WebSocketException) {
        if (ended) return
        // A reply already due (the echo of the peer's close, a pong) goes out before the connection ends: tungstenite wrote it
        // at the start of the read that failed (Autobahn 7.1.2-7.1.5); a close frame for the error only with the option.
        if (core.hasReplyToFlush && (config.sendCloseOnProtocolError || !core.canWrite || !core.hasPendingReply)) {
            failAfterFlush = e
            requestFlush()
        } else {
            end(e)
        }
    }

    // ------------------------------------------------------------------------------------------
    // Writing
    // ------------------------------------------------------------------------------------------

    /**
     * Queue [message] without flushing (tungstenite `write`, `SinkExt::feed`). Suspends while the
     * output buffer is full ([WebSocketConfig.maxWriteBufferSize]); ⚖️ the reference fails with
     * [WebSocketException.WriteBufferFull] instead (SPEC §4.3). Returns once the frame is accepted;
     * see the class notes on cancellation. A [Message.Close] is [close] without waiting for the flush.
     *
     * @throws WebSocketException.Protocol [ProtocolError.SendAfterClosing] once a close frame was
     *   sent or received, or ⚖️ a control frame over 125 bytes.
     * @throws WebSocketException.Capacity ⚖️ a message over [WebSocketConfig.maxMessageSize].
     * @throws WebSocketException.AlreadyClosed the connection has ended.
     */
    suspend fun feed(message: Message) = writeOp { queue(message) }

    /**
     * Write out and flush everything queued, including automatic replies (tungstenite `flush`).
     * Returns normally if the connection ends normally meanwhile (as tokio-tungstenite's `poll_flush`).
     * @throws WebSocketException.Io the stream failed.
     * @throws WebSocketException.AlreadyClosed the connection had already ended.
     */
    suspend fun flush() = writeOp { flushQueued() }

    /** [feed] then [flush] (tungstenite `send`, `SinkExt::send`). */
    suspend fun send(message: Message) = writeOp {
        queue(message)
        flushQueued()
    }

    /**
     * Queue [message] and ask for a flush without suspending: false when the output buffer is full
     * (nothing was queued; the reference's [WebSocketException.WriteBufferFull]). Other errors as [feed].
     */
    fun trySend(message: Message): Boolean {
        check(!writing) { "another write is in progress on this WebSocket" }
        checkOpen()
        if (!core.tryWrite(message)) return false
        requestFlush()
        return true
    }

    /**
     * Start the closing handshake with [frame] and flush (tungstenite `close`, tokio-tungstenite
     * `close`); `send(Message.Close(frame))` is the same. Only the first close sends a frame; later
     * ones just flush. Keep calling [receive] until it returns null to finish the handshake.
     * @throws WebSocketException.Protocol ⚖️ a close reason over 123 bytes.
     */
    suspend fun close(frame: CloseFrame? = null) = writeOp {
        checkOpen()
        core.close(frame)
        flushQueued()
    }

    private suspend fun queue(message: Message) {
        // Past writeBufferSize the reference writes the batch out here: give the driver the chance
        // to take it before adding more (before accepting, so a cancellation here queues nothing).
        if (core.wantsWrite) yield()
        while (true) {
            checkOpen()
            if (core.tryWrite(message)) break
            driverSignal.signal()
            writerSignal.await()
        }
        if (message is Message.Close) requestFlush() else if (core.wantsWrite) driverSignal.signal()
    }

    private suspend fun flushQueued() {
        checkOpen()
        val target = requestFlush()
        while (flushedUpTo < target) {
            if (ended) {
                failure?.let { throw it }
                return
            }
            writerSignal.await()
        }
    }

    private fun requestFlush(): Long {
        val target = ++flushRequested
        driverSignal.signal()
        return target
    }

    private inline fun <T> writeOp(block: () -> T): T {
        check(!writing) { "another write is in progress on this WebSocket" }
        writing = true
        try {
            return block()
        } finally {
            writing = false
        }
    }

    private fun checkOpen() {
        if (ended) throw alreadyClosed()
    }

    private fun alreadyClosed(): WebSocketException.AlreadyClosed {
        val cause = failure ?: failAfterFlush
        return cause as? WebSocketException.AlreadyClosed ?: WebSocketException.AlreadyClosed(cause)
    }

    // ------------------------------------------------------------------------------------------
    // The write driver
    // ------------------------------------------------------------------------------------------

    /**
     * The write driver (SPEC §5): the only writer of the stream. Each pass moves a pending reply
     * behind the queued frames, writes the queue out when there is a flush to do, a reply to send
     * (the reference flushes replies at once) or more than [WebSocketConfig.writeBufferSize]
     * queued, and flushes. The queue is handed over by swapping buffers, so frames can be queued
     * while it is being written.
     */
    private suspend fun drive() {
        try {
            while (!ended) {
                val target = flushRequested
                core.bufferReply()
                val replied = core.takeReplyQueued()
                val flush = replied || target > flushedUpTo
                if (!flush && !core.wantsWrite && !core.hasPendingReply) {
                    driverSignal.await()
                    continue
                }
                if (!core.output.isEmpty) {
                    writeBuffer = core.takeOutput(writeBuffer)
                    stream.write(writeBuffer)
                    core.outputWritten()
                    writerSignal.signal()
                }
                if (flush) {
                    stream.flush()
                    if (target > flushedUpTo) flushedUpTo = target
                    writerSignal.signal()
                    failAfterFlush?.let { end(it); return }
                    core.flushed() // a server whose closing handshake is done ends here
                }
            }
        } catch (_: WebSocketException.ConnectionClosed) {
            end(null)
        } catch (e: IoException) {
            if (!ended) end(writeFailure(e))
        } catch (e: CancellationException) {
            throw e // the connection ended (end cancels the driver)
        } catch (e: Throwable) {
            if (!ended) end(e)
        } finally {
            writeBuffer.clear()
            writeBuffer.releaseIfIdle()
        }
    }

    /**
     * The end a write error means. ⚖️ Once the peer's close frame has arrived, any write error
     * (not only a reset, as the reference's `check_connection_reset`) is the normal end: the peer
     * may close the connection as soon as it sent its close frame, and whether our echo then fails
     * with a reset, a broken pipe or not at all depends on timing.
     */
    private fun writeFailure(e: IoException): WebSocketException? =
        if (!core.canRead) null else WebSocketException.Io(e)

    /** End the connection: terminate the core, close the stream, stop the driver, wake the writer. */
    private fun end(f: Throwable?) {
        if (ended) return
        ended = true
        failure = f
        core.terminate()
        stream.close()
        driver?.cancel()
        core.output.clear()
        core.output.releaseIfIdle()
        writerSignal.signal()
    }

    /**
     * Close the connection at once, without a closing handshake (dropping a `WebSocket` in the
     * reference ⚖️: Kotlin has no drop). Operations waiting and later ones throw
     * [WebSocketException.AlreadyClosed].
     */
    fun abort() = end(WebSocketException.AlreadyClosed())

    // ------------------------------------------------------------------------------------------
    // Split
    // ------------------------------------------------------------------------------------------

    /**
     * The reading and the writing half, for two coroutines (tokio-tungstenite's `StreamExt::split`;
     * native here instead of a lock around the whole connection). Both share this connection:
     * automatic replies from the reader go through the same write driver as the writer's frames.
     */
    fun split(): Pair<WebSocketReader, WebSocketWriter> = WebSocketReader(this) to WebSocketWriter(this)

    companion object {
        private const val FINAL_FLUSH_MILLIS = 1_000L

        /**
         * A connection over a stream whose handshake is already done (tungstenite
         * `from_raw_socket`, `from_partially_read`; tokio-tungstenite `from_raw_socket`,
         * `from_partially_read`). [prefix] holds bytes already read past the handshake. [deflate]: ⚖️ the
         * permessage-deflate the handshake negotiated ([PerMessageDeflate.fromResponse]), null for none.
         */
        suspend fun fromRawStream(
            stream: IoStream,
            role: Role,
            config: WebSocketConfig? = null,
            prefix: Bytes? = null,
            deflate: PerMessageDeflate? = null,
        ): WebSocket = start(stream, WebSocketCore(role, config ?: WebSocketConfig(), prefix?.takeUnless { it.isEmpty }, deflate))

        /**
         * Take over a connection an HTTP server or client upgraded (SPEC §1; tokio-tungstenite's
         * `server-custom-accept` example with hyper): the server side after answering `101` (see
         * [neton.websocket.handshake.createResponse]) and `upgradeOn(request.extensions)`; the
         * client side after [neton.websocket.handshake.verifyUpgradeResponse] and
         * `upgradeOn(response.extensions)`. Bytes the HTTP connection read past the head come first.
         * ⚖️ With permessage-deflate: the server calls [negotiatePerMessageDeflate] on its response, and either side
         * passes [PerMessageDeflate.fromResponse] of the response as [deflate].
         */
        suspend fun fromUpgraded(upgraded: Upgraded, role: Role, config: WebSocketConfig? = null, deflate: PerMessageDeflate? = null): WebSocket {
            val (stream, prefix) = upgraded.downcast()
            return fromRawStream(stream, role, config, prefix, deflate)
        }

        /** Start the connection's write driver on the caller's dispatcher. */
        internal suspend fun start(stream: IoStream, core: WebSocketCore): WebSocket {
            val ws = WebSocket(stream, core)
            val dispatcher = currentCoroutineContext()[ContinuationInterceptor] ?: Dispatchers.Unconfined
            // Its own Job: the driver lives as long as the connection, not as the creating coroutine.
            ws.driver = CoroutineScope(dispatcher + Job()).launch(start = CoroutineStart.UNDISPATCHED) { ws.drive() }
            return ws
        }
    }
}

/** The reading half of a [WebSocket] ([WebSocket.split]). */
class WebSocketReader internal constructor(private val ws: WebSocket) {
    /** [WebSocket.receive]. */
    suspend fun receive(): Message? = ws.receive()

    /** [WebSocket.canRead]. */
    val canRead: Boolean get() = ws.canRead
}

/** The writing half of a [WebSocket] ([WebSocket.split]); the tokio-tungstenite `Sink` half. */
class WebSocketWriter internal constructor(private val ws: WebSocket) {
    /** [WebSocket.send]. */
    suspend fun send(message: Message) = ws.send(message)

    /** [WebSocket.feed]. */
    suspend fun feed(message: Message) = ws.feed(message)

    /** [WebSocket.flush]. */
    suspend fun flush() = ws.flush()

    /** [WebSocket.trySend]. */
    fun trySend(message: Message): Boolean = ws.trySend(message)

    /** [WebSocket.close] (`SinkExt::close`). */
    suspend fun close(frame: CloseFrame? = null) = ws.close(frame)

    /** [WebSocket.canWrite]. */
    val canWrite: Boolean get() = ws.canWrite
}

/**
 * One coroutine waiting for an event, on one thread: [signal] wakes the waiter, or is remembered
 * for the next [await] when nobody waits. Waiters re-check their condition after waking.
 */
private class Signal {
    private var waiter: CancellableContinuation<Unit>? = null
    private var pending = false

    fun signal() {
        val w = waiter
        if (w == null) {
            pending = true
            return
        }
        waiter = null
        w.resume(Unit)
    }

    suspend fun await() {
        if (pending) {
            pending = false
            return
        }
        suspendCancellableCoroutine { c ->
            waiter = c
            c.invokeOnCancellation { if (waiter === c) waiter = null }
        }
    }
}

/** Whether [e] is the peer resetting the connection (`io::ErrorKind::ConnectionReset`). */
internal expect fun isConnectionReset(e: IoException): Boolean
