package neton.websocket.handshake

import neton.http.HttpException
import neton.http.Version
import neton.http.h1.isCompleteFast
import neton.http.h1.parse.HeaderSlots
import neton.http.h1.parse.HttpParseError
import neton.http.h1.parse.ParseStatus
import neton.http.h1.parse.parseHeaders
import neton.http.header.HeaderMap
import neton.http.header.HeaderName
import neton.http.header.HeaderValue
import neton.io.bytes.Buffer
import neton.io.bytes.Bytes
import neton.websocket.CapacityError
import neton.websocket.ProtocolError
import neton.websocket.WebSocketException
import neton.websocket.protocolError

// Shared handshake parts (tungstenite `T/src/handshake/mod.rs`, `machine.rs`, `headers.rs`; SPEC §3.1).
// Everything here is sans-I/O: the suspend entry points (`neton.websocket.clientHandshake`,
// `accept`, ...) only move bytes between an IoStream and these types.

/** Limit for the number of header lines (`MAX_HEADERS`); one more is [CapacityError.TooManyHeaders]. */
const val MAX_HEADERS: Int = 124

private val WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11".encodeToByteArray()

/**
 * Derive the `Sec-WebSocket-Accept` response header from a `Sec-WebSocket-Key` request header
 * (`derive_accept_key`): base64(SHA-1(key + GUID)), RFC 6455 §4.2.2.
 *
 * Can be used to do a handshake by hand before handing the stream to a WebSocket.
 */
fun deriveAcceptKey(requestKey: ByteArray): String = Base64.encode(Sha1().update(requestKey).update(WS_GUID).digest())

/** `version_as_str`: the text of an HTTP/1 version; HTTP/2 and 3 are [ProtocolError.WrongHttpVersion]. */
internal fun versionAsStr(version: Version): String = when (version) {
    Version.HTTP_09 -> "HTTP/0.9"
    Version.HTTP_10 -> "HTTP/1.0"
    Version.HTTP_11 -> "HTTP/1.1"
    else -> protocolError(ProtocolError.WrongHttpVersion)
}

/**
 * Bounds on reading a handshake head (tungstenite `AttackCheck`, `machine.rs:148-190`, whose values
 * are hard-coded; SPEC §3.1). The defaults are the reference's. Exceeding any is
 * [WebSocketException.AttackAttempt].
 *
 * @property maxBytes most bytes read before the head is complete.
 * @property maxPackets most reads before the head is complete.
 * @property minPacketSize once more than [minPacketCheckThreshold] reads were made, the average
 *   read must be at least this many bytes (slow-loris protection).
 * @property minPacketCheckThreshold reads before [minPacketSize] is enforced.
 */
data class HandshakeLimits(
    val maxBytes: Int = 65536,
    val maxPackets: Int = 512,
    val minPacketSize: Int = 128,
    val minPacketCheckThreshold: Int = 64,
    /**
     * ⚖️ The whole handshake (request and response) within this long, else [neton.websocket.WebSocketException.Timeout]
     * ("handshake"); 0: no limit, as the reference. Default 10 s (SPEC §11.8).
     */
    val timeoutMillis: Long = 10_000,
) {
    init {
        require(maxBytes > 0 && maxPackets > 0 && minPacketSize >= 0 && minPacketCheckThreshold >= 0 && timeoutMillis >= 0) {
            "invalid handshake limits: $this"
        }
    }
}

/** Run a handshake under [HandshakeLimits.timeoutMillis]. */
internal suspend fun <T> withinHandshakeLimit(limits: HandshakeLimits, block: suspend () -> T): T {
    if (limits.timeoutMillis == 0L) return block()
    return try {
        kotlinx.coroutines.withTimeout(limits.timeoutMillis) { block() }
    } catch (e: kotlinx.coroutines.TimeoutCancellationException) {
        throw neton.websocket.WebSocketException.Timeout("handshake", limits.timeoutMillis)
    }
}

/** The counters of [HandshakeLimits] (tungstenite `AttackCheck`). */
internal class AttackCheck(private val limits: HandshakeLimits) {
    private var packets = 0L
    private var bytes = 0L

    /** Call after each successful read with its byte count (`check_incoming_packet_size`). */
    fun checkIncomingPacketSize(size: Int) {
        packets++
        bytes += size
        if (bytes > limits.maxBytes) throw WebSocketException.AttackAttempt()
        if (packets > limits.maxPackets) throw WebSocketException.AttackAttempt()
        if (packets > limits.minPacketCheckThreshold && packets * limits.minPacketSize > bytes) {
            throw WebSocketException.AttackAttempt()
        }
    }
}

/**
 * A parse of a head in `bytes[offset, offset + length)`: returns null while incomplete, else the
 * number of bytes consumed and the value (the reference's `TryParse`). Throws on a syntax error.
 */
internal fun interface TryParse<T> {
    fun tryParse(bytes: ByteArray, offset: Int, length: Int): Pair<Int, T>?
}

/**
 * Reads one HTTP head from the peer (the reading state of tungstenite `HandshakeMachine`,
 * `machine.rs:42-80`), without I/O: the driver reads into [input], then calls [received] with the
 * count, or [receivedEof].
 *
 * ⚖️ Incremental parsing (SPEC §3.1; the reference re-parses the whole buffer after every read,
 * its TODO at `machine.rs:50`): the head is parsed only once its end (an empty line) has arrived,
 * found by scanning just the new bytes. Accepted heads, their values and the error kinds are the
 * reference's; an error inside an unfinished head is reported when the head ends, at end of
 * stream, or when a limit is hit, instead of right after the read that brought it.
 */
internal class HeadReader<T>(limits: HandshakeLimits, private val parser: TryParse<T>) {
    private val check = AttackCheck(limits)
    private var scanned = 0

    /** Full parses made so far (tests of the incremental parsing). */
    var parses = 0
        private set

    /** Where the driver appends what it reads. */
    val input = Buffer(READ_CHUNK)

    /** Room for the next read (the reference reads 4 KiB at a time). */
    fun prepareRead() { input.reserve(READ_CHUNK) }

    /**
     * [n] (> 0) bytes were appended to [input]. Returns the head and the bytes after it once the
     * head is complete, else null (read more).
     */
    fun received(n: Int): Pair<T, Bytes>? {
        try {
            check.checkIncomingPacketSize(n)
        } catch (e: WebSocketException.AttackAttempt) {
            // The reference would have reported an error in the earlier bytes already.
            parseOrNull(input.readableBytes - n)
            throw e
        }
        val a = input.backingArray()
        val off = input.readerIndex()
        val len = input.readableBytes
        if (!isCompleteFast(a, off, len, scanned)) {
            scanned = len
            return null
        }
        scanned = len
        val (size, head) = parseOrNull(len) ?: return null
        input.skip(size)
        return head to input.readSlice(input.readableBytes)
    }

    /** The stream ended before the head did: the error in what arrived, else `HandshakeIncomplete`. */
    fun receivedEof(): Nothing {
        parseOrNull(input.readableBytes)
        protocolError(ProtocolError.HandshakeIncomplete)
    }

    private fun parseOrNull(len: Int): Pair<Int, T>? {
        parses++
        return parser.tryParse(input.backingArray(), input.readerIndex(), len)
    }

    private companion object {
        const val READ_CHUNK = 4096
    }
}

/**
 * Throws the reference's error for a failed parse status (`From<httparse::Error>`, `error.rs:125-133`):
 * too many headers is a capacity error, the rest are protocol errors.
 */
internal fun throwParseError(status: Int): Nothing {
    val e = ParseStatus.error(status) ?: error("not a parse error: $status")
    if (e == HttpParseError.TooManyHeaders) throw WebSocketException.Capacity(CapacityError.TooManyHeaders)
    protocolError(ProtocolError.HttparseError(e))
}

/** An HTTP value error becomes [WebSocketException.HttpFormat] (`From<http::Error>` and friends). */
internal inline fun <R> httpFormat(block: () -> R): R = try {
    block()
} catch (e: HttpException) {
    throw WebSocketException.HttpFormat(e)
}

/** Parsed header slots to a [HeaderMap] (`FromHttparse for HeaderMap`, `headers.rs:18-32`). */
internal fun headersFrom(slots: HeaderSlots, bytes: ByteArray): HeaderMap<HeaderValue> = httpFormat {
    val headers = HeaderMap.new()
    for (i in 0 until slots.count) {
        val ns = slots.nameStart[i]
        val vs = slots.valueStart[i]
        headers.append(
            HeaderName.fromBytes(bytes, ns, slots.nameEnd[i] - ns),
            HeaderValue.fromBytes(bytes, vs, slots.valueEnd[i] - vs),
        )
    }
    headers
}

/**
 * Parse a block of header lines ended by an empty line (`TryParse for HeaderMap`, `headers.rs:33-42`):
 * null while incomplete, else the bytes consumed and the headers.
 */
fun tryParseHeaders(bytes: ByteArray, offset: Int = 0, length: Int = bytes.size - offset): Pair<Int, HeaderMap<HeaderValue>>? {
    val slots = HeaderSlots(MAX_HEADERS)
    val status = parseHeaders(bytes, offset, length, slots)
    return when {
        ParseStatus.isComplete(status) -> status to headersFrom(slots, bytes)
        ParseStatus.isPartial(status) -> null
        else -> throwParseError(status)
    }
}

/**
 * Whether a header value, split at spaces and commas, holds [token] ignoring ASCII case (the
 * server's `Connection` check, `server.rs:46-53`). A value that is not visible ASCII has no tokens.
 */
internal fun HeaderValue?.hasToken(token: String): Boolean {
    val s = this?.tryToStr() ?: return false
    return s.split(' ', ',').any { it.equals(token, ignoreCase = true) }
}

/** The whole value equals [expected] ignoring ASCII case; a non-ASCII value never does. */
internal fun HeaderValue?.equalsIgnoreCase(expected: String): Boolean =
    this?.tryToStr()?.equals(expected, ignoreCase = true) ?: false

/** `to_str()?`: the value as a string, or [WebSocketException.Utf8] when it is not visible ASCII. */
internal fun HeaderValue.toStrOrThrow(): String = tryToStr() ?: throw WebSocketException.Utf8("failed to convert header to a str")
