package neton.websocket

import neton.io.bytes.Buffer
import neton.io.bytes.Bytes
import neton.websocket.frame.CloseFrame

/**
 * A WebSocket message (tungstenite `Message`, `T/src/protocol/message.rs:155-280`).
 *
 * `toString()` is the reference's `Display`: the text when the payload is UTF-8, otherwise
 * `Binary Data<length=N>`. [Frame] is only for sending raw frames; reading never produces it.
 */
sealed interface Message {
    /** A text message. */
    data class Text(val text: Utf8Bytes) : Message {
        constructor(text: String) : this(Utf8Bytes.from(text))
        override fun toString(): String = text.asString()
    }

    /** A binary message. */
    data class Binary(val data: Bytes) : Message {
        override fun toString(): String = display()
    }

    /** A ping; the payload must be 125 bytes or less. */
    data class Ping(val data: Bytes) : Message {
        override fun toString(): String = display()
    }

    /** A pong; the payload must be 125 bytes or less. */
    data class Pong(val data: Bytes) : Message {
        override fun toString(): String = display()
    }

    /** A close message with an optional close frame. */
    data class Close(val frame: CloseFrame?) : Message {
        override fun toString(): String = display()
    }

    /** A raw frame, for sending only (e.g. to fragment a message). */
    data class Frame(val frame: neton.websocket.frame.Frame) : Message {
        override fun toString(): String = display()
    }

    val isText: Boolean get() = this is Text
    val isBinary: Boolean get() = this is Binary
    val isPing: Boolean get() = this is Ping
    val isPong: Boolean get() = this is Pong
    val isClose: Boolean get() = this is Close

    /** Payload length in bytes (for [Close], of the reason; for [Frame], of the whole frame). */
    val length: Int
        get() = when (this) {
            is Text -> text.size
            is Binary -> data.size
            is Ping -> data.size
            is Pong -> data.size
            is Close -> frame?.reason?.size ?: 0
            is Frame -> frame.length
        }

    /** True when there is no content (e.g. the peer sent an empty string). */
    val isEmpty: Boolean get() = length == 0

    /** The payload as bytes (for [Close], the reason). */
    fun intoData(): Bytes = when (this) {
        is Text -> text.bytes
        is Binary -> data
        is Ping -> data
        is Pong -> data
        is Close -> frame?.reason?.bytes ?: Bytes.EMPTY
        is Frame -> frame.intoPayload()
    }

    /** The payload as text; @throws WebSocketException.Utf8 when it is not UTF-8. */
    fun intoText(): Utf8Bytes = when (this) {
        is Text -> text
        is Binary -> Utf8Bytes.tryFrom(data)
        is Ping -> Utf8Bytes.tryFrom(data)
        is Pong -> Utf8Bytes.tryFrom(data)
        is Close -> frame?.reason ?: Utf8Bytes.EMPTY
        is Frame -> frame.intoText()
    }

    /** The payload decoded as a [String]; @throws WebSocketException.Utf8 when it is not UTF-8. */
    fun toText(): String = intoText().asString()

    companion object {
        /** A text message. */
        fun text(text: String): Message = Text(Utf8Bytes.from(text))

        /** A text message from validated bytes. */
        fun text(text: Utf8Bytes): Message = Text(text)

        /** A binary message holding a copy of [data] (the reference's `From<&[u8]>`). */
        fun binary(data: ByteArray): Message = Binary(Bytes.copyOf(data))

        /** A binary message over [data], no copy (the reference's `From<Bytes>` / `From<Vec<u8>>`). */
        fun binary(data: Bytes): Message = Binary(data)
    }
}

private fun Message.display(): String =
    try { toText() } catch (_: WebSocketException.Utf8) { "Binary Data<length=$length>" }

/**
 * A message being reassembled from fragments (tungstenite `IncompleteMessage`, `message.rs:78-152`).
 *
 * Fragments are appended to one buffer; text is validated fragment by fragment with a code point
 * split across fragments carried over, and no [String] is made. The finished payload is handed out
 * as a slice of that buffer, without another copy.
 */
internal class IncompleteMessage(private val isText: Boolean) {
    private val buf = Buffer(1024)
    private val utf8: Utf8Validator? = if (isText) Utf8Validator() else null

    /** Bytes collected so far. */
    val length: Int get() = buf.readableBytes

    /**
     * Append `a[off, off + len)`: first the overflow-safe size check against [maxSize], then
     * (text) validation, then the copy.
     */
    fun extend(a: ByteArray, off: Int, len: Int, maxSize: Int?) {
        val max = (maxSize ?: Int.MAX_VALUE).toLong()
        val my = length.toLong()
        if (my > max || len > max - my) {
            throw WebSocketException.Capacity(CapacityError.MessageTooLong(my + len, max))
        }
        if (utf8 != null) {
            val bad = utf8.feed(a, off, off + len)
            if (bad >= 0) throw WebSocketException.Utf8("invalid utf-8 sequence from index ${my + (bad - off)}")
        }
        buf.writeBytes(a, off, len)
    }

    /** The finished message; @throws WebSocketException.Utf8 if text ends inside a code point. */
    fun complete(): Message {
        if (utf8 != null && !utf8.isComplete) {
            throw WebSocketException.Utf8("incomplete utf-8 byte sequence at the end of the message")
        }
        val data = buf.readSlice(buf.readableBytes)
        return if (isText) Message.Text(Utf8Bytes.unchecked(data)) else Message.Binary(data)
    }
}
