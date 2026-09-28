package neton.websocket.frame

/**
 * WebSocket opcode as in RFC 6455 (tungstenite `OpCode`, `T/src/protocol/frame/coding.rs:8-117`).
 *
 * [code] is the 4-bit wire value; [from] returns shared instances, so decoding allocates nothing.
 * `toString()` is the reference's `Display` (`TEXT`, `RESERVED_DATA_3`, ...).
 */
sealed class OpCode {
    /** The 4-bit wire value (tungstenite `From<OpCode> for u8`). */
    abstract val code: Int

    /** Data opcodes: 0x0 continuation, 0x1 text, 0x2 binary, 0x3–0x7 reserved. */
    sealed class Data : OpCode() {
        data object Continue : Data() { override val code get() = 0; override fun toString() = "CONTINUE" }
        data object Text : Data() { override val code get() = 1; override fun toString() = "TEXT" }
        data object Binary : Data() { override val code get() = 2; override fun toString() = "BINARY" }
        data class Reserved(override val code: Int) : Data() {
            init { require(code in 3..7) { "reserved data opcode out of range: $code" } }
            override fun toString() = "RESERVED_DATA_$code"
        }
    }

    /** Control opcodes: 0x8 close, 0x9 ping, 0xA pong, 0xB–0xF reserved. */
    sealed class Control : OpCode() {
        data object Close : Control() { override val code get() = 8; override fun toString() = "CLOSE" }
        data object Ping : Control() { override val code get() = 9; override fun toString() = "PING" }
        data object Pong : Control() { override val code get() = 10; override fun toString() = "PONG" }
        data class Reserved(override val code: Int) : Control() {
            init { require(code in 11..15) { "reserved control opcode out of range: $code" } }
            override fun toString() = "RESERVED_CONTROL_$code"
        }
    }

    companion object {
        private val TABLE: Array<OpCode> = Array(16) { i ->
            when (i) {
                0 -> Data.Continue
                1 -> Data.Text
                2 -> Data.Binary
                in 3..7 -> Data.Reserved(i)
                8 -> Control.Close
                9 -> Control.Ping
                10 -> Control.Pong
                else -> Control.Reserved(i)
            }
        }

        /**
         * The opcode for a 4-bit wire value (tungstenite `From<u8> for OpCode`).
         * @throws IllegalArgumentException outside 0..15 (the reference panics).
         */
        fun from(code: Int): OpCode {
            require(code in 0..15) { "Bug: OpCode out of range: $code" }
            return TABLE[code]
        }
    }
}

// Wire values used on the hot paths without touching the sealed classes.
internal const val OP_CONTINUE = 0
internal const val OP_TEXT = 1
internal const val OP_BINARY = 2
internal const val OP_CLOSE = 8
internal const val OP_PING = 9
internal const val OP_PONG = 10

/**
 * Status code of a close frame (tungstenite `CloseCode`, `coding.rs:119-259`): the named codes of
 * RFC 6455 plus the ranges [Reserved] (1016–2999), [Iana] (3000–3999), [Library] (4000–4999) and
 * [Bad] (everything else: 0–999, 1004, 1014 and 5000 and above).
 * `toString()` is the numeric code, like the reference's `Display`.
 */
sealed class CloseCode {
    /** The numeric code (tungstenite `From<CloseCode> for u16`). */
    abstract val code: Int

    final override fun toString(): String = code.toString()

    /**
     * Whether the code may appear in a close frame on the wire (`coding.rs:193-197`): false for
     * [Bad], [Reserved], [Status] (1005), [Abnormal] (1006) and [Tls] (1015).
     */
    val isAllowed: Boolean
        get() = !(this is Bad || this is Reserved || this === Status || this === Abnormal || this === Tls)

    /** 1000: the purpose of the connection was fulfilled. */
    data object Normal : CloseCode() { override val code get() = 1000 }
    /** 1001: the endpoint is going away (server shutdown, page navigation). */
    data object Away : CloseCode() { override val code get() = 1001 }
    /** 1002: protocol error. */
    data object Protocol : CloseCode() { override val code get() = 1002 }
    /** 1003: received a type of data it cannot accept. */
    data object Unsupported : CloseCode() { override val code get() = 1003 }
    /** 1005: no status code was present (never sent on the wire). */
    data object Status : CloseCode() { override val code get() = 1005 }
    /** 1006: abnormal closure (never sent on the wire). */
    data object Abnormal : CloseCode() { override val code get() = 1006 }
    /** 1007: data inconsistent with the message type (e.g. non-UTF-8 text). */
    data object Invalid : CloseCode() { override val code get() = 1007 }
    /** 1008: policy violation. */
    data object Policy : CloseCode() { override val code get() = 1008 }
    /** 1009: message too big. */
    data object Size : CloseCode() { override val code get() = 1009 }
    /** 1010: the client expected an extension the server did not negotiate. */
    data object Extension : CloseCode() { override val code get() = 1010 }
    /** 1011: unexpected server condition. */
    data object Error : CloseCode() { override val code get() = 1011 }
    /** 1012: the server is restarting. */
    data object Restart : CloseCode() { override val code get() = 1012 }
    /** 1013: the server is overloaded, try again later. */
    data object Again : CloseCode() { override val code get() = 1013 }
    /** 1015: TLS handshake failure (never sent on the wire). */
    data object Tls : CloseCode() { override val code get() = 1015 }
    /** 1016–2999: reserved for future RFCs. */
    data class Reserved(override val code: Int) : CloseCode()
    /** 3000–3999: registered with IANA. */
    data class Iana(override val code: Int) : CloseCode()
    /** 4000–4999: private use by applications and libraries. */
    data class Library(override val code: Int) : CloseCode()
    /** 0–999, 1004, 1014, and 5000 and above: never valid (the reference maps 1004 / 1014 here too). */
    data class Bad(override val code: Int) : CloseCode()

    companion object {
        /**
         * The close code for a 16-bit value (tungstenite `From<u16> for CloseCode`, `coding.rs:233-258`).
         * Values outside 0..65535 are [Bad].
         */
        fun from(code: Int): CloseCode = when (code) {
            1000 -> Normal
            1001 -> Away
            1002 -> Protocol
            1003 -> Unsupported
            1005 -> Status
            1006 -> Abnormal
            1007 -> Invalid
            1008 -> Policy
            1009 -> Size
            1010 -> Extension
            1011 -> Error
            1012 -> Restart
            1013 -> Again
            1015 -> Tls
            in 1..999 -> Bad(code)
            in 1016..2999 -> Reserved(code)
            in 3000..3999 -> Iana(code)
            in 4000..4999 -> Library(code)
            else -> Bad(code)
        }
    }
}
