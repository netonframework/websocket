package neton.websocket

/**
 * Raw DEFLATE (RFC 1951, no zlib header) for permessage-deflate (RFC 7692; SPEC §11.7), on the platform's zlib.
 * Native state is allocated on first use and released by [release] (a GC cleaner is only a backstop).
 */
internal interface RawDeflater {
    /**
     * Compress `src[off, off + len)` as one message (RFC 7692 §7.2.1): the data, then a sync flush, without the
     * flush's trailing `00 00 ff ff`. Writes into [out] from [outOff]; returns the bytes written. [out] must hold
     * [deflateBound] of [len].
     */
    fun compress(src: ByteArray, off: Int, len: Int, out: ByteArray, outOff: Int): Int

    /** Start the next message with an empty window (no context takeover). */
    fun reset()

    fun release()
}

/**
 * The most bytes [RawDeflater.compress] writes for [len] input bytes: stored blocks (5 bytes per 64 KiB) at worst, a
 * sync flush, a partial block. Larger than zlib's own bound, which takes a platform-width `uLong`.
 */
internal fun deflateBound(len: Int): Int = len + (len shr 10) + 64

internal interface RawInflater {
    /**
     * Inflate `src[off, off + len)`, handing each decompressed chunk to [sink] (which may throw, e.g. for a size
     * limit, and so stop a decompression bomb early).
     * @throws WebSocketException.Protocol for data that is not valid DEFLATE.
     */
    fun inflate(src: ByteArray, off: Int, len: Int, sink: (ByteArray, Int, Int) -> Unit)

    /** Start the next message with an empty window (the peer uses no context takeover). */
    fun reset()

    fun release()
}

/** A compressor at zlib [level] with a [windowBits] window. */
internal expect fun rawDeflater(level: Int, windowBits: Int): RawDeflater

/** A decompressor for a peer compressing with a [windowBits] window. */
internal expect fun rawInflater(windowBits: Int): RawInflater

/** The bytes a sync flush ends with, removed by the sender and added back by the receiver (RFC 7692 §7.2.2). */
internal val DEFLATE_TAIL = byteArrayOf(0, 0, 0xff.toByte(), 0xff.toByte())
