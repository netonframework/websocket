@file:OptIn(ExperimentalForeignApi::class, kotlin.experimental.ExperimentalNativeApi::class)

package neton.websocket

import kotlinx.cinterop.CPointer
import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.UByteVar
import kotlinx.cinterop.addressOf
import kotlinx.cinterop.alloc
import kotlinx.cinterop.free
import kotlinx.cinterop.nativeHeap
import kotlinx.cinterop.pointed
import kotlinx.cinterop.ptr
import kotlinx.cinterop.reinterpret
import kotlinx.cinterop.toKString
import kotlinx.cinterop.sizeOf
import kotlinx.cinterop.usePinned
import platform.zlib.Z_BUF_ERROR
import platform.zlib.Z_DATA_ERROR
import platform.zlib.Z_DEFLATED
import platform.zlib.Z_DEFAULT_STRATEGY
import platform.zlib.Z_NEED_DICT
import platform.zlib.Z_OK
import platform.zlib.Z_STREAM_END
import platform.zlib.Z_SYNC_FLUSH
import platform.zlib.ZLIB_VERSION
import platform.zlib.deflate
import platform.zlib.deflateEnd
import platform.zlib.deflateInit2_
import platform.zlib.deflateReset
import platform.zlib.inflate
import platform.zlib.inflateEnd
import platform.zlib.inflateInit2_
import platform.zlib.inflateReset
import platform.zlib.z_stream
import kotlin.native.ref.Cleaner
import kotlin.native.ref.createCleaner

/** A z_stream on the native heap; [end] is deflateEnd or inflateEnd. Freed once, by [release] or the cleaner. */
private class ZStream(private val end: (CPointer<z_stream>) -> Unit) {
    val ptr: CPointer<z_stream> = nativeHeap.alloc<z_stream>().ptr
    var live = false

    fun free() {
        if (live) { end(ptr); live = false }
        nativeHeap.free(ptr.rawValue)
    }
}

/** Frees [stream] exactly once: by [release], else by the GC cleaner. */
private class Owner(stream: ZStream) {
    var stream: ZStream? = stream
    fun release() { stream?.free(); stream = null }
}

internal actual fun rawDeflater(level: Int, windowBits: Int): RawDeflater = ZlibDeflater(level, windowBits)

internal actual fun rawInflater(windowBits: Int): RawInflater = ZlibInflater(windowBits)

private class ZlibDeflater(private val level: Int, private val windowBits: Int) : RawDeflater {
    private val owner = Owner(ZStream { deflateEnd(it) })
    @Suppress("unused") private val cleaner: Cleaner = createCleaner(owner) { it.release() }

    private fun stream(): CPointer<z_stream> {
        val s = checkNotNull(owner.stream) { "deflater released" }
        if (!s.live) {
            // Negative window bits: raw deflate. memLevel 8 is zlib's default.
            val rc = deflateInit2_(s.ptr, level, Z_DEFLATED, -windowBits, 8, Z_DEFAULT_STRATEGY, ZLIB_VERSION, sizeOf<z_stream>().toInt())
            check(rc == Z_OK) { "deflateInit2 failed: $rc" }
            s.live = true
        }
        return s.ptr
    }

    override fun compress(src: ByteArray, off: Int, len: Int, out: ByteArray, outOff: Int): Int {
        val z = stream()
        val room = out.size - outOff
        val written = src.usePinned { si -> out.usePinned { so ->
            val zs = z.pointed
            zs.next_in = if (len == 0) null else si.addressOf(off).reinterpret<UByteVar>()
            zs.avail_in = len.toUInt()
            zs.next_out = so.addressOf(outOff).reinterpret<UByteVar>()
            zs.avail_out = room.toUInt()
            val rc = deflate(z, Z_SYNC_FLUSH)
            check(rc == Z_OK || rc == Z_BUF_ERROR) { "deflate failed: $rc" }
            check(zs.avail_in == 0u && zs.avail_out > 0u) { "deflate output exceeded its bound" }
            val n = room - zs.avail_out.toInt()
            zs.next_in = null; zs.next_out = null
            n
        } }
        // Nothing new since the last flush (an empty message after another): zlib writes nothing. The RFC's empty
        // message is one 0x00 byte (§7.2.3.6), which the receiver completes with 00 00 ff ff to an empty stored block.
        if (written == 0) { out[outOff] = 0; return 1 }
        // A sync flush ends with an empty stored block, 00 00 ff ff: the sender removes it (RFC 7692 §7.2.1).
        check(written >= 4) { "deflate wrote no sync flush" }
        return written - 4
    }

    override fun reset() {
        val s = owner.stream ?: return
        if (s.live) deflateReset(s.ptr)
    }

    override fun release() = owner.release()
}

private class ZlibInflater(private val windowBits: Int) : RawInflater {
    private val owner = Owner(ZStream { inflateEnd(it) })
    @Suppress("unused") private val cleaner: Cleaner = createCleaner(owner) { it.release() }
    private val chunk = ByteArray(CHUNK)

    private fun stream(): CPointer<z_stream> {
        val s = checkNotNull(owner.stream) { "inflater released" }
        if (!s.live) {
            val rc = inflateInit2_(s.ptr, -windowBits, ZLIB_VERSION, sizeOf<z_stream>().toInt())
            check(rc == Z_OK) { "inflateInit2 failed: $rc" }
            s.live = true
        }
        return s.ptr
    }

    override fun inflate(src: ByteArray, off: Int, len: Int, sink: (ByteArray, Int, Int) -> Unit) {
        if (len == 0) return
        val z = stream()
        src.usePinned { si -> chunk.usePinned { so ->
            val zs = z.pointed
            zs.next_in = si.addressOf(off).reinterpret<UByteVar>()
            zs.avail_in = len.toUInt()
            try {
                while (true) {
                    zs.next_out = so.addressOf(0).reinterpret<UByteVar>()
                    zs.avail_out = CHUNK.toUInt()
                    val rc = inflate(z, Z_SYNC_FLUSH)
                    val n = CHUNK - zs.avail_out.toInt()
                    when (rc) {
                        Z_OK, Z_BUF_ERROR -> {}
                        // A final block ends the stream; RFC 7692 §7.2.2 lets the peer set BFINAL. What follows (the
                        // appended 00 00 ff ff) must then be nothing more; the next message starts a new stream.
                        Z_STREAM_END -> inflateReset(z)
                        Z_DATA_ERROR, Z_NEED_DICT -> throw WebSocketException.Protocol(
                            ProtocolError.InvalidCompressedData(zs.msg?.toKString() ?: "inflate error $rc"),
                        )
                        else -> error("inflate failed: $rc")
                    }
                    if (n > 0) sink(chunk, 0, n)
                    if (zs.avail_in == 0u && zs.avail_out > 0u) break
                    if (rc == Z_BUF_ERROR && n == 0) break
                }
            } finally {
                zs.next_in = null; zs.next_out = null
            }
        } }
    }

    override fun reset() {
        val s = owner.stream ?: return
        if (s.live) inflateReset(s.ptr)
    }

    override fun release() = owner.release()

    private companion object {
        const val CHUNK = 16 * 1024
    }
}
