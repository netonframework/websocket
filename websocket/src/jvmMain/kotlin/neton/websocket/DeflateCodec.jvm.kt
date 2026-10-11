package neton.websocket

import java.util.zip.DataFormatException
import java.util.zip.Deflater
import java.util.zip.Inflater

// permessage-deflate on the JVM's zlib (java.util.zip, nowrap: raw DEFLATE), with the native codec's behaviour.
// ⚖️ A Deflater's window is always 15 bits: when the negotiation asks this side for a smaller one, messages are sent as
// stored blocks (level 0), which refer back to nothing and so fit any window — valid, only not compressed. An
// Inflater's 15-bit window reads data made with any smaller window.

internal actual fun rawDeflater(level: Int, windowBits: Int): RawDeflater = JvmDeflater(if (windowBits < 15) 0 else level)

internal actual fun rawInflater(windowBits: Int): RawInflater = JvmInflater()

private class JvmDeflater(private val level: Int) : RawDeflater {
    private var deflater: Deflater? = null

    private fun deflater(): Deflater = deflater ?: Deflater(level, true).also { deflater = it }

    override fun compress(src: ByteArray, off: Int, len: Int, out: ByteArray, outOff: Int): Int {
        val d = deflater()
        d.setInput(src, off, len)
        val room = out.size - outOff
        val written = d.deflate(out, outOff, room, Deflater.SYNC_FLUSH)
        check(d.needsInput() && written < room) { "deflate output exceeded its bound" }
        // Nothing new since the last flush (an empty message after another): zlib writes nothing. The RFC's empty
        // message is one 0x00 byte (§7.2.3.6), which the receiver completes with 00 00 ff ff to an empty stored block.
        if (written == 0) { out[outOff] = 0; return 1 }
        // A sync flush ends with an empty stored block, 00 00 ff ff: the sender removes it (RFC 7692 §7.2.1).
        check(written >= 4) { "deflate wrote no sync flush" }
        return written - 4
    }

    override fun reset() { deflater?.reset() }

    override fun release() {
        deflater?.end()
        deflater = null
    }
}

private class JvmInflater : RawInflater {
    private var inflater: Inflater? = null
    private val chunk = ByteArray(CHUNK)

    private fun inflater(): Inflater = inflater ?: Inflater(true).also { inflater = it }

    override fun inflate(src: ByteArray, off: Int, len: Int, sink: (ByteArray, Int, Int) -> Unit) {
        if (len == 0) return
        val z = inflater()
        z.setInput(src, off, len)
        while (true) {
            val n = try {
                z.inflate(chunk, 0, CHUNK)
            } catch (e: DataFormatException) {
                throw WebSocketException.Protocol(ProtocolError.InvalidCompressedData(e.message ?: "inflate error"))
            }
            if (n > 0) sink(chunk, 0, n)
            if (z.needsDictionary()) {
                throw WebSocketException.Protocol(ProtocolError.InvalidCompressedData("a preset dictionary is required"))
            }
            if (z.finished()) {
                // A final block ends the stream (RFC 7692 §7.2.2 lets the peer set BFINAL); what follows starts anew.
                val remaining = z.remaining
                z.reset()
                if (remaining == 0) break
                z.setInput(src, off + len - remaining, remaining)
                continue
            }
            if (n == 0 && z.needsInput()) break
        }
    }

    override fun reset() { inflater?.reset() }

    override fun release() {
        inflater?.end()
        inflater = null
    }

    private companion object {
        const val CHUNK = 16 * 1024
    }
}
