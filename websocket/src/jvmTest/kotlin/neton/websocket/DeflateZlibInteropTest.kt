package neton.websocket

import java.io.File
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals

/**
 * permessage-deflate against an independent zlib: CPython's, through a child `python3`, with exactly the negotiated
 * window (raw DEFLATE, wbits = -bits). For each window 9..15, with and without context takeover:
 * - JVM to zlib: messages compressed here are inflated there, with `00 00 ff ff` appended per message (RFC 7692
 *   §7.2.2) and one decompressor per connection, or per message without context takeover.
 * - zlib to JVM: messages zlib compresses with that window and a sync flush, tail removed, are inflated here.
 */
class DeflateZlibInteropTest {
    private val messages = listOf(
        ByteArray(3000) { "the quick brown fox jumps over the lazy dog. "[it % 45].code.toByte() },
        ByteArray(3000) { "the quick brown fox jumps over the lazy dog. "[it % 45].code.toByte() },
        ByteArray(0),
        ByteArray(70_000) { (it % 251).toByte() },
        ByteArray(5000) { ((it * 2654435761L) ushr 13).toByte() },
        // A random block twice: its only matches lie 4000 bytes back, beyond every window under 12 bits.
        kotlin.random.Random(7).nextBytes(4000).let { it + it },
    )

    @Test fun jvmToZlib() {
        for (bits in 9..15) for (noCtx in listOf(false, true)) {
            val dir = tempDir()
            val deflater = rawDeflater(6, bits)
            messages.forEachIndexed { i, m ->
                if (noCtx) deflater.reset()
                val out = ByteArray(deflateBound(m.size))
                val n = deflater.compress(m, 0, m.size, out, 0)
                File(dir, "$i.z").writeBytes(out.copyOf(n))
                File(dir, "$i.raw").writeBytes(m)
            }
            deflater.release()
            assertEquals("ok", python(INFLATE, dir, bits, noCtx), "window $bits, no context takeover $noCtx")
        }
    }

    @Test fun zlibToJvm() {
        for (bits in 9..15) for (noCtx in listOf(false, true)) {
            val dir = tempDir()
            messages.forEachIndexed { i, m -> File(dir, "$i.raw").writeBytes(m) }
            assertEquals("ok", python(DEFLATE, dir, bits, noCtx), "window $bits, no context takeover $noCtx")
            val inflater = rawInflater(bits)
            messages.indices.forEach { i ->
                if (noCtx) inflater.reset()
                val z = File(dir, "$i.z").readBytes() + DEFLATE_TAIL
                val got = java.io.ByteArrayOutputStream()
                inflater.inflate(z, 0, z.size) { b, off, len -> got.write(b, off, len) }
                assertContentEquals(messages[i], got.toByteArray(), "message $i, window $bits, no context takeover $noCtx")
            }
            inflater.release()
        }
    }

    private fun tempDir(): File = File.createTempFile("deflate-interop", "").also { it.delete(); it.mkdir(); it.deleteOnExit() }

    private fun python(script: String, dir: File, bits: Int, noCtx: Boolean): String {
        val exe = listOf("python3", "python").firstOrNull { runCatching { ProcessBuilder(it, "--version").start().waitFor() == 0 }.getOrDefault(false) }
            ?: error("python3 is required for the independent zlib")
        // The script goes through a file, not `-c`: on Windows ProcessBuilder does not escape the quotes inside an
        // argument, so a script passed inline reaches Python with its string literals stripped.
        val file = File(dir, "interop.py").apply { writeText(script) }
        val p = ProcessBuilder(exe, "-I", file.path, dir.path, bits.toString(), if (noCtx) "1" else "0", messages.size.toString())
            .redirectErrorStream(true).start()
        val output = p.inputStream.bufferedReader().readText().trim()
        check(p.waitFor(60, TimeUnit.SECONDS)) { "python did not finish" }
        return output
    }

    private companion object {
        val INFLATE = """
import sys, zlib, os
d, bits, noctx, n = sys.argv[1], int(sys.argv[2]), sys.argv[3] == "1", int(sys.argv[4])
z = zlib.decompressobj(-bits)
for i in range(n):
    if noctx: z = zlib.decompressobj(-bits)
    data = open(os.path.join(d, f"{i}.z"), "rb").read() + b"\x00\x00\xff\xff"
    # In small output steps: zlib only checks a distance against its window when the bytes it refers to are not
    # still in the caller's output buffer, so one large call would accept references beyond the window.
    got, buf = b"", data
    while True:
        chunk = z.decompress(buf, 256)
        got += chunk
        buf = z.unconsumed_tail
        if not chunk and not buf:
            break
    if got != open(os.path.join(d, f"{i}.raw"), "rb").read():
        print(f"message {i} differs: {len(got)} bytes"); sys.exit(0)
print("ok")
""".trimIndent()

        val DEFLATE = """
import sys, zlib, os
d, bits, noctx, n = sys.argv[1], int(sys.argv[2]), sys.argv[3] == "1", int(sys.argv[4])
c = zlib.compressobj(6, zlib.DEFLATED, -bits)
for i in range(n):
    if noctx: c = zlib.compressobj(6, zlib.DEFLATED, -bits)
    raw = open(os.path.join(d, f"{i}.raw"), "rb").read()
    out = c.compress(raw) + c.flush(zlib.Z_SYNC_FLUSH)
    assert out.endswith(b"\x00\x00\xff\xff"), out[-8:]
    open(os.path.join(d, f"{i}.z"), "wb").write(out[:-4])
print("ok")
""".trimIndent()
    }
}
