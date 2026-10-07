package neton.websocket

import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.addressOf
import kotlinx.cinterop.convert
import kotlinx.cinterop.pointed
import kotlinx.cinterop.toKString
import kotlinx.cinterop.usePinned
import neton.io.bytes.Buffer
import neton.websocket.frame.FrameHeader
import platform.posix.SEEK_END
import platform.posix.SEEK_SET
import platform.posix.closedir
import platform.posix.fclose
import platform.posix.fopen
import platform.posix.fread
import platform.posix.fseek
import platform.posix.ftell
import platform.posix.getenv
import platform.posix.opendir
import platform.posix.readdir
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.fail

/**
 * The reference's three fuzz targets (SPEC §9: `T/fuzz/fuzz_targets/{parse_frame_header,read_message_client,
 * read_message_server}.rs`) as deterministic tests: each seed of the reference corpus (`T/fuzz/seeds/<target>`), a few
 * seeded mutations of it, and random inputs that run without the corpus. An input must not crash or hang, and may only
 * fail with a [WebSocketException]; the reading targets feed it whole and in random chunks.
 *
 * The corpus is read from `NETON_WS_FUZZ_SEEDS`, else from the reference checkout under `$HOME`; without it only the
 * random inputs run, and the test says so.
 */
class FuzzTest {
    @Test
    fun parseFrameHeader() = runTarget("parse_frame_header", mutants = 64) { data, _ ->
        val b = Buffer()
        b.writeBytes(data)
        try {
            while (true) {
                val before = b.readableBytes
                val parsed = FrameHeader.parse(b) ?: break
                check(b.readableBytes < before) { "a parsed header consumed nothing" }
                // As a frame socket would: skip the payload when it is there, else stop.
                if (parsed.length > b.readableBytes) break
                b.skip(parsed.length.toInt())
            }
        } catch (_: WebSocketException.Protocol) {
        }
    }

    @Test
    fun readMessageClient() = runTarget("read_message_client", mutants = 32) { data, rnd -> readAll(Role.Client, data, rnd) }

    @Test
    fun readMessageServer() = runTarget("read_message_server", mutants = 32) { data, rnd -> readAll(Role.Server, data, rnd) }

    /** Feed [data] to a fresh core (whole when [rnd] is null, else in random chunks), reading until it needs more or fails. */
    private fun readAll(role: Role, data: ByteArray, rnd: Random?) {
        val core = WebSocketCore(role)
        var off = 0
        var reads = 0
        try {
            while (true) {
                if (off < data.size) {
                    val n = if (rnd == null) data.size - off else minOf(data.size - off, 1 + rnd.nextInt(minOf(4096, core.maxReadSize)))
                    core.input.writeBytes(data, off, n)
                    off += n
                }
                if (off == data.size) core.receivedEof()
                while (core.read() != null) {
                    messages++
                    // Every message consumes at least a frame header: more reads than bytes would mean a loop.
                    if (++reads > data.size + 16) fail("${data.size} input bytes gave more than $reads messages")
                }
                // At the end of the input a read ends or fails; it never waits for more.
                if (off == data.size) fail("read returned null at end of input")
            }
        } catch (e: WebSocketException) {
            outcomes[e::class.simpleName!!] = (outcomes[e::class.simpleName!!] ?: 0) + 1
        }
    }

    // What the inputs reached, printed per target: decoded messages and how reading ended.
    private var messages = 0L
    private val outcomes = HashMap<String, Int>()

    private fun runTarget(name: String, mutants: Int, body: (ByteArray, Random?) -> Unit) {
        val seeds = loadSeeds(name)
        val rnd = Random(name.hashCode())
        val inputs = ArrayList<ByteArray>()
        for (seed in seeds) {
            inputs += seed
            repeat(mutants) { inputs += mutate(seed, rnd) }
        }
        repeat(2_000) { inputs += randomInput(rnd) }
        for ((i, input) in inputs.withIndex()) {
            for (chunked in listOf(false, true)) {
                try {
                    body(input, if (chunked) Random(i) else null)
                } catch (e: AssertionError) {
                    throw e
                } catch (e: Throwable) {
                    fail("$name input $i (${input.size} bytes${if (chunked) ", chunked" else ""}): ${e::class.simpleName}: ${e.message}", e)
                }
            }
        }
        println("FuzzTest.$name: ${seeds.size} seeds, ${inputs.size} inputs" +
            (if (outcomes.isEmpty()) "" else ", $messages messages, ended by ${outcomes.entries.sortedBy { it.key }}") +
            if (seeds.isEmpty()) " (no corpus: set NETON_WS_FUZZ_SEEDS to the reference's fuzz/seeds)" else "")
    }

    /** A random byte string, or random frames with random header fields, lengths and masks. */
    private fun randomInput(rnd: Random): ByteArray {
        if (rnd.nextBoolean()) return rnd.nextBytes(rnd.nextInt(64))
        val out = ArrayList<Byte>()
        repeat(1 + rnd.nextInt(6)) {
            out += rnd.nextInt(256).toByte()
            val masked = if (rnd.nextBoolean()) 0x80 else 0
            val len = when (rnd.nextInt(4)) { 0 -> rnd.nextInt(126); 1 -> rnd.nextInt(300); else -> rnd.nextInt(32) }
            when (rnd.nextInt(5)) {
                0 -> { out += (masked or 126).toByte(); out += (len ushr 8).toByte(); out += len.toByte() }
                1 -> { out += (masked or 127).toByte(); repeat(8) { k -> out += if (k < 6) rnd.nextInt(256).toByte() else 0 } }
                else -> out += (masked or minOf(len, 125)).toByte()
            }
            if (masked != 0) repeat(4) { out += rnd.nextInt(256).toByte() }
            repeat(rnd.nextInt(minOf(len, 125) + 1)) { out += rnd.nextInt(256).toByte() }
        }
        return out.toByteArray()
    }

    /** One to four edits: flip a bit, set a byte, truncate, duplicate a slice, insert random bytes. */
    private fun mutate(seed: ByteArray, rnd: Random): ByteArray {
        var b = seed.copyOf()
        repeat(1 + rnd.nextInt(4)) {
            if (b.isEmpty()) { b = rnd.nextBytes(1 + rnd.nextInt(16)); return@repeat }
            val at = rnd.nextInt(b.size)
            b = when (rnd.nextInt(5)) {
                0 -> b.also { it[at] = (it[at].toInt() xor (1 shl rnd.nextInt(8))).toByte() }
                1 -> b.also { it[at] = rnd.nextInt(256).toByte() }
                2 -> b.copyOf(at)
                3 -> { val len = rnd.nextInt(minOf(64, b.size - at) + 1); b.copyOf(at + len) + b.copyOfRange(at, b.size) }
                else -> b.copyOf(at) + rnd.nextBytes(1 + rnd.nextInt(16)) + b.copyOfRange(at, b.size)
            }
        }
        return b
    }

    @OptIn(ExperimentalForeignApi::class)
    private fun loadSeeds(target: String): List<ByteArray> {
        val root = getenv("NETON_WS_FUZZ_SEEDS")?.toKString()
            ?: getenv("HOME")?.toKString()?.let { "$it/projects/reference/rust/tungstenite-0.30.0/fuzz/seeds" }
            ?: return emptyList()
        val dir = "$root/$target"
        val d = opendir(dir) ?: return emptyList()
        val names = ArrayList<String>()
        try {
            while (true) {
                val e = readdir(d) ?: break
                val n = e.pointed.d_name.toKString()
                if (!n.startsWith(".")) names += n
            }
        } finally {
            closedir(d)
        }
        names.sort()
        return names.map { readFile("$dir/$it") }
    }

    @OptIn(ExperimentalForeignApi::class)
    private fun readFile(path: String): ByteArray {
        val f = fopen(path, "rb") ?: error("cannot open $path")
        try {
            fseek(f, 0.convert(), SEEK_END)
            val size: Int = ftell(f).convert()
            fseek(f, 0.convert(), SEEK_SET)
            val out = ByteArray(size)
            if (size > 0) out.usePinned { p -> check(fread(p.addressOf(0), 1.convert(), size.convert(), f).toInt() == size) { "short read of $path" } }
            return out
        } finally {
            fclose(f)
        }
    }
}
