package neton.websocket.frame

import java.io.File
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * OpCode's first use in a fresh JVM, whichever class is touched first. Class initialization runs once per JVM, so the
 * rest of the suite (which has long initialized OpCode) cannot see an initialization-order bug: the table once read a
 * subclass object as null when a subclass was initialized before OpCode's companion. Each case runs in its own process.
 */
class OpCodeInitTest {
    @Test fun fromFirst() = assertFreshJvm("from")
    @Test fun dataObjectFirst() = assertFreshJvm("data")
    @Test fun controlObjectFirst() = assertFreshJvm("control")
    @Test fun reservedFirst() = assertFreshJvm("reserved")

    private fun assertFreshJvm(first: String) {
        val java = File(System.getProperty("java.home"), "bin/java").path
        val process = ProcessBuilder(java, "-cp", System.getProperty("java.class.path"), OpCodeFirstTouch::class.java.name, first)
            .redirectErrorStream(true).start()
        val output = process.inputStream.bufferedReader().readText()
        check(process.waitFor(60, TimeUnit.SECONDS)) { "child JVM did not finish" }
        assertEquals(0, process.exitValue(), output)
        assertEquals("ok", output.trim(), output)
    }
}

/** The child process: touches [args] `[0]` first, then checks every opcode; prints "ok" or what is wrong. */
object OpCodeFirstTouch {
    @JvmStatic
    fun main(args: Array<String>) {
        val touched: Any = when (args[0]) {
            "from" -> OpCode.from(0)
            "data" -> OpCode.Data.Continue
            "control" -> OpCode.Control.Close
            else -> OpCode.from(3)
        }
        val problems = mutableListOf<String>()
        if (touched.toString().isEmpty()) problems += "empty name"
        for (code in 0..15) {
            val op: OpCode? = OpCode.from(code)
            if (op == null) problems += "from($code) is null" else if (op.code != code) problems += "from($code).code = ${op.code}"
        }
        if (OpCode.from(0) !== OpCode.Data.Continue) problems += "from(0) is not Data.Continue"
        if (OpCode.from(8) !== OpCode.Control.Close) problems += "from(8) is not Control.Close"
        println(if (problems.isEmpty()) "ok" else problems.joinToString("; "))
    }
}
