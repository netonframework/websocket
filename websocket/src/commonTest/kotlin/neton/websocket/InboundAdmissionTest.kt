package neton.websocket

import kotlin.test.*

class InboundAdmissionTest {
    @Test fun refusalPrecedesPayloadBufferGrowth() {
        val core = WebSocketCore(Role.Server, WebSocketConfig(readBufferSize = 32))
        var requested = 0
        core.setInboundAdmission { requested = it; throw IllegalStateException("full") }
        // Masked binary header declaring 4096 bytes, no payload sent.
        core.input.writeBytes(byteArrayOf(0x82.toByte(), 0xfe.toByte(), 0x10, 0, 0, 0, 0, 0))
        val capacity = core.input.backingArray().size
        assertFailsWith<IllegalStateException> { core.read() }
        assertEquals(4096, requested)
        assertEquals(capacity, core.input.backingArray().size)
    }

    @Test fun discardDrainsLargeDataInChunksAndStillHandlesPing() {
        val core = WebSocketCore(Role.Server, WebSocketConfig(readBufferSize = 32))
        core.setInboundAdmission { error("No data reservation while closing") }
        core.discardData()
        core.input.writeBytes(byteArrayOf(0x82.toByte(), 0xfe.toByte(), 0x10, 0, 0, 0, 0, 0))
        assertNull(core.read())
        val capacity = core.input.backingArray().size
        repeat(128) {
            core.input.writeBytes(ByteArray(32))
            assertNull(core.read())
        }
        assertEquals(capacity, core.input.backingArray().size)
        core.input.writeBytes(byteArrayOf(0x89.toByte(), 0x81.toByte(), 0, 0, 0, 0, 7))
        assertContentEquals(byteArrayOf(7), assertIs<Message.Ping>(core.read()).data.toByteArray())
    }

    @Test fun everyFragmentAdmittedButControlFramesAreIndependent() {
        val core = WebSocketCore(Role.Server)
        val sizes = mutableListOf<Int>()
        core.setInboundAdmission { sizes += it }
        core.input.writeBytes(byteArrayOf(0x02, 0x81.toByte(), 0, 0, 0, 0, 1))
        assertNull(core.read())
        core.input.writeBytes(byteArrayOf(0x89.toByte(), 0x80.toByte(), 0, 0, 0, 0))
        assertIs<Message.Ping>(core.read())
        core.input.writeBytes(byteArrayOf(0x80.toByte(), 0x81.toByte(), 0, 0, 0, 0, 2))
        assertContentEquals(byteArrayOf(1, 2), assertIs<Message.Binary>(core.read()).data.toByteArray())
        assertEquals(listOf(1, 1), sizes)
    }
}
