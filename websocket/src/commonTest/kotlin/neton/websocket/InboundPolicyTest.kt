package neton.websocket

import kotlin.test.*

class InboundPolicyTest {
    private fun frame(op: Int, bytes: ByteArray = byteArrayOf(), fin: Boolean = true): ByteArray {
        require(bytes.size < 126)
        val key = byteArrayOf(13, 29, 47, 61)
        return byteArrayOf((op or if (fin) 128 else 0).toByte(), (128 or bytes.size).toByte()) + key +
            ByteArray(bytes.size) { (bytes[it].toInt() xor key[it % 4].toInt()).toByte() }
    }
    private fun discard(max: Int = 4096) = WebSocketCore(Role.Server,
        WebSocketConfig(readBufferSize = 32, maxMessageSize = max)).apply {
        setInboundDataPolicy(InboundDataPolicy.DISCARD)
        setInboundAdmission { error("Discard must not reserve message storage") }
    }

    @Test fun rejectBeforePayloadAllocation() {
        val core = WebSocketCore(Role.Server, WebSocketConfig(readBufferSize = 32))
        core.setInboundDataPolicy(InboundDataPolicy.REJECT)
        core.input.writeBytes(byteArrayOf(0x82.toByte(), 0xfe.toByte(), 0x10, 0, 0, 0, 0, 0))
        val capacity = core.input.backingArray().size
        assertFailsWith<InboundDataRejectedException> { core.read() }
        assertEquals(capacity, core.input.backingArray().size)
        core.discardData()
        repeat(128) { core.input.writeBytes(ByteArray(32)); assertNull(core.read()) }
        core.input.writeBytes(frame(9))
        assertIs<Message.Ping>(core.read())
    }

    @Test fun validatesUtf8AcrossFragmentsAndPartialMaskedReads() {
        val core = discard()
        val data = frame(1, byteArrayOf(0xe2.toByte()), false) + frame(9) +
            frame(0, byteArrayOf(0x82.toByte(), 0xac.toByte())) + frame(9)
        var pings = 0
        for (byte in data) {
            core.input.writeBytes(byteArrayOf(byte))
            when (core.read()) { is Message.Ping -> pings++; null -> Unit; else -> fail("Unexpected data") }
        }
        assertEquals(2, pings)
        assertTrue(core.canWrite)
    }

    @Test fun invalidUtf8AndFragmentSequencesAreNotSilentlyDiscarded() {
        val invalid = discard()
        invalid.input.writeBytes(frame(1, byteArrayOf(0xc0.toByte())))
        assertFailsWith<WebSocketException.Utf8> { invalid.read() }
        val continuation = discard()
        continuation.input.writeBytes(frame(0))
        assertFailsWith<WebSocketException.Protocol> { continuation.read() }
        val unfinished = discard()
        unfinished.input.writeBytes(frame(1, byteArrayOf(0xe2.toByte())))
        assertFailsWith<WebSocketException.Utf8> { unfinished.read() }
        val unmasked = discard()
        unmasked.input.writeBytes(byteArrayOf(0x81.toByte(), 0))
        assertFailsWith<WebSocketException.Protocol> { unmasked.read() }
    }

    @Test fun fragmentedMessageLimitIsEnforcedWithoutReassembly() {
        val core = discard(3)
        core.input.writeBytes(frame(2, byteArrayOf(1, 2), false))
        assertNull(core.read())
        core.input.writeBytes(frame(0, byteArrayOf(3, 4)))
        assertFailsWith<WebSocketException.Capacity> { core.read() }
    }

    @Test fun bufferedDiscardHasFiniteWorkBudget() {
        val core = discard()
        repeat(100) { core.input.writeBytes(frame(2)) }
        core.input.writeBytes(frame(9))
        assertNull(core.read())
        assertTrue(core.needsReadYield)
        assertTrue(core.input.readableBytes > 0)
        var yields = 1
        while (core.read() == null) { assertTrue(core.needsReadYield); yields++ }
        assertEquals(3, yields)
    }
}
