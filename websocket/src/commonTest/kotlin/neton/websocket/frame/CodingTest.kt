package neton.websocket.frame

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** Ported from `T/src/protocol/frame/coding.rs` tests (4). */
class CodingTest {
    @Test fun opcodeFromU8() {
        assertEquals(OpCode.Data.Binary, OpCode.from(2))
    }

    @Test fun opcodeIntoU8() {
        assertEquals(1, OpCode.Data.Text.code)
    }

    @Test fun closecodeFromU16() {
        assertEquals(CloseCode.Policy, CloseCode.from(1008))
    }

    @Test fun closecodeIntoU16() {
        assertEquals(1001, CloseCode.Away.code)
    }

    // ---- beyond the reference's tests: every range and `is_allowed` (SPEC §2) ----

    @Test fun opcodeRoundTripAndDisplay() {
        for (i in 0..15) assertEquals(i, OpCode.from(i).code)
        assertEquals("CONTINUE", OpCode.from(0).toString())
        assertEquals("RESERVED_DATA_3", OpCode.from(3).toString())
        assertEquals("CLOSE", OpCode.from(8).toString())
        assertEquals("RESERVED_CONTROL_15", OpCode.from(15).toString())
        assertTrue(OpCode.from(7) is OpCode.Data.Reserved)
        assertTrue(OpCode.from(11) is OpCode.Control.Reserved)
    }

    @Test fun closecodeRangesAndIsAllowed() {
        for (c in 0..5100) assertEquals(c, CloseCode.from(c).code)
        assertTrue(CloseCode.from(999) is CloseCode.Bad)
        assertTrue(CloseCode.from(1004) is CloseCode.Bad)
        assertTrue(CloseCode.from(1014) is CloseCode.Bad)
        assertTrue(CloseCode.from(1016) is CloseCode.Reserved)
        assertTrue(CloseCode.from(2999) is CloseCode.Reserved)
        assertTrue(CloseCode.from(3000) is CloseCode.Iana)
        assertTrue(CloseCode.from(4999) is CloseCode.Library)
        assertTrue(CloseCode.from(5000) is CloseCode.Bad)
        for (c in listOf(1005, 1006, 1015, 1004, 1014, 1016, 2999, 0, 999, 5000)) assertFalse(CloseCode.from(c).isAllowed, "$c")
        for (c in listOf(1000, 1001, 1002, 1003, 1007, 1008, 1009, 1010, 1011, 1012, 1013, 3000, 4999)) assertTrue(CloseCode.from(c).isAllowed, "$c")
        assertEquals("1000", CloseCode.Normal.toString())
    }
}
