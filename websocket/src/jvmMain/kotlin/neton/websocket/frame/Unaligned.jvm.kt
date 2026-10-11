package neton.websocket.frame

// Little-endian word access built from bytes (bytecode 1.8 has no VarHandle; HotSpot's C2 merges such byte accesses).

internal actual fun ByteArray.loadLongLe(index: Int): Long =
    (this[index].toLong() and 0xff) or
        ((this[index + 1].toLong() and 0xff) shl 8) or
        ((this[index + 2].toLong() and 0xff) shl 16) or
        ((this[index + 3].toLong() and 0xff) shl 24) or
        ((this[index + 4].toLong() and 0xff) shl 32) or
        ((this[index + 5].toLong() and 0xff) shl 40) or
        ((this[index + 6].toLong() and 0xff) shl 48) or
        ((this[index + 7].toLong() and 0xff) shl 56)

internal actual fun ByteArray.storeLongLe(index: Int, value: Long) {
    this[index] = value.toByte()
    this[index + 1] = (value ushr 8).toByte()
    this[index + 2] = (value ushr 16).toByte()
    this[index + 3] = (value ushr 24).toByte()
    this[index + 4] = (value ushr 32).toByte()
    this[index + 5] = (value ushr 40).toByte()
    this[index + 6] = (value ushr 48).toByte()
    this[index + 7] = (value ushr 56).toByte()
}

internal actual val wordsAreLittleEndian: Boolean = true
