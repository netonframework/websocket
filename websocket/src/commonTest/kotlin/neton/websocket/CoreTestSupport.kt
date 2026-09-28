package neton.websocket

import neton.io.bytes.Buffer
import neton.io.bytes.Bytes
import neton.websocket.frame.Frame
import neton.websocket.frame.FrameHeader
import neton.websocket.frame.FrameSocket
import neton.websocket.frame.OpCode

/** Encode one frame; [first] is FIN | RSV | opcode. A [mask] makes it a client frame. */
fun frameBytes(first: Int, payload: ByteArray = ByteArray(0), mask: ByteArray? = null): ByteArray {
    val h = FrameHeader(
        isFinal = first and 0x80 != 0, rsv1 = first and 0x40 != 0, rsv2 = first and 0x20 != 0, rsv3 = first and 0x10 != 0,
        opcode = OpCode.from(first and 0x0F), mask = mask,
    )
    val b = Buffer()
    Frame(h, Bytes.copyOf(payload)).format(b)
    return b.readAll()
}

val MASK = bytesOf(0x37, 0xfa, 0x21, 0x3d)

/** A frame as a client sends it (masked). */
fun fromClient(first: Int, payload: ByteArray = ByteArray(0)) = frameBytes(first, payload, MASK)

/** A frame as a server sends it (unmasked). */
fun fromServer(first: Int, payload: ByteArray = ByteArray(0)) = frameBytes(first, payload)

/** Decode (and unmask) every frame the core queued in its output, consuming it. */
fun WebSocketCore.drainFrames(): List<Frame> {
    val sock = FrameSocket()
    sock.input.writeBytes(output.readAll())
    val frames = mutableListOf<Frame>()
    while (true) frames += sock.read() ?: break
    return frames
}

fun closePayload(code: Int, reason: String = ""): ByteArray =
    bytesOf(code ushr 8, code and 0xFF) + reason.encodeToByteArray()
