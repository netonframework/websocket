package neton.websocket

import neton.io.core.IoException
import platform.posix.ECONNRESET

/** Winsock's "connection reset by peer" (neton-io reports Winsock codes on Windows). */
private const val WSAECONNRESET = 10054

internal actual fun isConnectionReset(e: IoException): Boolean = e.errno == ECONNRESET || e.errno == WSAECONNRESET
