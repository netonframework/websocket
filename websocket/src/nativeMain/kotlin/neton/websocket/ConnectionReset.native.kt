package neton.websocket

import neton.io.core.IoException
import neton.io.net.isConnectionReset as ioIsConnectionReset

internal actual fun isConnectionReset(e: IoException): Boolean = e.ioIsConnectionReset
