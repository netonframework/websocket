@file:OptIn(kotlin.experimental.ExperimentalNativeApi::class)

package neton.websocket.frame

import kotlin.native.getLongAt
import kotlin.native.setLongAt

@Suppress("NOTHING_TO_INLINE")
internal actual inline fun ByteArray.loadLongLe(index: Int): Long = getLongAt(index)

@Suppress("NOTHING_TO_INLINE")
internal actual inline fun ByteArray.storeLongLe(index: Int, value: Long) = setLongAt(index, value)

internal actual val wordsAreLittleEndian: Boolean = Platform.isLittleEndian
