package neton.websocket

/** Set before the first receive. Non-delivery policies currently require no compression. */
enum class InboundDataPolicy { DELIVER, REJECT, DISCARD }

/** The driver remains usable for the closing handshake after switching to closing discard. */
class InboundDataRejectedException : IllegalStateException("Inbound data is forbidden by policy")
