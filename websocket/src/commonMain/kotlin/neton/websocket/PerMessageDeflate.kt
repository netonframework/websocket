package neton.websocket

import neton.http.Request
import neton.http.Response
import neton.http.header.HeaderValue

/**
 * ⚖️ permessage-deflate (RFC 7692; tungstenite 0.30 has none, SPEC §11.7): what a side offers or accepts. Off unless
 * set as [WebSocketConfig.compression].
 *
 * The parameters keep the RFC's names: `server_*` limits the server's compressor, `client_*` the client's.
 * - A client offers them: [serverNoContextTakeover] / [serverMaxWindowBits] ask the server to limit itself,
 *   [clientNoContextTakeover] / [clientMaxWindowBits] announce what the client will do (it always says it accepts a
 *   `client_max_window_bits` from the server).
 * - A server requires them: it accepts an offer and adds its own `server_no_context_takeover` /
 *   `server_max_window_bits` / `client_no_context_takeover`; it asks the client for [clientMaxWindowBits] only when
 *   the offer allows it (`client_max_window_bits`), and otherwise declines the offer.
 *
 * Window bits are 9 to 15: zlib cannot compress raw DEFLATE with an 8-bit window, so an offer that would make this
 * side compress with 8 is declined (RFC 7692 §7.1.2.2 lets a server decline), and a server answer that makes the
 * client compress with 8 fails the handshake. Receiving with 8 works.
 *
 * Each connection with compression keeps a compressor (about 256 KiB at 15 bits) and a decompressor (about 44 KiB),
 * allocated on first use; [contextTakeover] off on both sides lets them restart per message.
 *
 * @property level zlib compression level, 0 to 9.
 */
data class PerMessageDeflateConfig(
    val serverNoContextTakeover: Boolean = false,
    val clientNoContextTakeover: Boolean = false,
    val serverMaxWindowBits: Int? = null,
    val clientMaxWindowBits: Int? = null,
    val level: Int = 6,
) {
    init {
        require(serverMaxWindowBits == null || serverMaxWindowBits in 9..15) { "serverMaxWindowBits must be 9 to 15" }
        require(clientMaxWindowBits == null || clientMaxWindowBits in 9..15) { "clientMaxWindowBits must be 9 to 15" }
        require(level in 0..9) { "level must be 0 to 9" }
    }

    /** The `Sec-WebSocket-Extensions` value a client offers. */
    fun offer(): String = buildString {
        append(EXTENSION)
        if (serverNoContextTakeover) append("; server_no_context_takeover")
        if (clientNoContextTakeover) append("; client_no_context_takeover")
        serverMaxWindowBits?.let { append("; server_max_window_bits=").append(it) }
        append("; client_max_window_bits")
        clientMaxWindowBits?.let { append('=').append(it) }
    }

    companion object {
        const val EXTENSION = "permessage-deflate"
    }
}

/**
 * The parameters in effect on a connection (both sides read the server's response the same way).
 * @property compressNoContextTakeover this side restarts its compressor per message.
 * @property decompressNoContextTakeover the peer restarts its compressor per message, so this side may restart its
 *   decompressor.
 */
class PerMessageDeflate internal constructor(
    val compressNoContextTakeover: Boolean,
    val decompressNoContextTakeover: Boolean,
    val compressWindowBits: Int,
    val decompressWindowBits: Int,
    val level: Int,
) {
    override fun toString(): String =
        "PerMessageDeflate(compress: window $compressWindowBits, no context takeover $compressNoContextTakeover; " +
            "decompress: window $decompressWindowBits, no context takeover $decompressNoContextTakeover)"

    companion object {
        /**
         * What [response] (a server's handshake response) negotiated for the side [role], or null when it has no
         * permessage-deflate; a client checks it against its offer [config]. For the HTTP upgrade path
         * ([WebSocket.fromRawStream] / [WebSocket.fromUpgraded]).
         * @throws WebSocketException.Protocol [ProtocolError.InvalidExtensionParameters] for parameters this side
         *   cannot use.
         */
        fun fromResponse(response: Response<*>, role: Role, config: PerMessageDeflateConfig = PerMessageDeflateConfig()): PerMessageDeflate? {
            val params = (if (role == Role.Client) acceptedByServer(response, config) else responseParams(response)) ?: return null
            return fromParams(params, role, config)
        }

        internal fun fromParams(p: Params, role: Role, config: PerMessageDeflateConfig): PerMessageDeflate {
            val serverBits = p.serverMaxWindowBits ?: 15
            val clientBits = p.clientMaxWindowBits ?: 15
            val compressBits = if (role == Role.Server) serverBits else clientBits
            if (compressBits == 8) invalid("an 8-bit window to compress with (zlib cannot)")
            return PerMessageDeflate(
                compressNoContextTakeover = if (role == Role.Server) p.serverNoContextTakeover else p.clientNoContextTakeover,
                decompressNoContextTakeover = if (role == Role.Server) p.clientNoContextTakeover else p.serverNoContextTakeover,
                compressWindowBits = compressBits,
                decompressWindowBits = if (role == Role.Server) clientBits else serverBits,
                level = config.level,
            )
        }
    }
}

/**
 * Server side (RFC 7692 §5, §7.1): accept the first acceptable permessage-deflate offer of [request] under [config]
 * and add the answer to [response]'s `Sec-WebSocket-Extensions`. Returns false, leaving [response] as it is, when the
 * request offers nothing acceptable. [PerMessageDeflate.fromResponse] then reads what was agreed.
 */
fun negotiatePerMessageDeflate(request: Request<*>, response: Response<*>, config: PerMessageDeflateConfig): Boolean {
    for (value in request.headers.getAll("Sec-WebSocket-Extensions")) {
        val text = value.tryToStr() ?: continue
        for (offer in splitExtensions(text)) {
            if (offer.name != PerMessageDeflateConfig.EXTENSION) continue
            val p = parseParams(offer.params) ?: continue                  // malformed offer: try the next
            val answer = answerOffer(p, config) ?: continue
            response.headers.append("Sec-WebSocket-Extensions", HeaderValue.fromStatic(answer))
            return true
        }
    }
    return false
}

/** The parameters of one permessage-deflate element. */
internal class Params(
    val serverNoContextTakeover: Boolean,
    val clientNoContextTakeover: Boolean,
    val serverMaxWindowBits: Int?,
    /** -1: offered without a value. */
    val clientMaxWindowBits: Int?,
)

/** The server's answer to offer [p], or null to decline it. */
private fun answerOffer(p: Params, config: PerMessageDeflateConfig): String? {
    // The server compresses with at most what the client asked and what it wants itself (default 15).
    val serverBits = listOfNotNull(p.serverMaxWindowBits, config.serverMaxWindowBits).minOrNull()
    if (serverBits == 8) return null                                      // zlib cannot compress with 8
    // The client's window: limited only if the offer allows it (client_max_window_bits present).
    val clientBits = when {
        config.clientMaxWindowBits == null -> if (p.clientMaxWindowBits != null && p.clientMaxWindowBits > 0) p.clientMaxWindowBits else null
        p.clientMaxWindowBits == null -> return null                       // the server needs a limit the client did not allow
        p.clientMaxWindowBits < 0 -> config.clientMaxWindowBits
        else -> minOf(p.clientMaxWindowBits, config.clientMaxWindowBits)
    }
    return buildString {
        append(PerMessageDeflateConfig.EXTENSION)
        if (p.serverNoContextTakeover || config.serverNoContextTakeover) append("; server_no_context_takeover")
        if (p.clientNoContextTakeover || config.clientNoContextTakeover) append("; client_no_context_takeover")
        if (serverBits != null) append("; server_max_window_bits=").append(serverBits)
        if (clientBits != null) append("; client_max_window_bits=").append(clientBits)
    }
}

/**
 * Client side: the permessage-deflate parameters of the server's response, checked against the client's [offer]
 * (RFC 7692 §5.1, §7.1.1.2, §7.1.2.1–2). Null when the response has no permessage-deflate.
 */
internal fun acceptedByServer(response: Response<*>, offer: PerMessageDeflateConfig): Params? {
    val p = responseParams(response) ?: return null
    if (offer.serverNoContextTakeover && !p.serverNoContextTakeover) invalid("server_no_context_takeover was requested")
    val asked = offer.serverMaxWindowBits
    if (asked != null && (p.serverMaxWindowBits ?: 15) > asked) invalid("server_max_window_bits above the requested $asked")
    val clientBits = p.clientMaxWindowBits
    if (clientBits != null && clientBits < 0) invalid("client_max_window_bits without a value")
    if (clientBits != null && offer.clientMaxWindowBits != null && clientBits > offer.clientMaxWindowBits) {
        invalid("client_max_window_bits above the offered ${offer.clientMaxWindowBits}")
    }
    return p
}

/** The single permessage-deflate element of a response, parsed strictly; null when there is none. */
private fun responseParams(response: Response<*>): Params? {
    var found: Params? = null
    for (value in response.headers.getAll("Sec-WebSocket-Extensions")) {
        val text = value.tryToStr() ?: invalid("non-ASCII Sec-WebSocket-Extensions")
        for (e in splitExtensions(text)) {
            if (e.name != PerMessageDeflateConfig.EXTENSION) continue
            if (found != null) invalid("permessage-deflate answered twice")
            found = parseParams(e.params) ?: invalid("malformed permessage-deflate parameters: $text")
        }
    }
    return found
}

private class Element(val name: String, val params: List<Pair<String, String?>>)

/** `name; key[=value]; ...` elements of an extension list (RFC 6455 §9.1), names and keys lowercased. */
private fun splitExtensions(value: String): List<Element> = value.split(',').mapNotNull { element ->
    val parts = element.split(';').map { it.trim() }
    val name = parts[0].lowercase()
    if (name.isEmpty()) return@mapNotNull null
    val params = parts.drop(1).filter { it.isNotEmpty() }.map { p ->
        val eq = p.indexOf('=')
        if (eq < 0) p.lowercase() to null
        else p.substring(0, eq).trim().lowercase() to p.substring(eq + 1).trim().removeSurrounding("\"")
    }
    Element(name, params)
}

/** Strict parameters (RFC 7692 §7.1): known names, each at most once, valid values; null otherwise. */
private fun parseParams(params: List<Pair<String, String?>>): Params? {
    var snct = false; var cnct = false
    var sbits: Int? = null; var cbits: Int? = null
    val seen = HashSet<String>()
    for ((k, v) in params) {
        if (!seen.add(k)) return null
        when (k) {
            "server_no_context_takeover" -> { if (v != null) return null; snct = true }
            "client_no_context_takeover" -> { if (v != null) return null; cnct = true }
            "server_max_window_bits" -> sbits = windowBits(v ?: return null) ?: return null
            "client_max_window_bits" -> cbits = if (v == null) -1 else windowBits(v) ?: return null
            else -> return null
        }
    }
    return Params(snct, cnct, sbits, cbits)
}

/** 8 to 15 written without leading zeros (RFC 7692 §7.1.2.1). */
private fun windowBits(v: String): Int? {
    if (v.isEmpty() || v.length > 2 || v[0] == '0' || !v.all { it in '0'..'9' }) return null
    return v.toInt().takeIf { it in 8..15 }
}

private fun invalid(detail: String): Nothing = throw WebSocketException.Protocol(ProtocolError.InvalidExtensionParameters(detail))
