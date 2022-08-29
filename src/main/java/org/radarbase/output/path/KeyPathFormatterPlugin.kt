package org.radarbase.output.path

import org.radarbase.output.path.RecordPathFactory.Companion.sanitizeId
import org.radarbase.output.path.ValuePathFormatterPlugin.Companion.lookup

class KeyPathFormatterPlugin : PathFormatterPlugin() {
    override val prefix: String = "key"

    override val allowedFormats: String = "key:my.key.index"

    override fun lookup(parameterContents: String): PathFormatParameters.() -> String {
        val index = parameterContents.split('.')
        require(index.none { it.isBlank() }) { "Cannot format key record with index $parameterContents" }
        return {
            sanitizeId(key.lookup(index), "unknown-key")
        }
    }
}
