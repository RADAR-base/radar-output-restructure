package org.radarbase.output.path

import org.radarbase.output.path.ValuePathFormatterPlugin.Companion.lookup

class KeyPathFormatterPlugin : PathFormatterPlugin() {
    override val allowedFormats: String = "key:my.key.index"

    override fun createLookupTable(
        parameterNames: Set<String>
    ): Map<String, (PathFormatParameters) -> String> = parameterNames
        .filter { it.startsWith("key:") }
        .associateWith { name ->
            val index = name.removePrefix("key:").split('.')
            return@associateWith { params ->
                RecordPathFactory.sanitizeId(params.key.lookup(index), "unknown-key")
            }
        }
}
