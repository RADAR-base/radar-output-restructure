package org.radarbase.output.path

import org.apache.avro.generic.GenericRecord
import org.radarbase.output.path.RecordPathFactory.Companion.sanitizeId

class ValuePathFormatterPlugin : PathFormatterPlugin() {
    override val allowedFormats: String = "value:my.value.index"

    override fun createLookupTable(
        parameterNames: Set<String>
    ): Map<String, (PathFormatParameters) -> String> = parameterNames
        .filter { it.startsWith("value:") }
        .associateWith { name ->
            val index = name.removePrefix("value:").split('.')
            return@associateWith { params ->
                sanitizeId(params.value.lookup(index), "unknown-value")
            }
        }

    companion object {
        fun GenericRecord.lookup(index: List<String>): Any? =
            index.fold<String, Any?>(this) { r, item ->
                r?.let { (it as? GenericRecord)?.get(item) }
            }
    }
}
