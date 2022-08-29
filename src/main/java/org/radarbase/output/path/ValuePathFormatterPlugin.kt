package org.radarbase.output.path

import org.apache.avro.generic.GenericRecord
import org.radarbase.output.path.RecordPathFactory.Companion.sanitizeId

class ValuePathFormatterPlugin : PathFormatterPlugin() {
    override val prefix: String = "value"

    override val allowedFormats: String = "value:my.value.index"

    override fun lookup(parameterContents: String): PathFormatParameters.() -> String {
        val index = parameterContents.split('.')
        require(index.none { it.isBlank() }) { "Cannot format value record with index $parameterContents" }
        return {
            sanitizeId(value.lookup(index), "unknown-value")
        }
    }

    companion object {
        fun GenericRecord.lookup(index: List<String>): Any? =
            index.fold<String, Any?>(this) { r, item ->
                r?.let { (it as? GenericRecord)?.get(item) }
            }
    }
}
