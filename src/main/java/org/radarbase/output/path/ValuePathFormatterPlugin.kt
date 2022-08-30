package org.radarbase.output.path

import org.apache.avro.AvroRuntimeException
import org.apache.avro.generic.GenericRecord
import org.radarbase.output.path.RecordPathFactory.Companion.sanitizeId
import org.slf4j.LoggerFactory

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
            index.fold<String, Any?>(this) { record, item ->
                record
                    ?.let { it as? GenericRecord }
                    ?.let {
                        try {
                            it.get(item)
                        } catch (ex: AvroRuntimeException) {
                            logger.warn("Unknown field {} in record using index {}", item, index)
                            null
                        }
                    }
            }

        private val logger = LoggerFactory.getLogger(ValuePathFormatterPlugin::class.java)
    }
}
