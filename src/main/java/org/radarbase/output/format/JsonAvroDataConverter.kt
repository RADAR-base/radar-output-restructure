package org.radarbase.output.format

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericFixed
import org.apache.avro.generic.GenericRecord
import java.nio.ByteBuffer
import java.util.EnumSet

class JsonAvroDataConverter(
    private val excludeFields: Set<String>
) {
    fun convertRecord(record: GenericRecord, prefix: String? = null): Map<String, Any?> {
        val schema = record.schema
        return buildMap {
            for (field in schema.fields) {
                val fieldPrefix = if (prefix == null) field.name() else "$prefix.${field.name()}"
                convertAvro(record.get(field.pos()), field.schema(), fieldPrefix)
                    .ifNotExcluded { put(field.name(), it) }
            }
        }
    }

    private fun convertAvro(data: Any?, schema: Schema, prefix: String): Any? {
        if (schema.type !in compositeTypes && prefix in excludeFields) return EXCLUDE_FIELD
        return when (schema.type) {
            Schema.Type.RECORD -> convertRecord(data as GenericRecord)
            Schema.Type.MAP -> {
                val valueType = schema.valueType
                buildMap {
                    for ((key, value1) in data as Map<*, *>) {
                        convertAvro(value1, valueType, "$prefix.$key")
                            .ifNotExcluded { put(key.toString(), it) }
                    }
                }
            }
            Schema.Type.ARRAY -> {
                val itemType = schema.elementType
                buildList {
                    (data as List<*>).forEachIndexed { i, orig ->
                        convertAvro(orig, itemType, "$prefix.$i")
                            .ifNotExcluded { add(it) }
                    }
                }
            }
            Schema.Type.UNION -> {
                val typeIndex = GenericData().resolveUnion(schema, data)
                convertAvro(data, schema.types[typeIndex], prefix)
            }
            Schema.Type.BYTES -> (data as ByteBuffer).array()
            Schema.Type.FIXED -> (data as GenericFixed).bytes()
            Schema.Type.ENUM, Schema.Type.STRING -> data.toString()
            Schema.Type.INT, Schema.Type.LONG, Schema.Type.DOUBLE, Schema.Type.FLOAT, Schema.Type.BOOLEAN, Schema.Type.NULL -> data
            else -> throw IllegalArgumentException("Cannot parse field type " + schema.type)
        }
    }

    companion object {
        private val compositeTypes = EnumSet.of(Schema.Type.RECORD, Schema.Type.MAP, Schema.Type.ARRAY, Schema.Type.UNION)
        private val EXCLUDE_FIELD = Any()

        private fun Any?.ifNotExcluded(apply: (Any?) -> Unit) {
            if (this !== EXCLUDE_FIELD) apply(this)
        }
    }
}
