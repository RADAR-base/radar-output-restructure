package org.radarbase.output.format

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericFixed
import org.apache.avro.generic.GenericRecord
import java.nio.ByteBuffer
import java.util.*

internal class CsvAvroDataConverter(
    private val headers: Array<String>,
    private val excludeFields: Set<String>,
) {
    fun convertRecord(record: GenericRecord): Map<String, Any?> = buildMap<String, Any> {
        convertRecordValues(record).forEachIndexed { i, value ->
            put(headers[i], value)
        }
    }

    fun convertRecordValues(record: GenericRecord): Array<String> {
        val values = arrayOfNulls<String>(headers.size)
        val schema = record.schema
        val endIndex = schema.fields.fold(0) { valueIndex, field ->
            convertAvro(values, valueIndex, record.get(field.pos()), field.schema(), field.name())
        }
        require(endIndex >= headers.size) { "Values and headers do not match" }
        @Suppress("UNCHECKED_CAST")
        return values as Array<String>
    }

    private fun convertAvro(
        values: Array<String?>,
        startIndex: Int,
        data: Any?,
        schema: Schema,
        prefix: String,
    ): Int = when (schema.type) {
        Schema.Type.RECORD -> {
            val record = data as GenericRecord
            val subSchema = record.schema
            subSchema.fields.fold(startIndex) { index, field ->
                val subData = record.get(field.pos())
                convertAvro(
                    values,
                    index,
                    subData,
                    field.schema(),
                    "$prefix.${field.name()}",
                )
            }
        }
        Schema.Type.MAP -> {
            val valueType = schema.valueType
            (data as Map<*, *>).entries.fold(startIndex) { index, (key, value) ->
                convertAvro(values, index, value, valueType, "$prefix.$key")
            }
        }
        Schema.Type.ARRAY -> {
            val itemType = schema.elementType
            (data as List<*>).foldIndexed(startIndex) { i, index, orig ->
                convertAvro(values, index, orig, itemType, "$prefix.$i")
            }
        }
        Schema.Type.UNION -> {
            val type = GenericData().resolveUnion(schema, data)
            convertAvro(values, startIndex, data, schema.types[type], prefix)
        }
        Schema.Type.BYTES -> {
            addValue(prefix, values, startIndex, BASE64_ENCODER.encodeToString((data as ByteBuffer).array()))
        }
        Schema.Type.FIXED -> {
            addValue(prefix, values, startIndex, BASE64_ENCODER.encodeToString((data as GenericFixed).bytes()))
        }
        Schema.Type.STRING, Schema.Type.ENUM, Schema.Type.INT, Schema.Type.LONG,
        Schema.Type.DOUBLE, Schema.Type.FLOAT, Schema.Type.BOOLEAN -> {
            addValue(prefix, values, startIndex, data.toString())
        }
        Schema.Type.NULL -> {
            addValue(prefix, values, startIndex, "")
        }
        else -> throw IllegalArgumentException("Cannot parse field type " + schema.type)
    }

    private fun addValue(prefix: String, values: Array<String?>, index: Int, value: String): Int {
        if (prefix in excludeFields) return index
        val header = headers[index]
        require(prefix == header) { "Header $prefix does not match $header" }
        values[index] = value
        return index + 1
    }

    companion object {
        private val BASE64_ENCODER = Base64.getEncoder().withoutPadding()
    }
}
