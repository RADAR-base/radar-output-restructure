package org.radarbase.output.format

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericFixed
import org.apache.avro.generic.GenericRecord
import java.nio.ByteBuffer
import java.util.*

internal class CsvAvroDataConverter(
    private val headers: Array<String>,
) {
    private val values: MutableList<String> = ArrayList(this.headers.size)

    fun convertRecord(record: GenericRecord): Map<String, Any?> {
        values.clear()
        val schema = record.schema
        for (field in schema.fields) {
            convertAvro(values, record.get(field.pos()), field.schema(), field.name())
        }
        val map = LinkedHashMap<String, Any>()
        for (i in headers.indices) {
            map[headers[i]] = values[i]
        }
        values.clear()
        return map
    }

    fun convertRecordValues(record: GenericRecord): List<String> {
        values.clear()
        val schema = record.schema
        for (field in schema.fields) {
            convertAvro(values, record.get(field.pos()), field.schema(), field.name())
        }
        require(values.size >= headers.size) { "Values and headers do not match" }
        return values
    }

    private fun convertAvro(
        values: MutableList<String>,
        data: Any?,
        schema: Schema,
        prefix: String,
    ) {
        when (schema.type) {
            Schema.Type.RECORD -> {
                val record = data as GenericRecord
                val subSchema = record.schema
                for (field in subSchema.fields) {
                    val subData = record.get(field.pos())
                    convertAvro(
                        values,
                        subData,
                        field.schema(),
                        prefix + '.'.toString() + field.name(),
                    )
                }
            }
            Schema.Type.MAP -> {
                val valueType = schema.valueType
                for ((key, value) in data as Map<*, *>) {
                    val name = "$prefix.$key"
                    convertAvro(values, value, valueType, name)
                }
            }
            Schema.Type.ARRAY -> {
                val itemType = schema.elementType
                for ((i, orig) in (data as List<*>).withIndex()) {
                    convertAvro(values, orig, itemType, "$prefix.$i")
                }
            }
            Schema.Type.UNION -> {
                val type = GenericData().resolveUnion(schema, data)
                convertAvro(values, data, schema.types[type], prefix)
            }
            Schema.Type.BYTES -> {
                checkHeader(prefix, values.size)
                values.add(BASE64_ENCODER.encodeToString((data as ByteBuffer).array()))
            }
            Schema.Type.FIXED -> {
                checkHeader(prefix, values.size)
                values.add(BASE64_ENCODER.encodeToString((data as GenericFixed).bytes()))
            }
            Schema.Type.STRING, Schema.Type.ENUM, Schema.Type.INT, Schema.Type.LONG,
            Schema.Type.DOUBLE, Schema.Type.FLOAT, Schema.Type.BOOLEAN -> {
                checkHeader(prefix, values.size)
                values.add(data.toString())
            }
            Schema.Type.NULL -> {
                checkHeader(prefix, values.size)
                values.add("")
            }
            else -> throw IllegalArgumentException("Cannot parse field type " + schema.type)
        }
    }

    private fun checkHeader(prefix: String, size: Int) {
        require(prefix == headers[size]) { "Header $prefix does not match ${headers[size]}" }
    }

    companion object {
        private val BASE64_ENCODER = Base64.getEncoder().withoutPadding()
    }
}
