/*
 * Copyright 2017 The Hyve
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.radarbase.hdfs.data

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.util.MinimalPrettyPrinter
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericFixed
import org.apache.avro.generic.GenericRecord
import java.io.IOException
import java.io.Reader
import java.io.Writer
import java.nio.ByteBuffer
import java.util.*

/**
 * Writes an Avro record to JSON format.
 */
class JsonAvroConverter @Throws(IOException::class)
constructor(writer: Writer) : RecordConverter {

    private val generator: JsonGenerator = JSON_FACTORY.createGenerator(writer)
            .setPrettyPrinter(MinimalPrettyPrinter("\n"))

    @Throws(IOException::class)
    override fun writeRecord(record: GenericRecord): Boolean {
        JSON_WRITER.writeValue(generator, convertRecord(record))
        return true
    }

    override fun convertRecord(record: GenericRecord): Map<String, Any?> {
        val map = HashMap<String, Any?>()
        val schema = record.schema
        for (field in schema.fields) {
            map[field.name()] = convertAvro(record.get(field.pos()), field.schema())
        }
        return map
    }

    private fun convertAvro(data: Any?, schema: Schema): Any? {
        when (schema.type) {
            Schema.Type.RECORD -> return convertRecord(data as GenericRecord)
            Schema.Type.MAP -> {
                val value = HashMap<String, Any?>()
                val valueType = schema.valueType
                for ((key, value1) in data as Map<*, *>) {
                    value[key.toString()] = convertAvro(value1, valueType)
                }
                return value
            }
            Schema.Type.ARRAY -> {
                val origList = data as List<*>
                val itemType = schema.elementType
                val list = ArrayList<Any?>(origList.size)
                for (orig in origList) {
                    list.add(convertAvro(orig, itemType))
                }
                return list
            }
            Schema.Type.UNION -> {
                val type = GenericData().resolveUnion(schema, data)
                return convertAvro(data, schema.types[type])
            }
            Schema.Type.BYTES -> return (data as ByteBuffer).array()
            Schema.Type.FIXED -> return (data as GenericFixed).bytes()
            Schema.Type.ENUM, Schema.Type.STRING -> return data.toString()
            Schema.Type.INT, Schema.Type.LONG, Schema.Type.DOUBLE, Schema.Type.FLOAT, Schema.Type.BOOLEAN, Schema.Type.NULL -> return data
            else -> throw IllegalArgumentException("Cannot parse field type " + schema.type)
        }
    }

    @Throws(IOException::class)
    override fun flush() = generator.flush()

    @Throws(IOException::class)
    override fun close() = generator.close()

    companion object {
        private val JSON_FACTORY = JsonFactory()
        private val JSON_WRITER = ObjectMapper(JSON_FACTORY).writer()

        val factory: RecordConverterFactory = object : RecordConverterFactory {
            override val extension: String = ".json"

            override val formats: Collection<String> = setOf("json")

            @Throws(IOException::class)
            override fun converterFor(writer: Writer,
                                      record: GenericRecord, writeHeader: Boolean,
                                      reader: Reader): RecordConverter = JsonAvroConverter(writer)
        }
    }
}
