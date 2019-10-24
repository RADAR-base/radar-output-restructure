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

import java.io.BufferedReader
import java.io.IOException
import java.io.Reader
import java.io.Writer
import java.nio.ByteBuffer
import java.util.ArrayList
import java.util.Base64
import java.util.Collections
import java.util.LinkedHashMap
import java.util.regex.Pattern
import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericFixed
import org.apache.avro.generic.GenericRecord

/**
 * Converts deep hierarchical Avro records into flat CSV format. It uses a simple dot syntax in the
 * column names to indicate hierarchy. After the first data record is added, all following
 * records need to have exactly the same hierarchy (or at least a subset of it.)
 */
class CsvAvroConverter @Throws(IOException::class)
constructor(private val writer: Writer, record: GenericRecord, writeHeader: Boolean,
            reader: Reader) : RecordConverter {
    private var headers: List<String>
    private val values: MutableList<String>

    init {
        headers = if (writeHeader) {
                    createHeaders(record)
                            .also { writeLine(it) }
                } else {
                    // If file already exists read the schema from the CSV file
                    BufferedReader(reader, 200).use {
                        bufReader -> parseCsvLine(bufReader) }
                }

        values = ArrayList(headers.size)
    }

    /**
     * Write AVRO record to CSV file.
     * @param record the AVRO record to be written to CSV file
     * @return true if write was successful, false if cannot write record to the current CSV file
     * @throws IOException for other IO and Mapping errors
     */
    @Throws(IOException::class)
    override fun writeRecord(record: GenericRecord): Boolean {
        try {
            val retValues = convertRecordValues(record)
            if (retValues.size < headers.size) {
                return false
            }

            writeLine(retValues)
            return true
        } catch (ex: IllegalArgumentException) {
            return false
        } catch (ex: IndexOutOfBoundsException) {
            return false
        }

    }

    @Throws(IOException::class)
    private fun writeLine(objects: List<*>) {
        for (i in objects.indices) {
            if (i > 0) {
                writer.append(',')
            }
            writer.append(objects[i].toString())
        }
        writer.append('\n')
    }


    override fun convertRecord(record: GenericRecord): Map<String, Any?> {
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
        return values
    }

    private fun convertAvro(values: MutableList<String>, data: Any?, schema: Schema, prefix: String) {
        when (schema.type) {
            Schema.Type.RECORD -> {
                val record = data as GenericRecord
                val subSchema = record.schema
                for (field in subSchema.fields) {
                    val subData = record.get(field.pos())
                    convertAvro(values, subData, field.schema(), prefix + '.'.toString() + field.name())
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
            Schema.Type.STRING -> {
                checkHeader(prefix, values.size)
                values.add(cleanCsvString(data.toString()))
            }
            Schema.Type.ENUM, Schema.Type.INT, Schema.Type.LONG, Schema.Type.DOUBLE, Schema.Type.FLOAT, Schema.Type.BOOLEAN -> {
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
        require(prefix == headers[size]) {
            ("Header " + prefix + " does not match "
                    + headers[size])
        }
    }

    @Throws(IOException::class)
    override fun close() {
        writer.close()
    }

    @Throws(IOException::class)
    override fun flush() {
        writer.flush()
    }

    companion object {
        private val ESCAPE_PATTERN = Pattern.compile("\\p{C}|[,\"]")
        private val TAB_PATTERN = Pattern.compile("\t")
        private val LINE_ENDING_PATTERN = Pattern.compile("[\n\r]+")
        private val NON_PRINTING_PATTERN = Pattern.compile("\\p{C}")
        private val QUOTE_OR_COMMA_PATTERN = Pattern.compile("[\",]")
        private val QUOTE_PATTERN = Pattern.compile("\"")
        private val BASE64_ENCODER = Base64.getEncoder().withoutPadding()

        val factory: RecordConverterFactory
            get() = object : RecordConverterFactory {

                override val extension: String
                    get() = ".csv"

                override val formats: Collection<String>
                    get() = setOf("csv")

                @Throws(IOException::class)
                override fun converterFor(writer: Writer, record: GenericRecord,
                                          writeHeader: Boolean, reader: Reader): CsvAvroConverter {
                    return CsvAvroConverter(writer, record,
                            writeHeader, reader)
                }

                override fun hasHeader(): Boolean {
                    return true
                }
            }

        @Throws(IOException::class)
        fun parseCsvLine(reader: Reader): List<String> {
            val headers = ArrayList<String>()
            var builder = StringBuilder(20)
            var ch = reader.read()
            var inString = false
            var hadQuote = false

            while (ch != '\n'.toInt() && ch != '\r'.toInt() && ch != -1) {
                when (ch) {
                    '"'.toInt() -> when {
                        inString -> {
                            inString = false
                            hadQuote = true
                        }
                        hadQuote -> {
                            inString = true
                            hadQuote = false
                            builder.append('"')
                        }
                        else -> inString = true
                    }
                    ','.toInt() -> when {
                        inString -> builder.append(',')
                        else -> {
                            if (hadQuote) {
                                hadQuote = false
                            }
                            headers.add(builder.toString())
                            builder = StringBuilder(20)
                        }
                    }
                    else -> builder.append(ch.toChar())
                }
                ch = reader.read()
            }
            headers.add(builder.toString())
            return headers
        }

        internal fun createHeaders(record: GenericRecord): List<String> {
            val headers = ArrayList<String>()
            val schema = record.schema
            for (field in schema.fields) {
                createHeader(headers, record.get(field.pos()), field.schema(), field.name())
            }
            return headers
        }

        fun cleanCsvString(orig: String): String {
            return if (ESCAPE_PATTERN.matcher(orig).find()) {
                var cleaned = LINE_ENDING_PATTERN.matcher(orig).replaceAll("\\\\n")
                cleaned = TAB_PATTERN.matcher(cleaned).replaceAll("    ")
                cleaned = NON_PRINTING_PATTERN.matcher(cleaned).replaceAll("?")
                if (QUOTE_OR_COMMA_PATTERN.matcher(cleaned).find()) {
                    '"'.toString() + QUOTE_PATTERN.matcher(cleaned).replaceAll("\"\"") + '"'.toString()
                } else {
                    cleaned
                }
            } else {
                orig
            }
        }

        private fun createHeader(headers: MutableList<String>, data: Any?, schema: Schema, prefix: String) {
            when (schema.type) {
                Schema.Type.RECORD -> {
                    val record = data as GenericRecord
                    val subSchema = record.schema
                    for (field in subSchema.fields) {
                        val subData = record.get(field.pos())
                        createHeader(headers, subData, field.schema(), prefix + '.'.toString() + field.name())
                    }
                }
                Schema.Type.MAP -> {
                    val valueType = schema.valueType
                    for ((key, value) in data as Map<*, *>) {
                        val name = "$prefix.$key"
                        createHeader(headers, value, valueType, name)
                    }
                }
                Schema.Type.ARRAY -> {
                    val itemType = schema.elementType
                    for ((i, orig) in (data as List<*>).withIndex()) {
                        createHeader(headers, orig, itemType, "$prefix.$i")
                    }
                }
                Schema.Type.UNION -> {
                    val type = GenericData().resolveUnion(schema, data)
                    createHeader(headers, data, schema.types[type], prefix)
                }
                Schema.Type.BYTES, Schema.Type.FIXED, Schema.Type.ENUM, Schema.Type.STRING, Schema.Type.INT, Schema.Type.LONG, Schema.Type.DOUBLE, Schema.Type.FLOAT, Schema.Type.BOOLEAN, Schema.Type.NULL -> headers.add(prefix)
                else -> throw IllegalArgumentException("Cannot parse field type " + schema.type)
            }
        }
    }
}
