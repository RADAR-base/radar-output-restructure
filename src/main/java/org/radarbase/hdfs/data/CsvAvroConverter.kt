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

import com.opencsv.CSVReader
import com.opencsv.CSVWriter
import java.nio.ByteBuffer
import java.util.ArrayList
import java.util.Base64
import java.util.LinkedHashMap
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericFixed
import org.apache.avro.generic.GenericRecord
import java.io.*
import java.nio.file.Files
import java.nio.file.Path

/**
 * Converts deep hierarchical Avro records into flat CSV format. It uses a simple dot syntax in the
 * column names to indicate hierarchy. After the first data record is added, all following
 * records need to have exactly the same hierarchy (or at least a subset of it.)
 */
class CsvAvroConverter(
        private val writer: Writer,
        record: GenericRecord,
        writeHeader: Boolean,
        reader: Reader
) : RecordConverter {

    private val csvWriter = CSVWriter(writer)
    private var headers: List<String>
    private val values: MutableList<String>

    init {
        headers = if (writeHeader) {
            createHeaders(record)
                    .also { csvWriter.writeNext(it.toTypedArray(), false) }
        } else {
            CSVReader(reader).readNext().toList()
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

            csvWriter.writeNext(retValues.toTypedArray(), false)
            values.clear()
            return true
        } catch (ex: IllegalArgumentException) {
            return false
        } catch (ex: IndexOutOfBoundsException) {
            return false
        }
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

    private fun convertRecordValues(record: GenericRecord): List<String> {
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
                values.add(data.toString())
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
        require(prefix == headers[size]) { "Header $prefix does not match ${headers[size]}" }
    }

    @Throws(IOException::class)
    override fun close() = writer.close()

    @Throws(IOException::class)
    override fun flush() = writer.flush()

    companion object {
        private val BASE64_ENCODER = Base64.getEncoder().withoutPadding()

        val factory = object : RecordConverterFactory {
            override val extension: String = ".csv"

            override val formats: Collection<String> = setOf("csv")

            @Throws(IOException::class)
            override fun deduplicate(fileName: String, source: Path, target: Path,
                            compression: Compression, usingFields: List<String>) {
                val withHeader = hasHeader

                Files.newInputStream(source).use {
                    inFile -> compression.decompress(inFile).use {
                    zipIn -> InputStreamReader(zipIn).use {
                    inReader -> BufferedReader(inReader).use {
                    reader -> CSVReader(reader).use {
                    csvReader -> Files.newOutputStream(target).use {
                    fileOut -> BufferedOutputStream(fileOut).use {
                    bufOut -> compression.compress(fileName, bufOut).use {
                    zipOut -> OutputStreamWriter(zipOut).use {
                    writer -> CSVWriter(writer).use { csvWriter ->
                        val lines = readDeduplicatedSequence(csvReader, withHeader, usingFields)
                        csvWriter.writeAll(lines.asIterable(), false)
                    } } } } } } } } } }
            }

            @Throws(IOException::class)
            override fun converterFor(writer: Writer, record: GenericRecord,
                                      writeHeader: Boolean, reader: Reader): CsvAvroConverter =
                    CsvAvroConverter(writer, record,
                            writeHeader, reader)

            override val hasHeader: Boolean = true
        }

        fun readDeduplicatedSequence(csvReader: CSVReader, withHeader: Boolean, usingFields: List<String>): Sequence<Array<String>> {
            val header = if (withHeader) {
                csvReader.readNext() ?: return emptySequence()
            } else null

            val fieldIndexes = usingFields
                    .map { f -> header?.indexOf(f) }
                    .takeIf { indexes -> indexes.none { it == null } }
                    ?.filterNotNull()
                    ?: emptyList()

            val readLines = generateSequence { csvReader.readNext() }

            val lines = if (fieldIndexes.isEmpty()) {
                readLines.distinctBy { it.joinToString() }
            } else {
                readLines.distinctBy { line -> fieldIndexes.joinToString { line[it] } }
            }
            return header?.let { sequenceOf(header) + lines }
                    ?: lines
        }

        internal fun createHeaders(record: GenericRecord): List<String> {
            val headers = ArrayList<String>()
            val schema = record.schema
            for (field in schema.fields) {
                createHeader(headers, record.get(field.pos()), field.schema(), field.name())
            }
            return headers
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
