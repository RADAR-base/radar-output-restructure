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

package org.radarbase.output.format

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.radarbase.output.compression.Compression
import org.radarbase.output.util.ResourceContext.Companion.resourceContext
import java.io.*
import java.nio.file.Path
import java.util.regex.Pattern
import kotlin.collections.component1
import kotlin.collections.component2
import kotlin.io.path.inputStream
import kotlin.io.path.outputStream

interface RecordConverterFactory : Format {
    /**
     * Create a converter to write records of given type to given writer. A header is needed only
     * in certain converters. The given record is not converted yet, it is only used as an example.
     * @param writer to write data to
     * @param record to generate the headers and schemas from.
     * @param writeHeader whether to write a header, if applicable
     * @return RecordConverter that is ready to be used
     * @throws IOException if the converter could not be created
     */
    @Throws(IOException::class)
    fun converterFor(
        writer: Writer,
        record: GenericRecord,
        writeHeader: Boolean,
        reader: Reader,
        excludeFields: Set<String> = emptySet(),
    ): RecordConverter

    val hasHeader: Boolean
        get() = false

    /**
     * Deduplicate [source] with [fileName] and store the result in [target]. Returns `false` if no
     * deduplication was performed because it was not necessary. In that case, [target] is not
     * created.
     */
    @Throws(IOException::class)
    suspend fun deduplicate(
        fileName: String,
        source: Path,
        target: Path,
        compression: Compression,
        distinctFields: Set<String> = emptySet(),
        ignoreFields: Set<String> = emptySet(),
    ): Boolean {
        val withHeader = hasHeader

        val (header, lines) = resourceContext {
            val reader = resourceChain { source.inputStream() }
                .chain { compression.decompress(it) }
                .conclude { it.bufferedReader() }

            readFile(reader, withHeader)
        }

        resourceContext {
            val writer = resourceChain { target.outputStream() }
                .chain { it.buffered() }
                .chain { compression.compress(fileName, it) }
                .conclude { it.writer() }

            writeFile(writer, header, lines)
        }

        return true
    }

    suspend fun readTimeSeconds(
        source: InputStream,
        compression: Compression,
    ): Pair<Array<String>?, List<Double>>?

    suspend fun contains(
        source: Path,
        record: GenericRecord,
        compression: Compression,
        usingFields: Set<String>,
        ignoreFields: Set<String>,
    ): Boolean

    override fun matchesFilename(name: String): Boolean {
        return name.matches((".*" + Pattern.quote(extension) + "(\\.[^.]+)?").toRegex())
    }

    fun headerFor(record: GenericRecord): Array<String> {
        val headers = ArrayList<String>()
        val schema = record.schema
        for (field in schema.fields) {
            createHeader(headers, record.get(field.pos()), field.schema(), field.name())
        }
        return headers.toTypedArray()
    }

    private fun createHeader(
        headers: MutableList<String>,
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
                    createHeader(
                        headers,
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
            Schema.Type.BYTES, Schema.Type.FIXED, Schema.Type.ENUM, Schema.Type.STRING,
            Schema.Type.INT, Schema.Type.LONG, Schema.Type.DOUBLE, Schema.Type.FLOAT,
            Schema.Type.BOOLEAN, Schema.Type.NULL ->
                headers.add(prefix)
            else -> throw IllegalArgumentException("Cannot parse field type " + schema.type)
        }
    }

    companion object {
        /**
         * @param reader file to read from
         * @return optional header with full contents
         */
        @Throws(IOException::class)
        fun readFile(reader: BufferedReader, withHeader: Boolean): Pair<String?, Set<String>> {
            val header = if (withHeader) {
                reader.readLine() ?: return Pair(null, emptySet())
            } else null

            return Pair(header, reader.lineSequence().toCollection(LinkedHashSet()))
        }

        @Throws(IOException::class)
        fun writeFile(writer: Writer, header: String?, lines: Collection<String>) {
            if (header != null) {
                writer.write(header)
                writer.write('\n'.code)
            }

            for (line in lines) {
                writer.write(line)
                writer.write('\n'.code)
            }
        }
    }
}
