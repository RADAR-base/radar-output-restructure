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

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.util.MinimalPrettyPrinter
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.avro.generic.GenericRecord
import java.io.IOException
import java.io.Writer

/**
 * Writes an Avro record to JSON format.
 */
class JsonAvroConverter(
    writer: Writer,
    excludeFields: Set<String>,
) : RecordConverter {
    private val converter = JsonAvroDataConverter(excludeFields)
    private val generator: JsonGenerator = JSON_FACTORY.createGenerator(writer)
        .setPrettyPrinter(MinimalPrettyPrinter("\n"))

    @Throws(IOException::class)
    override fun writeRecord(record: GenericRecord): Boolean {
        JSON_WRITER.writeValue(generator, converter.convertRecord(record))
        return true
    }

    override fun convertRecord(record: GenericRecord): Map<String, Any?> =
        converter.convertRecord(record)

    @Throws(IOException::class)
    override fun flush() = generator.flush()

    @Throws(IOException::class)
    override fun close() = generator.close()

    companion object {
        private val JSON_FACTORY = JsonFactory()
        internal val JSON_WRITER = ObjectMapper(JSON_FACTORY).writer()
        internal val JSON_READER = ObjectMapper(JSON_FACTORY).reader()

        val factory: RecordConverterFactory = JsonAvroConverterFactory()
    }
}
