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

package org.radarbase.output.data

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import kotlinx.coroutines.test.runTest
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DecoderFactory
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.radarbase.output.compression.IdentityCompression
import org.radarbase.output.data.CsvAvroConverterTest.Companion.readAllLines
import org.radarbase.output.data.CsvAvroConverterTest.Companion.writeTestNumbers
import org.radarbase.output.format.JsonAvroConverter
import java.io.IOException
import java.io.InputStream
import java.io.StringReader
import java.io.StringWriter
import java.nio.file.Path
import kotlin.io.path.bufferedWriter

class JsonAvroConverterTest {
    @Test
    @Throws(IOException::class)
    fun fullAvroTest() {
        val parser = Parser()
        val schema = parser.parse(javaClass.resourceStream("full.avsc"))
        val reader = GenericDatumReader<GenericRecord>(schema)
        val decoder = DecoderFactory.get().jsonDecoder(schema, javaClass.resourceStream("full.json"))
        val record = reader.read(null, decoder)

        val map = JsonAvroConverter
            .factory.converterFor(StringWriter(), record, false, StringReader("test"))
            .convertRecord(record)
        val writer = ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).writer()
        val result = writer.writeValueAsString(map)

        val expected = javaClass.resourceStream("full.json")
            .reader()
            .useLines { it.joinToString("\n") }

        println(result)

        val expectedLines = expected.split("\n").dropLastWhile { it.isEmpty() }
        val resultLines = result.split("\n").dropLastWhile { it.isEmpty() }
        assertEquals(expectedLines.size, resultLines.size)

        val ignoreLines = listOf(2, 3, 13)
        expectedLines.indices
            .filterNot { ignoreLines.contains(it) }
            .forEach { assertEquals(expectedLines[it], resultLines[it]) }
    }

    @Test
    @Throws(IOException::class)
    fun deduplicate(@TempDir folder: Path) = runTest {
        val path = folder.resolve("test.txt")
        path.bufferedWriter().use { writer -> writeTestNumbers(writer) }
        JsonAvroConverter.factory.deduplicate("t", path, path, IdentityCompression())
        assertEquals(listOf("a,b", "1,2", "3,4", "1,3", "a,a", "3,3"), path.readAllLines())
    }

    companion object {
        fun Class<*>.resourceStream(name: String): InputStream = requireNotNull(getResourceAsStream(name)) { "Missing $name" }
    }
}
