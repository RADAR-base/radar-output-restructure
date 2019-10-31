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

import org.apache.avro.Schema.Parser
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.avro.io.DecoderFactory
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.*
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import java.nio.file.Path
import java.util.*
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream

class CsvAvroConverterTest {
    @Test
    @Throws(IOException::class)
    fun writeRecord() {
        val parser = Parser()
        val schema = parser.parse(javaClass.getResourceAsStream("full.avsc"))
        val reader = GenericDatumReader<GenericRecord>(schema)
        val decoder = DecoderFactory.get().jsonDecoder(schema, javaClass.getResourceAsStream("full.json"))
        val record = reader.read(null, decoder)

        val writer = StringWriter()
        val factory = CsvAvroConverter.factory
        val converter = factory.converterFor(writer, record, true, StringReader("test"))

        val map = converter.convertRecord(record)
        val keys = listOf("a", "b", "c", "d", "e", "f", "g", "h", "i.some",
                "i.other", "j.0", "j.1", "k", "l.la", "m")
        val expectedKeys = LinkedHashSet(keys)
        assertEquals(expectedKeys, map.keys)

        val actualIterator = map.values.iterator()
        val expectedIterator = listOf<Any>(
                "a", byteArrayOf(255.toByte()), byteArrayOf(255.toByte()), "1000000000000000000",
                "1.21322421E-15", "0.1213231", "132101", "", "1", "-1", "", "some", "Y", "", "false").iterator()

        var i = 0
        while (actualIterator.hasNext()) {
            assertTrue(expectedIterator.hasNext(), "Actual value has more entries than expected value")
            val actual = actualIterator.next()
            val expected = expectedIterator.next()

            if (expected is ByteArray) {
                assertEquals(Base64.getEncoder().withoutPadding().encodeToString(expected), actual, "Array for argument " + keys[i] + " does not match")
            } else {
                assertEquals(expected, actual, "Value for argument " + keys[i] + " does not match")
            }
            i++
        }
        assertFalse(expectedIterator.hasNext(), "Actual value has fewer entries than expected value")

        converter.writeRecord(record)

        val writtenValue = writer.toString()
        val lines = writtenValue.split("\n".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
        assertEquals(2, lines.size)
        assertEquals(keys.joinToString(","), lines[0])
        println(lines[1])
    }

    @Test
    @Throws(IOException::class)
    fun differentSchema() {
        val schemaA = SchemaBuilder.record("A").fields().name("a").type("string").noDefault().endRecord()
        val recordA = GenericRecordBuilder(schemaA).set("a", "something").build()

        val writer = StringWriter()
        val converter = CsvAvroConverter
                .factory.converterFor(writer, recordA, true, StringReader("test"))
        converter.writeRecord(recordA)

        val schemaB = SchemaBuilder.record("B").fields().name("b").type("string").noDefault().endRecord()
        val recordB = GenericRecordBuilder(schemaB).set("b", "something").build()

        /* Same number of columns but different schema, so CsvAvroConverter.write() will return false
        signifying that a new CSV file must be used to write this record
         */
        assertFalse(converter.writeRecord(recordB))
        println(writer.toString())
    }


    @Test
    @Throws(IOException::class)
    fun differentSchema2() {
        val schemaA = SchemaBuilder.record("A").fields().name("a").type("string").noDefault().name("b").type("string").noDefault().endRecord()
        val recordA = GenericRecordBuilder(schemaA).set("a", "something").set("b", "2nd something").build()

        val writer = StringWriter()
        val converter = CsvAvroConverter
                .factory.converterFor(writer, recordA, true, StringReader("test"))
        converter.writeRecord(recordA)

        val schemaB = SchemaBuilder.record("B").fields().name("b").type("string").noDefault().name("a").type("string").noDefault().endRecord()
        val recordB = GenericRecordBuilder(schemaB).set("b", "something").set("a", "2nd something").build()

        /* Same number of columns and same header but different order,
        so CsvAvroConverter.write() will return false signifying that
        a new CSV file must be used to write this record
         */
        assertFalse(converter.writeRecord(recordB))
        println(writer.toString())
    }

    @Test
    @Throws(IOException::class)
    fun subSchema() {
        val schemaA = SchemaBuilder.record("A").fields().name("a").type("string").noDefault().name("b").type("string").withDefault("def").endRecord()
        val recordA = GenericRecordBuilder(schemaA)
                .set("a", "something")
                .set("b", "somethingElse")
                .build()

        val writer = StringWriter()
        val converter = CsvAvroConverter
                .factory.converterFor(writer, recordA, true, StringReader("test"))
        converter.writeRecord(recordA)

        val schemaB = SchemaBuilder.record("B").fields().name("b").type("string").noDefault().endRecord()
        val recordB = GenericData.Record(schemaB)
        recordB.put("b", "something")

        converter.writeRecord(recordB)

        val recordC = GenericRecordBuilder(schemaA).set("a", "that").build()
        recordC.put("a", "that")
        converter.writeRecord(recordC)

        println(writer.toString())
    }

    @Test
    @Throws(IOException::class)
    fun deduplicate(@TempDir dir: Path) {
        val path = dir.resolve("test")
        val toPath = dir.resolve("test.dedup")
        Files.newBufferedWriter(path).use { writer -> writeTestNumbers(writer) }
        CsvAvroConverter.factory.deduplicate("t", path, toPath, IdentityCompression())
        assertEquals(listOf("a,b", "1,2", "3,4", "1,3", "a,a"), Files.readAllLines(toPath))
    }


    @Test
    @Throws(IOException::class)
    fun deduplicateGzip(@TempDir dir: Path) {
        val path = dir.resolve("test.csv.gz")
        val toPath = dir.resolve("test.csv.gz.dedup")

        Files.newOutputStream(path).use { out -> GZIPOutputStream(out).use { gzipOut -> OutputStreamWriter(gzipOut).use { writer -> writeTestNumbers(writer) } } }
        CsvAvroConverter.factory.deduplicate("t", path, toPath, GzipCompression())
        val storedLines = Files.newInputStream(toPath).use {
            `in` -> GZIPInputStream(`in`).use {
            gzipIn -> InputStreamReader(gzipIn).readLines() } }

        assertEquals(listOf("a,b", "1,2", "3,4", "1,3", "a,a"), storedLines)
    }

    companion object {

        @Throws(IOException::class)
        internal fun writeTestNumbers(writer: Writer) {
            writer.write("a,b\n")
            writer.write("1,2\n")
            writer.write("3,4\n")
            writer.write("1,3\n")
            writer.write("3,4\n")
            writer.write("1,2\n")
            writer.write("a,a\n")
        }
    }
}