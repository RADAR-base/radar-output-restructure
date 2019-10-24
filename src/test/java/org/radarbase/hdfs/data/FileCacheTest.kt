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

import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.GenericRecordBuilder
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.radarbase.hdfs.Application
import org.radarbase.hdfs.accounting.Accountant
import org.radarbase.hdfs.accounting.TopicPartition
import org.radarbase.hdfs.config.HdfsSettings
import org.radarbase.hdfs.config.RestructureSettings
import org.radarbase.hdfs.config.RestructureSettings.Builder
import java.io.IOException
import java.io.InputStreamReader
import java.nio.file.Files
import java.nio.file.Path
import java.util.zip.GZIPInputStream

/**
 * Created by joris on 03/07/2017.
 */
class FileCacheTest {
    private lateinit var path: Path
    private lateinit var exampleRecord: Record
    private lateinit var tmpDir: Path
    private lateinit var factory: Application
    private lateinit var accountant: Accountant
    private lateinit var topicPartition: TopicPartition

    @BeforeEach
    @Throws(IOException::class)
    fun setUp(@TempDir path: Path, @TempDir tmpPath: Path) {
        this.path = path.resolve("f")
        this.tmpDir = tmpPath

        val schema = SchemaBuilder.record("simple").fields()
                .name("a").type("string").noDefault()
                .endRecord()
        this.exampleRecord = GenericRecordBuilder(schema).set("a", "something").build()

        setUp(settingsBuilder().build())

        this.topicPartition = TopicPartition("t", 0)
    }

    private fun settingsBuilder(): Builder {
        return Builder(path.parent.toString())
    }

    @Throws(IOException::class)
    private fun setUp(settings: RestructureSettings) {
        this.factory = Application.Builder(
                settings,
                HdfsSettings.Builder("test").build())
                .build()
        this.accountant = Accountant(factory, "t")
    }

    @Test
    @Throws(IOException::class)
    fun testGzip() {
        val builder = settingsBuilder()
        builder.compression = "gzip"
        setUp(builder.build())

        FileCache(factory, path, exampleRecord, tmpDir, accountant).use { cache ->
            cache.writeRecord(exampleRecord,
                    Accountant.Transaction(topicPartition, 0L))
        }

        println("Gzip: " + Files.size(path))

        val lines = Files.newInputStream(path).use {
            fin -> GZIPInputStream(fin).use {
            gzipIn -> InputStreamReader(gzipIn).readLines() } }

        assertEquals(listOf("a", "something"), lines)
    }

    @Test
    @Throws(IOException::class)
    fun testGzipAppend() {
        val builder = settingsBuilder()
        builder.compression = "gzip"
        setUp(builder.build())

        FileCache(factory, path, exampleRecord, tmpDir, accountant).use { cache ->
            cache.writeRecord(exampleRecord,
                    Accountant.Transaction(topicPartition, 0))
        }

        FileCache(factory, path, exampleRecord, tmpDir, accountant).use { cache ->
            cache.writeRecord(exampleRecord,
                    Accountant.Transaction(topicPartition, 0))
        }

        println("Gzip appended: " + Files.size(path))

        val lines = Files.newInputStream(path).use {
            fin -> GZIPInputStream(fin).use {
            gzipIn -> InputStreamReader(gzipIn).readLines() } }

        assertEquals(listOf("a", "something", "something"), lines)
    }

    @Test
    @Throws(IOException::class)
    fun testPlain() {
        FileCache(factory, path, exampleRecord, tmpDir, accountant).use { cache ->
            cache.writeRecord(exampleRecord,
                    Accountant.Transaction(topicPartition, 0))
        }

        println("Plain: " + Files.size(path))

        val lines = Files.newBufferedReader(path).readLines()
        assertEquals(listOf("a", "something"), lines)
    }

    @Test
    @Throws(IOException::class)
    fun testPlainAppend() {
        FileCache(factory, path, exampleRecord, tmpDir, accountant).use { cache ->
            cache.writeRecord(exampleRecord,
                    Accountant.Transaction(topicPartition, 0))
        }

        FileCache(factory, path, exampleRecord, tmpDir, accountant).use { cache ->
            cache.writeRecord(exampleRecord,
                    Accountant.Transaction(topicPartition, 1))
        }

        println("Plain appended: " + Files.size(path))

        val lines = Files.newBufferedReader(path).readLines()
        assertEquals(listOf("a", "something", "something"), lines)
    }

    @Test
    @Throws(IOException::class)
    fun compareTo(@TempDir tmp3: Path) {
        val file3 = tmp3.resolve("file3")
        FileCache(factory, path, exampleRecord, tmpDir, accountant).use { cache1 ->
            FileCache(factory, path, exampleRecord, tmpDir, accountant).use { cache2 ->
                FileCache(factory, file3, exampleRecord, tmpDir, accountant).use { cache3 ->
                    assertEquals(0, cache1.compareTo(cache2))
                    // filenames are not equal
                    assertTrue(cache1 < cache3)
                    cache1.writeRecord(exampleRecord, Accountant.Transaction(topicPartition, 0))
                    // last used
                    assertTrue(cache1 > cache2)
                    // last used takes precedence over filename
                    assertTrue(cache1 > cache3)

                    // last used reversal
                    cache2.writeRecord(exampleRecord, Accountant.Transaction(topicPartition, 1))
                    cache3.writeRecord(exampleRecord, Accountant.Transaction(topicPartition, 2))
                    assertTrue(cache1 < cache2)
                    assertTrue(cache1 < cache3)
                }
            }
        }
    }
}
