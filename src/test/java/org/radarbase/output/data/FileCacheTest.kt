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

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.GenericRecordBuilder
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.mockito.kotlin.mock
import org.radarbase.output.Application
import org.radarbase.output.accounting.Accountant
import org.radarbase.output.accounting.TopicPartition
import org.radarbase.output.config.LocalConfig
import org.radarbase.output.config.PathConfig
import org.radarbase.output.config.ResourceConfig
import org.radarbase.output.config.RestructureConfig
import org.radarbase.output.config.S3Config
import org.radarbase.output.util.ResourceContext.Companion.resourceContext
import org.radarbase.output.util.SuspendedCloseable.Companion.useSuspended
import org.radarbase.output.worker.FileCache
import java.io.IOException
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Instant
import java.util.zip.GZIPInputStream
import kotlin.io.path.bufferedReader
import kotlin.io.path.fileSize
import kotlin.io.path.inputStream

/**
 * Created by joris on 03/07/2017.
 */
class FileCacheTest {
    private lateinit var localPath: Path
    private lateinit var path: Path
    private lateinit var exampleRecord: Record
    private lateinit var tmpDir: Path
    private lateinit var factory: Application
    private lateinit var accountant: Accountant
    private lateinit var topicPartition: TopicPartition

    private val lastModified = Instant.now()

    private lateinit var config: RestructureConfig

    @BeforeEach
    @Throws(IOException::class)
    fun setUp(@TempDir path: Path, @TempDir tmpPath: Path) {
        this.path = Paths.get("radar-output-storage/f")
        this.localPath = path.resolve("f")
        this.tmpDir = tmpPath

        val schema = SchemaBuilder.record("simple").fields()
            .name("a").type("string").noDefault()
            .endRecord()
        this.exampleRecord = GenericRecordBuilder(schema).set("a", "something").build()

        config = RestructureConfig(
            paths = PathConfig(
                temp = tmpPath,
            ),
            source = ResourceConfig("s3", path = Paths.get("in"), s3 = S3Config("http://ep", "null", "null", "test")),
            targets = mapOf("radar-output-storage" to ResourceConfig("local", path = path, local = LocalConfig())),
        )

        setUp(config)

        this.topicPartition = TopicPartition("t", 0)
    }

    @Throws(IOException::class)
    private fun setUp(localConfig: RestructureConfig) {
        this.factory = Application(localConfig)
        this.accountant = mock()
    }

    @Test
    @Throws(IOException::class)
    fun testGzip() = runTest {
        setUp(config.copy(compression = config.compression.copy(type = "gzip")))

        FileCache(factory, "topic", path, tmpDir, accountant).useSuspended { cache ->
            cache.initialize(exampleRecord)
            cache.writeRecord(
                record = exampleRecord,
                transaction = Accountant.Transaction(topicPartition, 0L, lastModified),
            )
        }

        println("Gzip: " + localPath.fileSize())

        val lines = resourceContext {
            resourceChain { localPath.inputStream() }
                .chain { GZIPInputStream(it) }
                .chain { it.reader() }
                .result
                .readLines()
        }

        assertEquals(listOf("a", "something"), lines)
    }

    @Test
    @Throws(IOException::class)
    fun testGzipAppend() = runTest {
        setUp(config.copy(compression = config.compression.copy(type = "gzip")))

        FileCache(factory, "topic", path, tmpDir, accountant).useSuspended { cache ->
            cache.initialize(exampleRecord)
            cache.writeRecord(
                record = exampleRecord,
                transaction = Accountant.Transaction(topicPartition, 0, lastModified),
            )
        }

        FileCache(factory, "topic", path, tmpDir, accountant).useSuspended { cache ->
            cache.initialize(exampleRecord)
            cache.writeRecord(
                record = exampleRecord,
                transaction = Accountant.Transaction(topicPartition, 0, lastModified),
            )
        }

        println("Gzip appended: " + localPath.fileSize())
        val lines = resourceContext {
            resourceChain { localPath.inputStream() }
                .chain { GZIPInputStream(it) }
                .chain { it.reader() }
                .result
                .readLines()
        }
        assertEquals(listOf("a", "something", "something"), lines)
    }

    @Test
    @Throws(IOException::class)
    fun testPlain() = runTest {
        FileCache(factory, "topic", path, tmpDir, accountant).useSuspended { cache ->
            cache.initialize(exampleRecord)
            cache.writeRecord(
                record = exampleRecord,
                transaction = Accountant.Transaction(topicPartition, 0, lastModified),
            )
        }

        println("Plain: " + localPath.fileSize())

        val lines = localPath.bufferedReader().use { it.readLines() }
        assertEquals(listOf("a", "something"), lines)
    }

    @Test
    @Throws(IOException::class)
    fun testPlainAppend() = runTest {
        FileCache(factory, "topic", path, tmpDir, accountant).useSuspended { cache ->
            cache.initialize(exampleRecord)
            cache.writeRecord(
                record = exampleRecord,
                transaction = Accountant.Transaction(topicPartition, 0, lastModified),
            )
        }

        FileCache(factory, "topic", path, tmpDir, accountant).useSuspended { cache ->
            cache.initialize(exampleRecord)
            cache.writeRecord(
                record = exampleRecord,
                transaction = Accountant.Transaction(topicPartition, 1, lastModified),
            )
        }

        println("Plain appended: " + localPath.fileSize())

        val lines = localPath.bufferedReader().use { it.readLines() }
        assertEquals(listOf("a", "something", "something"), lines)
    }

    @Test
    @Throws(IOException::class)
    fun compareTo() = runTest {
        val file3 = path.parent.resolve("g")

        resourceContext {
            val cache1 = createResource { FileCache(factory, "topic", path, tmpDir, accountant) }
            val cache2 = createResource { FileCache(factory, "topic", path, tmpDir, accountant) }
            val cache3 = createResource { FileCache(factory, "topic", file3, tmpDir, accountant) }

            listOf(cache1, cache2, cache3)
                .map { cache -> launch(Dispatchers.IO) { cache.initialize(exampleRecord) } }
                .joinAll()

            val transaction = Accountant.Transaction(topicPartition, 0, lastModified)
            assertEquals(0, cache1.compareTo(cache2))
            // filenames are not equal
            assertTrue(cache1 < cache3)
            cache1.writeRecord(exampleRecord, transaction)
            // last used
            assertTrue(cache1 > cache2)
            // last used takes precedence over filename
            assertTrue(cache1 > cache3)

            // last used reversal
            cache2.writeRecord(exampleRecord, transaction.copy(offset = 1))
            cache3.writeRecord(exampleRecord, transaction.copy(offset = 2))
            assertTrue(cache1 < cache2)
            assertTrue(cache1 < cache3)
        }
    }
}
