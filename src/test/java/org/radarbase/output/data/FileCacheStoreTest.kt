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
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericRecordBuilder
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.mockito.kotlin.check
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.radarbase.output.Application
import org.radarbase.output.accounting.Accountant
import org.radarbase.output.accounting.OffsetRangeSet
import org.radarbase.output.accounting.TopicPartition
import org.radarbase.output.accounting.TopicPartitionOffsetRange
import org.radarbase.output.config.*
import org.radarbase.output.util.SuspendedCloseable.Companion.useSuspended
import org.radarbase.output.worker.FileCacheStore
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.time.Instant
import kotlin.io.path.createDirectories

class FileCacheStoreTest {
    private val lastModified = Instant.now()

    @Test
    @Throws(IOException::class)
    fun appendLine(@TempDir root: Path, @TempDir tmpDir: Path) = runTest {
        val f1 = root.resolve("f1")
        val f2 = root.resolve("f2")
        val f3 = root.resolve("f3")
        val d4 = root.resolve("d4")
        d4.createDirectories()
        val f4 = d4.resolve("f4.txt")
        val newFile = root.resolve("newFile")

        val simpleSchema = SchemaBuilder.record("simple").fields()
            .name("a").type("string").noDefault()
            .endRecord()

        val conflictSchema = SchemaBuilder.record("simple").fields()
            .name("a").type("string").noDefault()
            .name("b").type("string").noDefault()
            .endRecord()

        var record: GenericRecord

        val topicPartition0 = TopicPartition("t", 0)
        val topicPartition1 = TopicPartition("t", 1)

        val offsetRange0 =
            TopicPartitionOffsetRange(topicPartition0, OffsetRangeSet.Range(0, 0, lastModified))
        val offsetRange1 =
            TopicPartitionOffsetRange(topicPartition1, OffsetRangeSet.Range(0, 8, lastModified))

        val factory = Application(
            RestructureConfig(
                paths = PathConfig(
                    output = root,
                    temp = tmpDir
                ),
                worker = WorkerConfig(cacheSize = 2),
                source = ResourceConfig("hdfs", hdfs = HdfsConfig(listOf("test"))),
            ),
        )

        val accountant = mock<Accountant>()
        factory.newFileCacheStore(accountant).useSuspended { cache ->
            val i0 = 0
            var i1 = 0

            record = GenericRecordBuilder(simpleSchema).set("a", "something").build()
            var transaction: Accountant.Transaction =
                Accountant.Transaction(topicPartition1, i1++.toLong(), lastModified)
            assertEquals(FileCacheStore.WriteResponse.NO_CACHE_AND_WRITE,
                cache.writeRecord(f1, record, transaction))
            record = GenericRecordBuilder(simpleSchema).set("a", "somethingElse").build()
            transaction = Accountant.Transaction(topicPartition1, i1++.toLong(), lastModified)
            assertEquals(FileCacheStore.WriteResponse.CACHE_AND_WRITE,
                cache.writeRecord(f1, record, transaction))
            record = GenericRecordBuilder(simpleSchema).set("a", "something").build()
            transaction = Accountant.Transaction(topicPartition0, i0.toLong(), lastModified)
            assertEquals(FileCacheStore.WriteResponse.NO_CACHE_AND_WRITE,
                cache.writeRecord(f2, record, transaction))
            record = GenericRecordBuilder(simpleSchema).set("a", "third").build()
            transaction = Accountant.Transaction(topicPartition1, i1++.toLong(), lastModified)
            assertEquals(FileCacheStore.WriteResponse.CACHE_AND_WRITE,
                cache.writeRecord(f1, record, transaction))
            record = GenericRecordBuilder(simpleSchema).set("a", "f3").build()
            transaction = Accountant.Transaction(topicPartition1, i1++.toLong(), lastModified)
            assertEquals(FileCacheStore.WriteResponse.NO_CACHE_AND_WRITE,
                cache.writeRecord(f3, record, transaction))
            record = GenericRecordBuilder(simpleSchema).set("a", "f2").build()
            transaction = Accountant.Transaction(topicPartition1, i1++.toLong(), lastModified)
            assertEquals(FileCacheStore.WriteResponse.NO_CACHE_AND_WRITE,
                cache.writeRecord(f2, record, transaction))
            record = GenericRecordBuilder(simpleSchema).set("a", "f3").build()
            transaction = Accountant.Transaction(topicPartition1, i1++.toLong(), lastModified)
            assertEquals(FileCacheStore.WriteResponse.CACHE_AND_WRITE,
                cache.writeRecord(f3, record, transaction))
            record = GenericRecordBuilder(simpleSchema).set("a", "f4").build()
            transaction = Accountant.Transaction(topicPartition1, i1++.toLong(), lastModified)
            assertEquals(FileCacheStore.WriteResponse.NO_CACHE_AND_WRITE,
                cache.writeRecord(f4, record, transaction))
            record = GenericRecordBuilder(simpleSchema).set("a", "f3").build()
            transaction = Accountant.Transaction(topicPartition1, i1++.toLong(), lastModified)
            assertEquals(FileCacheStore.WriteResponse.CACHE_AND_WRITE,
                cache.writeRecord(f3, record, transaction))
            record =
                GenericRecordBuilder(conflictSchema).set("a", "f3").set("b", "conflict").build()
            transaction = Accountant.Transaction(topicPartition1, i1.toLong(), lastModified)
            assertEquals(FileCacheStore.WriteResponse.CACHE_AND_NO_WRITE,
                cache.writeRecord(f3, record, transaction))
            record =
                GenericRecordBuilder(conflictSchema).set("a", "f1").set("b", "conflict").build()
            // Cannot write to file even though the file is not in cache since schema is different
            assertEquals(FileCacheStore.WriteResponse.NO_CACHE_AND_NO_WRITE,
                cache.writeRecord(f1, record, transaction))
            // Can write the same record to a new file
            assertEquals(FileCacheStore.WriteResponse.NO_CACHE_AND_WRITE,
                cache.writeRecord(newFile, record, transaction))
        }

        val offsets = OffsetRangeSet()

        verify(accountant, times(7)).process(check {
            offsets.addAll(it.offsets)
        })

        assertTrue(offsets.contains(offsetRange0))
        assertTrue(offsets.contains(offsetRange1))

        launch(Dispatchers.IO) {
            assertEquals("a\nsomething\nsomethingElse\nthird\n", String(Files.readAllBytes(f1)))
        }
        launch(Dispatchers.IO) {
            assertEquals("a\nsomething\nf2\n", String(Files.readAllBytes(f2)))
        }
        launch(Dispatchers.IO) {
            assertEquals("a\nf3\nf3\nf3\n", String(Files.readAllBytes(f3)))
        }
        launch(Dispatchers.IO) {
            assertEquals("a\nf4\n", String(Files.readAllBytes(f4)))
        }
        launch(Dispatchers.IO) {
            assertEquals("a,b\nf1,conflict\n", String(Files.readAllBytes(newFile)))
        }
    }
}
