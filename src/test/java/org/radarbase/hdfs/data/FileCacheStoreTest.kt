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
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericRecordBuilder
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.radarbase.hdfs.Application
import org.radarbase.hdfs.accounting.Accountant
import org.radarbase.hdfs.accounting.OffsetRange
import org.radarbase.hdfs.accounting.TopicPartition
import org.radarbase.hdfs.config.HdfsSettings
import org.radarbase.hdfs.config.RestructureSettings
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path

class FileCacheStoreTest {
    @Test
    @Throws(IOException::class)
    fun appendLine(@TempDir root: Path, @TempDir tmpDir: Path) {
        val f1 = root.resolve("f1")
        val f2 = root.resolve("f2")
        val f3 = root.resolve("f3")
        val d4 = root.resolve("d4")
        Files.createDirectories(d4)
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

        val offsetRange0 = OffsetRange(topicPartition0, 0, 0)
        val offsetRange1 = OffsetRange(topicPartition1, 0, 8)

        val factory = Application.Builder(
                RestructureSettings.Builder(root.toString()).apply {
                    cacheSize = 2
                    tempDir = tmpDir
                }.build(),
                HdfsSettings.Builder("test").build())
                .build()
        val accountant = Accountant(factory, "t")

        factory.newFileCacheStore(accountant).use { cache ->
            var i0 = 0
            var i1 = 0
            var transaction: Accountant.Transaction

            record = GenericRecordBuilder(simpleSchema).set("a", "something").build()
            transaction = Accountant.Transaction(topicPartition1, i1++.toLong())
            assertEquals(FileCacheStore.WriteResponse.NO_CACHE_AND_WRITE,
                    cache.writeRecord(f1, record, transaction))
            record = GenericRecordBuilder(simpleSchema).set("a", "somethingElse").build()
            transaction = Accountant.Transaction(topicPartition1, i1++.toLong())
            assertEquals(FileCacheStore.WriteResponse.CACHE_AND_WRITE,
                    cache.writeRecord(f1, record, transaction))
            record = GenericRecordBuilder(simpleSchema).set("a", "something").build()
            transaction = Accountant.Transaction(topicPartition0, i0.toLong())
            assertEquals(FileCacheStore.WriteResponse.NO_CACHE_AND_WRITE,
                    cache.writeRecord(f2, record, transaction))
            record = GenericRecordBuilder(simpleSchema).set("a", "third").build()
            transaction = Accountant.Transaction(topicPartition1, i1++.toLong())
            assertEquals(FileCacheStore.WriteResponse.CACHE_AND_WRITE,
                    cache.writeRecord(f1, record, transaction))
            record = GenericRecordBuilder(simpleSchema).set("a", "f3").build()
            transaction = Accountant.Transaction(topicPartition1, i1++.toLong())
            assertEquals(FileCacheStore.WriteResponse.NO_CACHE_AND_WRITE,
                    cache.writeRecord(f3, record, transaction))
            record = GenericRecordBuilder(simpleSchema).set("a", "f2").build()
            transaction = Accountant.Transaction(topicPartition1, i1++.toLong())
            assertEquals(FileCacheStore.WriteResponse.NO_CACHE_AND_WRITE,
                    cache.writeRecord(f2, record, transaction))
            record = GenericRecordBuilder(simpleSchema).set("a", "f3").build()
            transaction = Accountant.Transaction(topicPartition1, i1++.toLong())
            assertEquals(FileCacheStore.WriteResponse.CACHE_AND_WRITE,
                    cache.writeRecord(f3, record, transaction))
            record = GenericRecordBuilder(simpleSchema).set("a", "f4").build()
            transaction = Accountant.Transaction(topicPartition1, i1++.toLong())
            assertEquals(FileCacheStore.WriteResponse.NO_CACHE_AND_WRITE,
                    cache.writeRecord(f4, record, transaction))
            record = GenericRecordBuilder(simpleSchema).set("a", "f3").build()
            transaction = Accountant.Transaction(topicPartition1, i1++.toLong())
            assertEquals(FileCacheStore.WriteResponse.CACHE_AND_WRITE,
                    cache.writeRecord(f3, record, transaction))
            record = GenericRecordBuilder(conflictSchema).set("a", "f3").set("b", "conflict").build()
            transaction = Accountant.Transaction(topicPartition1, i1.toLong())
            assertEquals(FileCacheStore.WriteResponse.CACHE_AND_NO_WRITE,
                    cache.writeRecord(f3, record, transaction))
            record = GenericRecordBuilder(conflictSchema).set("a", "f1").set("b", "conflict").build()
            // Cannot write to file even though the file is not in cache since schema is different
            assertEquals(FileCacheStore.WriteResponse.NO_CACHE_AND_NO_WRITE,
                    cache.writeRecord(f1, record, transaction))
            // Can write the same record to a new file
            assertEquals(FileCacheStore.WriteResponse.NO_CACHE_AND_WRITE,
                    cache.writeRecord(newFile, record, transaction))
        }

        assertTrue(accountant.offsets.contains(offsetRange0))
        assertTrue(accountant.offsets.contains(offsetRange1))

        assertEquals("a\nsomething\nsomethingElse\nthird\n", String(Files.readAllBytes(f1)))
        assertEquals("a\nsomething\nf2\n", String(Files.readAllBytes(f2)))
        assertEquals("a\nf3\nf3\nf3\n", String(Files.readAllBytes(f3)))
        assertEquals("a\nf4\n", String(Files.readAllBytes(f4)))
        assertEquals("a,b\nf1,conflict\n", String(Files.readAllBytes(newFile)))
    }
}
