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

package org.radarbase.output

import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.radarbase.output.accounting.OffsetFilePersistence
import org.radarbase.output.accounting.OffsetPersistenceFactory
import org.radarbase.output.accounting.TopicPartition
import org.radarbase.output.accounting.TopicPartitionOffsetRange
import org.radarbase.output.config.LocalConfig
import org.radarbase.output.target.LocalTargetStorage
import org.radarbase.output.target.TargetStorage
import org.radarbase.output.util.SuspendedCloseable.Companion.useSuspended
import java.io.IOException
import java.nio.file.Path
import java.time.Instant
import kotlin.io.path.createFile

class OffsetRangeFileTest {
    private lateinit var testFile: Path
    private lateinit var targetStorage: TargetStorage
    private lateinit var offsetPersistence: OffsetPersistenceFactory
    private val lastModified = Instant.now()

    @BeforeEach
    @Throws(IOException::class)
    fun setUp(@TempDir dir: Path) {
        testFile = dir.resolve("test")
        testFile.createFile()
        targetStorage = LocalTargetStorage(LocalConfig())
        offsetPersistence = OffsetFilePersistence(targetStorage)
    }

    @Test
    @Throws(IOException::class)
    fun readEmpty() = runTest {
        assertEquals(java.lang.Boolean.TRUE, offsetPersistence.read(testFile)?.isEmpty)

        targetStorage.delete(testFile)

        // will not create
        assertNull(offsetPersistence.read(testFile))
    }

    @Test
    @Throws(IOException::class)
    fun write() = runTest {
        offsetPersistence.writer(this@runTest, testFile).useSuspended { rangeFile ->
            rangeFile.add(TopicPartitionOffsetRange.parseFilename("a+0+0+1", lastModified))
            rangeFile.add(TopicPartitionOffsetRange.parseFilename("a+0+1+2", lastModified))
        }

        val set = offsetPersistence.read(testFile)
        assertNotNull(set)
        requireNotNull(set)
        assertTrue(set.contains(TopicPartitionOffsetRange.parseFilename("a+0+0+1", lastModified)))
        assertTrue(set.contains(TopicPartitionOffsetRange.parseFilename("a+0+1+2", lastModified)))
        assertTrue(set.contains(TopicPartitionOffsetRange.parseFilename("a+0+0+2", lastModified)))
        assertFalse(set.contains(TopicPartitionOffsetRange.parseFilename("a+0+0+3", lastModified)))
        assertFalse(set.contains(TopicPartitionOffsetRange.parseFilename("a+0+2+3", lastModified)))
        assertFalse(set.contains(TopicPartitionOffsetRange.parseFilename("a+1+0+1", lastModified)))
        assertFalse(set.contains(TopicPartitionOffsetRange.parseFilename("b+0+0+1", lastModified)))
    }

    @Test
    @Throws(IOException::class)
    fun cleanUp() = runTest {
        offsetPersistence.writer(this@runTest, testFile).useSuspended { rangeFile ->
            rangeFile.add(TopicPartitionOffsetRange.parseFilename("a+0+0+1", lastModified))
            rangeFile.add(TopicPartitionOffsetRange.parseFilename("a+0+1+2", lastModified))
            rangeFile.add(TopicPartitionOffsetRange.parseFilename("a+0+4+4", lastModified))
        }

        targetStorage.newBufferedReader(testFile).use { br ->
            assertEquals(3, br.lines().count())
        }

        val rangeSet = offsetPersistence.read(testFile)
        assertEquals(2, rangeSet?.size(TopicPartition("a", 0)))
    }
}
