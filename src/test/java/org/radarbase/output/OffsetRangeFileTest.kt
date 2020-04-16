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

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.radarbase.output.accounting.OffsetRange
import org.radarbase.output.accounting.OffsetRangeFile
import org.radarbase.output.accounting.OffsetRangeSerialization
import org.radarbase.output.accounting.TopicPartition
import org.radarbase.output.config.LocalConfig
import org.radarbase.output.storage.LocalStorageDriver
import org.radarbase.output.storage.StorageDriver
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path

class OffsetRangeFileTest {
    private lateinit var testFile: Path
    private lateinit var storage: StorageDriver
    private lateinit var offsetReader: OffsetRangeSerialization.Reader

    @BeforeEach
    @Throws(IOException::class)
    fun setUp(@TempDir dir: Path) {
        testFile = dir.resolve("test")
        Files.createFile(testFile)
        storage = LocalStorageDriver(LocalConfig())
        offsetReader = OffsetRangeFile.OffsetFileReader(storage)
    }

    @Test
    @Throws(IOException::class)
    fun readEmpty() {
        assertTrue(offsetReader.read(testFile).offsets.isEmpty)

        storage.delete(testFile)

        // will create on write
        assertTrue(offsetReader.read(testFile).offsets.isEmpty)
    }

    @Test
    @Throws(IOException::class)
    fun write() {
        offsetReader.read(testFile).use { rangeFile ->
            rangeFile.add(OffsetRange.parseFilename("a+0+0+1"))
            rangeFile.add(OffsetRange.parseFilename("a+0+1+2"))
        }

        val set = offsetReader.read(testFile).use { it.offsets }
        assertTrue(set.contains(OffsetRange.parseFilename("a+0+0+1")))
        assertTrue(set.contains(OffsetRange.parseFilename("a+0+1+2")))
        assertTrue(set.contains(OffsetRange.parseFilename("a+0+0+2")))
        assertFalse(set.contains(OffsetRange.parseFilename("a+0+0+3")))
        assertFalse(set.contains(OffsetRange.parseFilename("a+0+2+3")))
        assertFalse(set.contains(OffsetRange.parseFilename("a+1+0+1")))
        assertFalse(set.contains(OffsetRange.parseFilename("b+0+0+1")))
    }

    @Test
    @Throws(IOException::class)
    fun cleanUp() {
        offsetReader.read(testFile).use { rangeFile ->
            rangeFile.add(OffsetRange.parseFilename("a+0+0+1"))
            rangeFile.add(OffsetRange.parseFilename("a+0+1+2"))
            rangeFile.add(OffsetRange.parseFilename("a+0+4+4"))
        }

        storage.newBufferedReader(testFile).use { br ->
            assertEquals(3, br.lines().count())
        }

        val rangeSet = offsetReader.read(testFile).use { it.offsets }
        assertEquals(2, rangeSet.size(TopicPartition("a", 0)))
    }
}
