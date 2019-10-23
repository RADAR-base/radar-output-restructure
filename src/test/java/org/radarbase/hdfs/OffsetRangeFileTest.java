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

package org.radarbase.hdfs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.support.io.TempDirectory;
import org.junit.jupiter.api.support.io.TempDirectory.TempDir;
import org.radarbase.hdfs.accounting.OffsetRange;
import org.radarbase.hdfs.accounting.OffsetRangeFile;
import org.radarbase.hdfs.accounting.OffsetRangeSet;
import org.radarbase.hdfs.accounting.TopicPartition;
import org.radarbase.hdfs.data.LocalStorageDriver;
import org.radarbase.hdfs.data.StorageDriver;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Path;

@ExtendWith(TempDirectory.class)
public class OffsetRangeFileTest {
    private Path testFile;
    private StorageDriver storage;

    @BeforeEach
    public void setUp(@TempDir Path dir) throws IOException {
        testFile = dir.resolve("test");
        Files.createFile(testFile);
        storage = new LocalStorageDriver();
    }

    @Test
    public void readEmpty() throws IOException {
        assertTrue(OffsetRangeFile.read(storage, testFile).getOffsets().isEmpty());

        storage.delete(testFile);

        // will create on write
        assertTrue(OffsetRangeFile.read(storage, testFile).getOffsets().isEmpty());
    }

    @Test
    public void write() throws IOException {
        try (OffsetRangeFile rangeFile = new OffsetRangeFile(storage, testFile, null)) {
            rangeFile.add(OffsetRange.parseFilename("a+0+0+1"));
            rangeFile.add(OffsetRange.parseFilename("a+0+1+2"));
        }

        OffsetRangeSet set = OffsetRangeFile.read(storage, testFile).getOffsets();
        assertTrue(set.contains(OffsetRange.parseFilename("a+0+0+1")));
        assertTrue(set.contains(OffsetRange.parseFilename("a+0+1+2")));
        assertTrue(set.contains(OffsetRange.parseFilename("a+0+0+2")));
        assertFalse(set.contains(OffsetRange.parseFilename("a+0+0+3")));
        assertFalse(set.contains(OffsetRange.parseFilename("a+0+2+3")));
        assertFalse(set.contains(OffsetRange.parseFilename("a+1+0+1")));
        assertFalse(set.contains(OffsetRange.parseFilename("b+0+0+1")));
    }

    @Test
    public void cleanUp() throws IOException {
        try (OffsetRangeFile rangeFile = new OffsetRangeFile(storage, testFile, null)) {
            rangeFile.add(OffsetRange.parseFilename("a+0+0+1"));
            rangeFile.add(OffsetRange.parseFilename("a+0+1+2"));
            rangeFile.add(OffsetRange.parseFilename("a+0+4+4"));
        }

        try (BufferedReader br = storage.newBufferedReader(testFile)) {
            assertEquals(3, br.lines().count());
        }

        OffsetRangeSet rangeSet = OffsetRangeFile.read(storage, testFile).getOffsets();
        assertEquals(2, rangeSet.size(new TopicPartition("a", 0)));
    }
}