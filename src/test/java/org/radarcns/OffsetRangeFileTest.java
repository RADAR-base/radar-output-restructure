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

package org.radarcns;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

public class OffsetRangeFileTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void readEmpty() throws IOException {
        File testFile = folder.newFile();
        try (OffsetRangeFile rangeFile = new OffsetRangeFile(testFile)) {
            assertTrue(rangeFile.read().isEmpty());
        }

        assertTrue(testFile.delete());

        // will create on write
        try (OffsetRangeFile rangeFile = new OffsetRangeFile(testFile)) {
            assertTrue(rangeFile.read().isEmpty());
        }
    }

    @Test
    public void write() throws IOException {
        File testFile = folder.newFile();
        try (OffsetRangeFile rangeFile = new OffsetRangeFile(testFile)) {
            rangeFile.write(OffsetRange.parse("a+0+0+1"));
            rangeFile.write(OffsetRange.parse("a+0+1+2"));
        }
        System.out.println(new String(Files.readAllBytes(testFile.toPath())));

        OffsetRangeSet set;
        // will create on write
        try (OffsetRangeFile rangeFile = new OffsetRangeFile(testFile)) {
            set = rangeFile.read();
        }
        System.out.println(set);
        assertTrue(set.contains(OffsetRange.parse("a+0+0+1")));
        assertTrue(set.contains(OffsetRange.parse("a+0+1+2")));
        assertTrue(set.contains(OffsetRange.parse("a+0+0+2")));
        assertFalse(set.contains(OffsetRange.parse("a+0+0+3")));
        assertFalse(set.contains(OffsetRange.parse("a+0+2+3")));
        assertFalse(set.contains(OffsetRange.parse("a+1+0+1")));
        assertFalse(set.contains(OffsetRange.parse("b+0+0+1")));
    }
}