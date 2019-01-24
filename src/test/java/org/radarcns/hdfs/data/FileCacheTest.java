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

package org.radarcns.hdfs.data;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.support.io.TempDirectory;
import org.junit.jupiter.api.support.io.TempDirectory.TempDir;
import org.radarcns.hdfs.Application;
import org.radarcns.hdfs.accounting.Accountant;
import org.radarcns.hdfs.accounting.Bin;
import org.radarcns.hdfs.accounting.TopicPartition;
import org.radarcns.hdfs.config.RestructureSettings;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.GZIPInputStream;

/**
 * Created by joris on 03/07/2017.
 */
@ExtendWith(TempDirectory.class)
public class FileCacheTest {
    private Path path;
    private Record exampleRecord;
    private Path tmpDir;
    private Bin bin;
    private Application factory;
    private Accountant accountant;
    private TopicPartition topicPartition;

    @BeforeEach
    public void setUp(@TempDir Path path, @TempDir Path tmpPath) throws IOException {
        this.path = path.resolve("f");
        this.tmpDir = tmpPath;

        Schema schema = SchemaBuilder.record("simple").fields()
                .name("a").type("string").noDefault()
                .endRecord();
        this.exampleRecord = new GenericRecordBuilder(schema).set("a", "something").build();

        setUp(settingsBuilder().build());

        this.bin = new Bin("t", "c", "00");
        this.topicPartition = new TopicPartition("t", 0);
    }

    private RestructureSettings.Builder settingsBuilder() {
        return new RestructureSettings.Builder(path.getParent().toString());
    }

    private void setUp(RestructureSettings settings) throws IOException {
        this.factory = new Application.Builder(settings).build();
        this.accountant = new Accountant(factory);
    }

    @Test
    public void testGzip() throws IOException {
        setUp(settingsBuilder().compression("gzip").build());

        try (FileCache cache = new FileCache(factory, path, exampleRecord, tmpDir, accountant)) {
            cache.writeRecord(exampleRecord,
                    new Accountant.Transaction(topicPartition, 0L, bin));
        }

        System.out.println("Gzip: " + Files.size(path));

        try (InputStream fin = Files.newInputStream(path);
             GZIPInputStream gzipIn = new GZIPInputStream(fin);
             Reader readerIn = new InputStreamReader(gzipIn);
             BufferedReader reader = new BufferedReader(readerIn)) {
            assertEquals("a", reader.readLine());
            assertEquals("something", reader.readLine());
            assertNull(reader.readLine());
        }
    }

    @Test
    public void testGzipAppend() throws IOException {
        setUp(settingsBuilder().compression("gzip").build());

        try (FileCache cache = new FileCache(factory, path, exampleRecord, tmpDir, accountant)) {
            cache.writeRecord(exampleRecord,
                    new Accountant.Transaction(topicPartition, 0, bin));
        }

        try (FileCache cache = new FileCache(factory, path, exampleRecord, tmpDir, accountant)) {
            cache.writeRecord(exampleRecord,
                    new Accountant.Transaction(topicPartition, 0, bin));
        }

        System.out.println("Gzip appended: " + Files.size(path));

        try (InputStream fin = Files.newInputStream(path);
             GZIPInputStream gzipIn = new GZIPInputStream(fin);
             Reader readerIn = new InputStreamReader(gzipIn);
             BufferedReader reader = new BufferedReader(readerIn)) {
            assertEquals("a", reader.readLine());
            assertEquals("something", reader.readLine());
            assertEquals("something", reader.readLine());
            assertNull(reader.readLine());
        }
    }


    @Test
    public void testPlain() throws IOException {
        try (FileCache cache = new FileCache(factory, path, exampleRecord, tmpDir, accountant)) {
            cache.writeRecord(exampleRecord,
                    new Accountant.Transaction(topicPartition, 0, bin));
        }

        System.out.println("Plain: " + Files.size(path));

        try (BufferedReader reader = Files.newBufferedReader(path)) {
            assertEquals("a", reader.readLine());
            assertEquals("something", reader.readLine());
            assertNull(reader.readLine());
        }
    }

    @Test
    public void testPlainAppend() throws IOException {
        try (FileCache cache = new FileCache(factory, path, exampleRecord, tmpDir, accountant)) {
            cache.writeRecord(exampleRecord,
                    new Accountant.Transaction(topicPartition, 0, bin));
        }

        try (FileCache cache = new FileCache(factory, path, exampleRecord, tmpDir, accountant)) {
            cache.writeRecord(exampleRecord,
                    new Accountant.Transaction(topicPartition, 1, bin));
        }

        System.out.println("Plain appended: " + Files.size(path));

        try (BufferedReader reader = Files.newBufferedReader(path)) {
            assertEquals("a", reader.readLine());
            assertEquals("something", reader.readLine());
            assertEquals("something", reader.readLine());
            assertNull(reader.readLine());
        }
    }

    @Test
    @ExtendWith(TempDirectory.class)
    public void compareTo(@TempDir Path tmp3) throws IOException {
        Path file3 = tmp3.resolve("file3");
        try (FileCache cache1 = new FileCache(factory, path, exampleRecord, tmpDir, accountant);
             FileCache cache2 = new FileCache(factory, path, exampleRecord, tmpDir, accountant);
             FileCache cache3 = new FileCache(factory, file3, exampleRecord, tmpDir, accountant)) {
            assertEquals(0, cache1.compareTo(cache2));
            // filenames are not equal
            assertTrue(cache1.compareTo(cache3) < 0);
            cache1.writeRecord(exampleRecord, new Accountant.Transaction(topicPartition, 0, bin));
            // last used
            assertTrue(cache1.compareTo(cache2) > 0);
            // last used takes precedence over filename
            assertTrue(cache1.compareTo(cache3) > 0);

            // last used reversal
            cache2.writeRecord(exampleRecord, new Accountant.Transaction(topicPartition, 1, bin));
            cache3.writeRecord(exampleRecord, new Accountant.Transaction(topicPartition, 2, bin));
            assertTrue(cache1.compareTo(cache2) < 0);
            assertTrue(cache1.compareTo(cache3) < 0);
        }
    }
}
