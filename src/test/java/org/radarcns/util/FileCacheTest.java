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

package org.radarcns.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.zip.GZIPInputStream;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Created by joris on 03/07/2017.
 */
public class FileCacheTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    private File file;
    private RecordConverterFactory csvFactory;
    private Record exampleRecord;

    @Before
    public void setUp() throws IOException {
        this.file = folder.newFile("f");
        this.csvFactory = CsvAvroConverter.getFactory();
        Schema schema = SchemaBuilder.record("simple").fields()
                .name("a").type("string").noDefault()
                .endRecord();
        this.exampleRecord = new GenericRecordBuilder(schema).set("a", "something").build();
    }

    @Test
    public void testGzip() throws IOException {
        try (FileCache cache = new FileCache(csvFactory, file, exampleRecord, true)) {
            cache.writeRecord(exampleRecord);
        }

        System.out.println("Gzip: " + file.length());

        try (FileInputStream fin = new FileInputStream(file);
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
        try (FileCache cache = new FileCache(csvFactory, file, exampleRecord, true)) {
            cache.writeRecord(exampleRecord);
        }

        try (FileCache cache = new FileCache(csvFactory, file, exampleRecord, true)) {
            cache.writeRecord(exampleRecord);
        }

        System.out.println("Gzip appended: " + file.length());

        try (FileInputStream fin = new FileInputStream(file);
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
        try (FileCache cache = new FileCache(csvFactory, file, exampleRecord, false)) {
            cache.writeRecord(exampleRecord);
        }

        System.out.println("Plain: " + file.length());

        try (FileReader readerIn = new FileReader(file);
                BufferedReader reader = new BufferedReader(readerIn)) {
            assertEquals("a", reader.readLine());
            assertEquals("something", reader.readLine());
            assertNull(reader.readLine());
        }
    }

    @Test
    public void testPlainAppend() throws IOException {
        try (FileCache cache = new FileCache(csvFactory, file, exampleRecord, false)) {
            cache.writeRecord(exampleRecord);
        }

        try (FileCache cache = new FileCache(csvFactory, file, exampleRecord, false)) {
            cache.writeRecord(exampleRecord);
        }

        System.out.println("Plain appended: " + file.length());

        try (FileReader readerIn = new FileReader(file);
                BufferedReader reader = new BufferedReader(readerIn)) {
            assertEquals("a", reader.readLine());
            assertEquals("something", reader.readLine());
            assertEquals("something", reader.readLine());
            assertNull(reader.readLine());
        }
    }

    @Test
    public void compareTo() throws IOException {
        File file3 = folder.newFile("g");

        try (FileCache cache1 = new FileCache(csvFactory, file, exampleRecord, false);
                FileCache cache2 = new FileCache(csvFactory, file, exampleRecord, false);
                FileCache cache3 = new FileCache(csvFactory, file3, exampleRecord, false)) {
            assertEquals(0, cache1.compareTo(cache2));
            // filenames are not equal
            assertEquals(-1, cache1.compareTo(cache3));
            cache1.writeRecord(exampleRecord);
            // last used
            assertEquals(1, cache1.compareTo(cache2));
            // last used takes precedence over filename
            assertEquals(1, cache1.compareTo(cache3));

            // last used reversal
            cache2.writeRecord(exampleRecord);
            cache3.writeRecord(exampleRecord);
            assertEquals(-1, cache1.compareTo(cache2));
            assertEquals(-1, cache1.compareTo(cache3));
        }
    }
}
