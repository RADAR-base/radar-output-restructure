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

package org.radarcns.data;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class CsvAvroConverterTest {
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void writeRecord() throws IOException {
        Parser parser = new Parser();
        Schema schema = parser.parse(getClass().getResourceAsStream("full.avsc"));
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        JsonDecoder decoder = DecoderFactory.get().jsonDecoder(schema, getClass().getResourceAsStream("full.json"));
        GenericRecord record = reader.read(null, decoder);

        StringWriter writer = new StringWriter();
        RecordConverterFactory factory = CsvAvroConverter.getFactory();
        RecordConverter converter = factory.converterFor(writer, record, true, new StringReader("test"));

        Map<String, Object> map = converter.convertRecord(record);
        List<String> keys = Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h", "i.some",
                "i.other", "j.0", "j.1", "k", "l.la", "m");
        Set<String> expectedKeys = new LinkedHashSet<>(keys);
        assertEquals(expectedKeys, map.keySet());

        Iterator<Object> actualIterator = map.values().iterator();
        Iterator<Object> expectedIterator = Arrays.<Object>asList(
                "a", new byte[] {(byte)255}, new byte[] {(byte)255}, 1000000000000000000L,
                1.21322421E-15, 0.1213231f, 132101, null, 1, -1, null, "some", "Y", null, false).iterator();

        int i = 0;
        while (actualIterator.hasNext()) {
            assertTrue("Actual value has more entries than expected value", expectedIterator.hasNext());
            Object actual = actualIterator.next();
            Object expected = expectedIterator.next();

            if (expected instanceof byte[]) {
                assertArrayEquals("Array for argument " + keys.get(i) + " does not match", (byte[])expected, (byte[])actual);
            } else {
                assertEquals("Value for argument " + keys.get(i) + " does not match", expected, actual);
            }
            i++;
        }
        assertFalse("Actual value has fewer entries than expected value", expectedIterator.hasNext());

        converter.writeRecord(record);

        String writtenValue = writer.toString();
        String[] lines = writtenValue.split("\n");
        assertEquals(2, lines.length);
        assertEquals(String.join(",", keys), lines[0]);
        System.out.println(lines[1]);
    }

    @Test
    public void differentSchema() throws IOException {
        Schema schemaA = SchemaBuilder.record("A").fields().name("a").type("string").noDefault().endRecord();
        GenericRecord recordA = new GenericRecordBuilder(schemaA).set("a", "something").build();

        StringWriter writer = new StringWriter();
        RecordConverter converter = CsvAvroConverter.getFactory().converterFor(writer, recordA, true, new StringReader("test"));
        converter.writeRecord(recordA);

        Schema schemaB = SchemaBuilder.record("B").fields().name("b").type("string").noDefault().endRecord();
        GenericRecord recordB = new GenericRecordBuilder(schemaB).set("b", "something").build();

        /* Same number of columns but different schema, so CsvAvroConverter.write() will return false
        signifying that a new CSV file must be used to write this record
         */
        assertFalse(converter.writeRecord(recordB));
        System.out.println(writer.toString());
    }


    @Test
    public void differentSchema2() throws IOException {
        Schema schemaA = SchemaBuilder.record("A").fields().name("a").type("string").noDefault().name("b").type("string").noDefault().endRecord();
        GenericRecord recordA = new GenericRecordBuilder(schemaA).set("a", "something").set("b", "2nd something").build();

        StringWriter writer = new StringWriter();
        RecordConverter converter = CsvAvroConverter.getFactory().converterFor(writer, recordA, true, new StringReader("test"));
        converter.writeRecord(recordA);

        Schema schemaB = SchemaBuilder.record("B").fields().name("b").type("string").noDefault().name("a").type("string").noDefault().endRecord();
        GenericRecord recordB = new GenericRecordBuilder(schemaB).set("b", "something").set("a", "2nd something").build();

        /* Same number of columns and same header but different order,
        so CsvAvroConverter.write() will return false signifying that
        a new CSV file must be used to write this record
         */
        assertFalse(converter.writeRecord(recordB));
        System.out.println(writer.toString());
    }

    @Test
    public void subSchema() throws IOException {
        Schema schemaA = SchemaBuilder.record("A").fields().name("a").type("string").noDefault().name("b").type("string").withDefault("def").endRecord();
        GenericRecord recordA = new GenericRecordBuilder(schemaA)
                .set("a", "something")
                .set("b", "somethingElse")
                .build();

        StringWriter writer = new StringWriter();
        RecordConverter converter = CsvAvroConverter.getFactory().converterFor(writer, recordA, true, new StringReader("test"));
        converter.writeRecord(recordA);

        Schema schemaB = SchemaBuilder.record("B").fields().name("b").type("string").noDefault().endRecord();
        GenericRecord recordB = new GenericData.Record(schemaB);
        recordB.put("b", "something");

        converter.writeRecord(recordB);

        GenericRecord recordC = new GenericRecordBuilder(schemaA).set("a", "that").build();
        recordC.put("a", "that");
        converter.writeRecord(recordC);

        System.out.println(writer.toString());
    }

    static void writeTestNumbers(Writer writer) throws IOException {
        writer.write("a,b\n");
        writer.write("1,2\n");
        writer.write("3,4\n");
        writer.write("1,3\n");
        writer.write("3,4\n");
        writer.write("1,2\n");
        writer.write("a,a\n");
    }

    @Test
    public void deduplicate() throws IOException {
        Path path = folder.newFile().toPath();
        try (BufferedWriter writer = Files.newBufferedWriter(path)) {
            writeTestNumbers(writer);
        }
        CsvAvroConverter.getFactory().sortUnique(path);
        assertEquals(Arrays.asList("a,b", "1,2", "1,3", "3,4", "a,a"), Files.readAllLines(path));
    }


    @Test
    public void deduplicateGzip() throws IOException {
        Path path = folder.newFile("test.csv.gz").toPath();
        try (OutputStream out = Files.newOutputStream(path);
             GZIPOutputStream gzipOut = new GZIPOutputStream(out);
             Writer writer = new OutputStreamWriter(gzipOut)) {
            writeTestNumbers(writer);
        }
        CsvAvroConverter.getFactory().sortUnique(path);
        try (InputStream in = Files.newInputStream(path);
                GZIPInputStream gzipIn = new GZIPInputStream(in);
                Reader inReader = new InputStreamReader(gzipIn);
                BufferedReader reader = new BufferedReader(inReader)) {
            assertEquals("a,b", reader.readLine());
            assertEquals("1,2", reader.readLine());
            assertEquals("1,3", reader.readLine());
            assertEquals("3,4", reader.readLine());
            assertEquals("a,a", reader.readLine());
            assertNull(reader.readLine());
        }
    }
}