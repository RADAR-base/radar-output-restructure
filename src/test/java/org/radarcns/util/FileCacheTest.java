package org.radarcns.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class FileCacheTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void appendLine() throws IOException {
        File f1 = folder.newFile();
        File f2 = folder.newFile();
        File f3 = folder.newFile();
        File d4 = folder.newFolder();
        File f4 = new File(d4, "f4.txt");

        assertTrue(f1.delete());
        assertTrue(d4.delete());

        RecordConverterFactory csvFactory = CsvAvroConverter.getFactory();
        Schema simpleSchema = SchemaBuilder.record("simple").fields()
                .name("a").type("string").noDefault()
                .endRecord();

        GenericRecord record;

        try (FileCache cache = new FileCache(csvFactory, 2)) {
            record = new GenericRecordBuilder(simpleSchema).set("a", "something").build();
            assertFalse(cache.writeRecord(f1, record));
            record = new GenericRecordBuilder(simpleSchema).set("a", "somethingElse").build();
            assertTrue(cache.writeRecord(f1, record));
            record = new GenericRecordBuilder(simpleSchema).set("a", "something").build();
            assertFalse(cache.writeRecord(f2, record));
            record = new GenericRecordBuilder(simpleSchema).set("a", "third").build();
            assertTrue(cache.writeRecord(f1, record));
            record = new GenericRecordBuilder(simpleSchema).set("a", "f3").build();
            assertFalse(cache.writeRecord(f3, record));
            record = new GenericRecordBuilder(simpleSchema).set("a", "f2").build();
            assertFalse(cache.writeRecord(f2, record));
            record = new GenericRecordBuilder(simpleSchema).set("a", "f3").build();
            assertTrue(cache.writeRecord(f3, record));
            record = new GenericRecordBuilder(simpleSchema).set("a", "f4").build();
            assertFalse(cache.writeRecord(f4, record));
            record = new GenericRecordBuilder(simpleSchema).set("a", "f3").build();
            assertTrue(cache.writeRecord(f3, record));
        }

        assertEquals("a\nsomething\nsomethingElse\nthird\n", new String(Files.readAllBytes(f1.toPath())));
        assertEquals("a\nsomething\nf2\n", new String(Files.readAllBytes(f2.toPath())));
        assertEquals("a\nf3\nf3\nf3\n", new String(Files.readAllBytes(f3.toPath())));
        assertEquals("a\nf4\n", new String(Files.readAllBytes(f4.toPath())));
    }
}
