package org.radarcns.util;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonMappingException;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

public class CsvAvroConverterTest {
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void writeRecord() throws IOException {
        Parser parser = new Parser();
        Schema schema = parser.parse(getClass().getResourceAsStream("full.avsc"));
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        JsonDecoder decoder = DecoderFactory.get().jsonDecoder(schema, getClass().getResourceAsStream("full.json"));
        GenericRecord record = reader.read(null, decoder);

        StringWriter writer = new StringWriter();
        RecordConverterFactory factory = CsvAvroConverter.getFactory();
        RecordConverter converter = factory.converterFor(writer, record, true);

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
        RecordConverter converter = CsvAvroConverter.getFactory().converterFor(writer, recordA, true);
        converter.writeRecord(recordA);

        Schema schemaB = SchemaBuilder.record("B").fields().name("b").type("string").noDefault().endRecord();
        GenericRecord recordB = new GenericRecordBuilder(schemaB).set("b", "something").build();

        exception.expect(JsonMappingException.class);
        converter.writeRecord(recordB);
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
        RecordConverter converter = CsvAvroConverter.getFactory().converterFor(writer, recordA, true);
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
}