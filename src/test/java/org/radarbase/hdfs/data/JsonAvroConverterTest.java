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

package org.radarbase.hdfs.data;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.support.io.TempDirectory;
import org.junit.jupiter.api.support.io.TempDirectory.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.radarbase.hdfs.data.CsvAvroConverterTest.writeTestNumbers;

public class JsonAvroConverterTest {
    @Test
    public void fullAvroTest() throws IOException {
        Parser parser = new Parser();
        Schema schema = parser.parse(getClass().getResourceAsStream("full.avsc"));
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        JsonDecoder decoder = DecoderFactory.get().jsonDecoder(schema, getClass().getResourceAsStream("full.json"));
        GenericRecord record = reader.read(null, decoder);

        Map<String, Object> map = JsonAvroConverter.getFactory().converterFor(new StringWriter(), record, false, new StringReader("test")).convertRecord(record);
        ObjectWriter writer = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).writer();
        String result = writer.writeValueAsString(map);

        String expected = new BufferedReader(new InputStreamReader(
                getClass().getResourceAsStream("full.json")))
                .lines().collect(Collectors.joining("\n"));

        System.out.println(result);

        String[] expectedLines = expected.split("\n");
        String[] resultLines = result.split("\n");
        assertEquals(expectedLines.length, resultLines.length);

        List<Integer> ignoreLines = Arrays.asList(2, 3, 13);
        for (int i = 0; i < expectedLines.length; i++) {
            if (ignoreLines.contains(i)) {
                continue;
            }
            assertEquals(expectedLines[i], resultLines[i]);
        }
    }

    @Test
    @ExtendWith(TempDirectory.class)
    public void deduplicate(@TempDir Path folder) throws IOException {
        Path path = folder.resolve("test.txt");
        try (BufferedWriter writer = Files.newBufferedWriter(path)) {
            writeTestNumbers(writer);
        }
        JsonAvroConverter.getFactory().deduplicate("t", path, path, new IdentityCompression());
        assertEquals(Arrays.asList("a,b", "1,2", "3,4", "1,3", "a,a"), Files.readAllLines(path));
    }
}