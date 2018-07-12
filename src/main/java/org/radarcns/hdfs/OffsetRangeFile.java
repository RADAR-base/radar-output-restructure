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

package org.radarcns.hdfs;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.csv.CsvFactory;
import com.fasterxml.jackson.dataformat.csv.CsvGenerator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

/**
 * Accesses a OffsetRange file using the CSV format. On construction, this will create the file if
 * not present.
 */
public final class OffsetRangeFile {
    private static final CsvSchema SCHEMA = CsvSchema.builder()
            .addNumberColumn("offsetFrom")
            .addNumberColumn("offsetTo")
            .addNumberColumn("partition")
            .addColumn("topic")
            .build();

    private static final CsvFactory CSV_FACTORY = new CsvFactory();
    private static final CsvMapper CSV_MAPPER = new CsvMapper(CSV_FACTORY);
    private static final ObjectReader CSV_READER = CSV_MAPPER.reader(SCHEMA.withHeader())
            .forType(OffsetRange.class);

    private OffsetRangeFile() {
        // utility class
    }

    public static void cleanUp(Path path) throws IOException {
        Path tmpPath = Files.createTempFile("offsets", ".csv.tmp");
        try (OffsetRangeFile.Writer offsets = new OffsetRangeFile.Writer(tmpPath)) {
            offsets.write(OffsetRangeFile.read(path));
        }
        Files.move(tmpPath, path, REPLACE_EXISTING);
    }

    public static OffsetRangeSet read(Path path) throws IOException {
        OffsetRangeSet set = new OffsetRangeSet();

        try (BufferedReader br = Files.newBufferedReader(path)) {
            MappingIterator<OffsetRange> ranges = CSV_READER.readValues(br);
            while(ranges.hasNext()) {
                set.add(ranges.next());
            }
        }
        return set;
    }

    public static class Writer implements Flushable, Closeable {
        private final BufferedWriter bufferedWriter;
        private final CsvGenerator generator;
        private final ObjectWriter writer;

        public Writer(Path path) throws IOException {
            boolean fileIsNew = !Files.exists(path) || Files.size(path) == 0;
            this.bufferedWriter = Files.newBufferedWriter(path, StandardOpenOption.APPEND, StandardOpenOption.CREATE);
            this.generator = CSV_FACTORY.createGenerator(bufferedWriter);
            this.writer = CSV_MAPPER.writerFor(OffsetRange.class)
                    .with(fileIsNew ? SCHEMA.withHeader() : SCHEMA);
        }

        public void write(OffsetRange range) throws IOException {
            writer.writeValue(generator, range);
        }

        public void write(OffsetRangeSet rangeSet) throws IOException {
            for (OffsetRange range : rangeSet) {
                write(range);
            }
        }

        @Override
        public void flush() throws IOException {
            generator.flush();
        }

        @Override
        public void close() throws IOException {
            generator.close();
            bufferedWriter.close();
        }
    }
}
