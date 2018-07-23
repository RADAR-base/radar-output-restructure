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

package org.radarcns.hdfs.accounting;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.csv.CsvFactory;
import com.fasterxml.jackson.dataformat.csv.CsvGenerator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.radarcns.hdfs.data.StorageDriver;
import org.radarcns.hdfs.util.PostponedWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.radarcns.hdfs.util.ThrowingConsumer.tryCatch;

/**
 * Accesses a OffsetRange file using the CSV format. On construction, this will create the file if
 * not present.
 */
public final class OffsetRangeFile extends PostponedWriter {
    private static final Logger logger = LoggerFactory.getLogger(OffsetRangeFile.class);

    private static final CsvSchema SCHEMA = CsvSchema.builder()
            .addNumberColumn("offsetFrom")
            .addNumberColumn("offsetTo")
            .addNumberColumn("partition")
            .addColumn("topic")
            .build()
            .withHeader();

    private static final CsvFactory CSV_FACTORY = new CsvFactory();
    private static final CsvMapper CSV_MAPPER = new CsvMapper(CSV_FACTORY);
    private static final ObjectReader CSV_READER = CSV_MAPPER.reader(SCHEMA.withHeader())
            .forType(OffsetRange.class);


    private final StorageDriver storage;
    private final Path path;
    private final OffsetRangeSet offsets;

    public OffsetRangeFile(StorageDriver storage, Path path, OffsetRangeSet startSet) {
        super("offsets", 1, TimeUnit.SECONDS);
        this.path = path;
        this.storage = storage;
        offsets = startSet == null ? new OffsetRangeSet() : startSet;
    }

    public static OffsetRangeFile read(StorageDriver storage, Path path) {
        try {
            if (storage.exists(path)) {
                OffsetRangeSet set = new OffsetRangeSet();
                try (BufferedReader br = storage.newBufferedReader(path)) {
                    MappingIterator<OffsetRange> ranges = CSV_READER.readValues(br);
                    while (ranges.hasNext()) {
                        set.add(ranges.next());
                    }
                }
                return new OffsetRangeFile(storage, path, set);
            } else {
                return new OffsetRangeFile(storage, path, null);
            }
        } catch (IOException ex) {
            logger.error("Error reading offsets file. Processing all offsets.");
            return new OffsetRangeFile(storage, path, null);
        }
    }

    public OffsetRangeSet getOffsets() {
        return offsets;
    }

    public void add(OffsetRange range) {
        offsets.add(range);
    }

    public void addAll(OffsetRangeSet rangeSet) {
        offsets.addAll(rangeSet);
    }

    protected void doWrite() {
        BufferedOutputStream out;
        CsvGenerator generator;

        try {
            Path tmpPath = createTempFile("offsets", ".csv");
            out = new BufferedOutputStream(Files.newOutputStream(tmpPath));
            generator = CSV_FACTORY.createGenerator(out);
            ObjectWriter writer = CSV_MAPPER.writerFor(OffsetRange.class).with(SCHEMA);

            offsets.ranges()
                    .forEach(tryCatch(r -> writer.writeValue(generator, r),
                            "Failed to write value"));

            generator.flush();
            generator.close();
            out.close();
            storage.store(tmpPath, path);
        } catch (IOException e) {
            logger.error("Failed to write offsets", e);
        }
    }
}
