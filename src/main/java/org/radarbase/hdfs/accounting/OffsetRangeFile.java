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

package org.radarbase.hdfs.accounting;

import static org.radarbase.hdfs.util.ThrowingConsumer.tryCatch;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.radarbase.hdfs.data.StorageDriver;
import org.radarbase.hdfs.util.PostponedWriter;
import org.radarbase.hdfs.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Accesses a OffsetRange file using the CSV format. On construction, this will create the file if
 * not present.
 */
public final class OffsetRangeFile extends PostponedWriter {
    public static final Pattern COMMA_PATTERN = Pattern.compile(",");
    private static final Logger logger = LoggerFactory.getLogger(OffsetRangeFile.class);

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
                    // ignore header
                    String line = br.readLine();
                    if (line != null) {
                        line = br.readLine();
                        while (line != null) {
                            String[] cols = COMMA_PATTERN.split(line);
                            String topic = cols[3];
                            while (topic.charAt(0) == '"') {
                                topic = topic.substring(1);
                            }
                            while (topic.charAt(topic.length() - 1) == '"') {
                                topic = topic.substring(0, topic.length() - 1);
                            }
                            set.add(new OffsetRange(
                                    topic,
                                    Integer.parseInt(cols[2]),
                                    Long.parseLong(cols[0]),
                                    Long.parseLong(cols[1])));
                            line = br.readLine();
                        }
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
        long timeStart = System.nanoTime();

        try {
            Path tmpPath = createTempFile("offsets", ".csv");

            try (Writer write = Files.newBufferedWriter(tmpPath)) {
                write.append("offsetFrom,offsetTo,partition,topic\n");
                offsets.stream()
                        .forEach(tryCatch(r -> {
                            write.write(String.valueOf(r.getOffsetFrom()));
                            write.write(',');
                            write.write(String.valueOf(r.getOffsetTo()));
                            write.write(',');
                            write.write(String.valueOf(r.getPartition()));
                            write.write(',');
                            write.write(r.getTopic());
                            write.write('\n');
                        },"Failed to write value"));
            }

            storage.store(tmpPath, path);
        } catch (IOException e) {
            logger.error("Failed to write offsets: {}", e.toString());
        } finally {
            Timer.getInstance().add("accounting.offsets", timeStart);
        }
    }
}
