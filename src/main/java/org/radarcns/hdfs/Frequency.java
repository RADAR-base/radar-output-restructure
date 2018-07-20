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

import org.radarcns.hdfs.data.StorageDriver;
import org.radarcns.hdfs.util.PostponedWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;


public class Frequency extends PostponedWriter {
    private static final Logger logger = LoggerFactory.getLogger(Frequency.class);

    private final ConcurrentMap<Bin, LongAdder> bins;
    private final Path path;
    private final StorageDriver storage;

    public Frequency(@Nonnull StorageDriver storageDriver, @Nonnull Path path,
            @Nonnull ConcurrentMap<Bin, LongAdder> initialData) {
        super("bins", 5, TimeUnit.SECONDS);
        Objects.requireNonNull(path);
        Objects.requireNonNull(initialData);
        this.storage = storageDriver;
        this.path = path;
        this.bins = initialData;
    }

    public static Frequency read(StorageDriver storage, Path path) {
        ConcurrentMap<Bin, LongAdder> map = new ConcurrentHashMap<>();
        try (BufferedReader input = storage.newBufferedReader(path)){
            // Read in all lines as multikeymap (key, key, key, value)
            String line = input.readLine();
            if (line != null) {
                line = input.readLine();
                while (line != null) {
                    String[] columns = line.split(",");
                    try {
                        Bin bin = new Bin(columns[0], columns[1], columns[2]);
                        LongAdder adder = new LongAdder();
                        adder.add(Long.valueOf(columns[3]));
                        map.put(bin, adder);
                    } catch (ArrayIndexOutOfBoundsException ex) {
                        logger.warn("Unable to read row of the bins file. Skipping.");
                    }
                    line = input.readLine();
                }
            }
        } catch (IOException e) {
            logger.warn("Could not read the file with bins. Creating new file when writing.");
        }
        return new Frequency(storage, path, map);
    }

    public void increment(Bin bin) {
        bins.computeIfAbsent(bin, b -> new LongAdder()).increment();
    }

    public void print() {
        bins.forEach((k, v) -> System.out.printf("%s - %d\n", k, v.sum()));
    }

    protected void doWrite() {
        try {
            Path tmpPath = Files.createTempFile("bins", ".csv");

            // Write all bins to csv
            try (BufferedWriter bw = Files.newBufferedWriter(tmpPath)) {
                String header = String.join(",", "topic", "device", "timestamp", "count");
                bw.write(header);
                bw.write('\n');

                for (Map.Entry<Bin, LongAdder> entry : bins.entrySet()) {
                    Bin bin = entry.getKey();
                    bw.write(bin.topic);
                    bw.write(',');
                    bw.write(bin.category);
                    bw.write(',');
                    bw.write(bin.time);
                    bw.write(',');
                    bw.write(String.valueOf(entry.getValue().sum()));
                    bw.write('\n');
                }
            }

            storage.store(tmpPath, path);
        } catch (IOException e) {
            logger.error("Failed to write bins", e);
        }
    }

    public static class Bin {
        private final String topic;
        private final String category;
        private final String time;

        public Bin(@Nonnull String topic, @Nonnull String category, @Nonnull String time) {
            this.topic = topic;
            this.category = category;
            this.time = time;
        }

        @Override
        public String toString() {
            return topic + '|' + category + '|' + time;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Bin bin = (Bin) o;
            return topic.equals(bin.topic) &&
                    category.equals(bin.category) &&
                    time.equals(bin.time);
        }

        @Override
        public int hashCode() {
            return Objects.hash(topic, category, time);
        }
    }
}
