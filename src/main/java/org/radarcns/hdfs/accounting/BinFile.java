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
import java.util.stream.Collectors;

/** Store overview of records written, divided into bins. */
public class BinFile extends PostponedWriter {
    private static final Logger logger = LoggerFactory.getLogger(BinFile.class);

    private final ConcurrentMap<Bin, LongAdder> bins;
    private final Path path;
    private final StorageDriver storage;

    public BinFile(@Nonnull StorageDriver storageDriver, @Nonnull Path path,
            @Nonnull ConcurrentMap<Bin, LongAdder> initialData) {
        super("bins", 5, TimeUnit.SECONDS);
        Objects.requireNonNull(path);
        Objects.requireNonNull(initialData);
        this.storage = storageDriver;
        this.path = path;
        this.bins = initialData;
    }

    public static BinFile read(StorageDriver storage, Path path) {
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
        return new BinFile(storage, path, map);
    }

    /** Add number of instances to given bin. */
    public void add(Bin bin, long value) {
        bins.computeIfAbsent(bin, b -> new LongAdder()).add(value);
    }

    /** Put a map of bins. */
    public void putAll(Map<? extends Bin, ? extends Number> binMap) {
        binMap.forEach((bin, v) -> add(bin, v.longValue()));
    }

    @Override
    public String toString() {
        return bins.entrySet().stream()
                .map(e -> e.getKey() + " - " + e.getValue().sum())
                .collect(Collectors.joining("\n"));
    }

    @Override
    protected void doWrite() {
        try {
            Path tempPath = createTempFile("bins", ".csv");

            // Write all bins to csv
            try (BufferedWriter bw = Files.newBufferedWriter(tempPath)) {
                String header = String.join(",", "topic", "device", "timestamp", "count");
                bw.write(header);
                bw.write('\n');

                for (Map.Entry<Bin, LongAdder> entry : bins.entrySet()) {
                    Bin bin = entry.getKey();
                    bw.write(bin.getTopic());
                    bw.write(',');
                    bw.write(bin.getCategory());
                    bw.write(',');
                    bw.write(bin.getTime());
                    bw.write(',');
                    bw.write(String.valueOf(entry.getValue().sum()));
                    bw.write('\n');
                }
            }

            storage.store(tempPath, path);
        } catch (IOException e) {
            logger.error("Failed to write bins", e);
        }
    }

}
