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

import static org.radarcns.hdfs.accounting.OffsetRangeFile.COMMA_PATTERN;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.radarcns.hdfs.data.StorageDriver;
import org.radarcns.hdfs.util.PostponedWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Store overview of records written, divided into bins. */
public class BinFile extends PostponedWriter {
    private static final Logger logger = LoggerFactory.getLogger(BinFile.class);
    private static final String BINS_HEADER = String.join(
            ",", "topic", "device", "timestamp", "count") + "\n";

    private final ConcurrentMap<Bin, Long> bins;
    private final Path path;
    private final StorageDriver storage;

    public BinFile(@Nonnull StorageDriver storageDriver, @Nonnull Path path) {
        super("bins", 5, TimeUnit.SECONDS);
        Objects.requireNonNull(path);
        this.storage = storageDriver;
        this.path = path;
        this.bins = new ConcurrentHashMap<>();
    }

    /** Add number of instances to given bin. */
    public void add(Bin bin, long value) {
        bins.compute(bin, compute(() -> 0L, v -> v + value));
    }

    /** Put a map of bins. */
    public void putAll(Map<? extends Bin, ? extends Number> binMap) {
        binMap.forEach((bin, v) -> add(bin, v.longValue()));
    }

    @Override
    public String toString() {
        return bins.entrySet().stream()
                .map(e -> e.getKey() + " - " + e.getValue())
                .collect(Collectors.joining("\n"));
    }

    @Override
    protected void doWrite() {
        Path tempPath;
        try {
            tempPath = createTempFile("bins", ".csv");
        } catch (IOException e) {
            logger.error("Cannot create temporary bins file: {}", e.toString());
            return;
        }

        BufferedReader reader = null;
        Stream<String[]> lines;

        try {
            reader = storage.newBufferedReader(path);

            if (reader.readLine() == null) {
                lines = Stream.empty();
            } else {
                lines = reader.lines()
                        .map(COMMA_PATTERN::split);
            }
        } catch (IOException ex){
            logger.warn("Could not read the file with bins. Creating new file when writing.");
            lines = Stream.empty();
        }

        Set<Bin> binKeys = new HashSet<>(bins.keySet());

        try (BufferedWriter bw = Files.newBufferedWriter(tempPath)) {
            bw.write(BINS_HEADER);

            lines.forEach(s -> {
                Bin bin = new Bin(s[0], s[1], s[2]);
                long value = Long.parseLong(s[3]);
                if (binKeys.remove(bin)) {
                    value += bins.remove(bin);
                }
                writeLine(bw, bin, value);
            });
            binKeys.forEach(bin -> writeLine(bw, bin, bins.remove(bin)));

            storage.store(tempPath, path);
        } catch (UncheckedIOException | IOException e) {
            logger.error("Failed to write bins: {}", e);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    logger.debug("Failed to close bin file reader", e);
                }
            }
        }
    }

    private static void writeLine(BufferedWriter writer, Bin bin, long value) {
        try {
            writer.write(bin.getTopic());
            writer.write(',');
            writer.write(bin.getCategory());
            writer.write(',');
            writer.write(bin.getTime());
            writer.write(',');
            writer.write(Long.toString(value));
            writer.write('\n');
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static <K, V1, V2> BiFunction<K, V1, V2> compute(Supplier<? extends V1> init, Function<? super V1, ? extends V2> update) {
        return (k, v) -> {
            if (v == null) {
                v = init.get();
            }
            return update.apply(v);
        };
    }
}
