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

import org.apache.commons.collections.MapIterator;
import org.apache.commons.collections.keyvalue.MultiKey;
import org.apache.commons.collections.map.MultiKeyMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;
import java.util.List;
import java.util.Objects;


public class Frequency {
    private static final Logger logger = LoggerFactory.getLogger(Frequency.class);

    private final MultiKeyMap bins;
    private final Path path;

    public Frequency(@Nonnull Path path, @Nonnull MultiKeyMap initialData) {
        Objects.requireNonNull(path);
        Objects.requireNonNull(initialData);
        this.path = path;
        this.bins = initialData;
    }

    public static Frequency read(Path path) {
        MultiKeyMap map = new MultiKeyMap();
        try {
            // Read in all lines as multikeymap (key, key, key, value)
            List<String> lines = Files.readAllLines(path);
            lines.subList(1, lines.size()).forEach(line -> {
                String[] columns = line.split(",");
                try {
                    map.put(columns[0], columns[1], columns[2], Integer.valueOf(columns[3]));
                } catch (ArrayIndexOutOfBoundsException ex) {
                    logger.warn("Unable to read row of the bins file. Skipping.");
                }
            });
        } catch (IOException e) {
            logger.warn("Could not read the file with bins. Creating new file when writing.");
        }
        return new Frequency(path, map);
    }

    public void add(String topicName, String id, Date date) {
        String timestamp = RestructureAvroRecords.createHourTimestamp(date);

        Integer count = (Integer) bins.get(topicName, id, timestamp);
        if (count == null) {
            bins.put(topicName, id, timestamp, 1);
        } else {
            bins.put(topicName, id, timestamp, count + 1);
        }
    }

    public void print() {
        MapIterator mapIterator = bins.mapIterator();

        while (mapIterator.hasNext()) {
            MultiKey key = (MultiKey) mapIterator.next();
            Integer value = (Integer) mapIterator.getValue();
            System.out.printf("%s|%s|%s - %d\n", key.getKey(0), key.getKey(1), key.getKey(2), value);
        }
    }

    public void write() {
        // Write all bins to csv
        MapIterator mapIterator = bins.mapIterator();
        try (BufferedWriter bw = Files.newBufferedWriter(path)) {
            String header = String.join(",","topic","device","timestamp","count");
            bw.write(header);
            bw.write('\n');

            while (mapIterator.hasNext()) {
                MultiKey key = (MultiKey) mapIterator.next();
                Integer value = (Integer) mapIterator.getValue();
                String data = String.join(",", key.getKey(0).toString(), key.getKey(1).toString(), key.getKey(2).toString(), value.toString());
                bw.write(data);
                bw.write('\n');
            }
        } catch (IOException e) {
            logger.error("Failed to write bins", e);
        }
    }
}
