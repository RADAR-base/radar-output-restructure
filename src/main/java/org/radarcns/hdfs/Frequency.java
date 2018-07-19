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
import org.radarcns.hdfs.data.StorageDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;


public class Frequency {
    private static final Logger logger = LoggerFactory.getLogger(Frequency.class);

    private final MultiKeyMap bins;
    private final Path path;
    private final StorageDriver storage;

    public Frequency(@Nonnull StorageDriver storageDriver, @Nonnull Path path, @Nonnull MultiKeyMap initialData) {
        Objects.requireNonNull(path);
        Objects.requireNonNull(initialData);
        this.storage = storageDriver;
        this.path = path;
        this.bins = initialData;
    }

    public static Frequency read(StorageDriver storage, Path path) {
        MultiKeyMap map = new MultiKeyMap();
        try (BufferedReader input = storage.newBufferedReader(path)){
            // Read in all lines as multikeymap (key, key, key, value)
            String line = input.readLine();
            if (line != null) {
                line = input.readLine();
                while (line != null) {
                    String[] columns = line.split(",");
                    try {
                        map.put(columns[0], columns[1], columns[2], Integer.valueOf(columns[3]));
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

    public synchronized void add(String topicName, String id, String timeBin) {
        Integer count = (Integer) bins.get(topicName, id, timeBin);
        if (count == null) {
            bins.put(topicName, id, timeBin, 1);
        } else {
            bins.put(topicName, id, timeBin, count + 1);
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

    public synchronized void write() {
        // Write all bins to csv
        MapIterator mapIterator = bins.mapIterator();
        try (BufferedWriter bw = storage.newBufferedWriter(path, false)) {
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
