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

package org.radarcns.util;

import org.apache.avro.generic.GenericRecord;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Caches open file handles. If more than the limit is cached, the half of the files that were used
 * the longest ago cache are evicted from cache.
 */
public class FileCacheStore implements Flushable, Closeable {
    private final boolean gzip;
    private final boolean deduplicate;

    private RecordConverterFactory converterFactory;
    private final int maxFiles;
    private final Map<Path, FileCache> caches;

    // Response codes for each write record case
    public static final int CACHE_AND_WRITE = 1; //used cache and write successful
    public static final int NO_CACHE_AND_WRITE= 2;
    public static final int CACHE_AND_NO_WRITE =3;
    public static final int NO_CACHE_AND_NO_WRITE =4;


    public FileCacheStore(RecordConverterFactory converterFactory, int maxFiles, boolean gzip, boolean deduplicate) {
        this.converterFactory = converterFactory;
        this.maxFiles = maxFiles;
        this.caches = new HashMap<>(maxFiles * 4 / 3 + 1);
        this.gzip = gzip;
        this.deduplicate = deduplicate;
    }

    /**
     * Append a record to given file. If the file handle and writer are already open in this cache,
     * those will be used. Otherwise, the file will be opened and the file handle cached.
     *
     * @param path file to append data to
     * @param record data
     * @return Integer value according to one of the response codes.
     * @throws IOException when failing to open a file or writing to it.
     */
    public int writeRecord(Path path, GenericRecord record) throws IOException {
        FileCache cache = caches.get(path);
        if (cache != null) {
            if(cache.writeRecord(record)){
                return CACHE_AND_WRITE;
            } else {
                // This is the case when cache is used but write is unsuccessful
                // because of different number columns in same topic
                return CACHE_AND_NO_WRITE;
            }
        } else {
            ensureCapacity();

            Path dir = path.getParent();
            Files.createDirectories(dir);

            cache = new FileCache(converterFactory, path, record, gzip);
            caches.put(path, cache);
            if(cache.writeRecord(record)) {
                return NO_CACHE_AND_WRITE;
            } else {
                // The file path was not in cache but the file exists and this write is
                // unsuccessful because of different number of columns
                return NO_CACHE_AND_NO_WRITE;
            }

        }
    }

    /**
     * Ensure that a new filecache can be added. Evict files used longest ago from cache if needed.
     */
    private void ensureCapacity() throws IOException {
        if (caches.size() == maxFiles) {
            ArrayList<FileCache> cacheList = new ArrayList<>(caches.values());
            Collections.sort(cacheList);
            for (int i = 0; i < cacheList.size() / 2; i++) {
                FileCache rmCache = cacheList.get(i);
                caches.remove(rmCache.getPath());
                rmCache.close();
                if (deduplicate) {
                    converterFactory.sortUnique(rmCache.getPath());
                }
            }
        }
    }

    @Override
    public void flush() throws IOException {
        for (FileCache cache : caches.values()) {
            cache.flush();
        }
    }

    @Override
    public void close() throws IOException {
        try {
            for (FileCache cache : caches.values()) {
                cache.close();
                if (deduplicate) {
                    converterFactory.sortUnique(cache.getPath());
                }
            }
        } finally {
            caches.clear();
        }
    }

}
