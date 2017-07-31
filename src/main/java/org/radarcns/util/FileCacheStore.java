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

import java.io.Closeable;
import java.io.File;
import java.io.Flushable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Caches open file handles. If more than the limit is cached, the half of the files that were used
 * the longest ago cache are evicted from cache.
 */
public class FileCacheStore implements Flushable, Closeable {
    private static final Logger logger = LoggerFactory.getLogger(FileCacheStore.class);
    private final boolean gzip;

    private RecordConverterFactory converterFactory;
    private final int maxFiles;
    private final Map<File, FileCache> caches;

    public FileCacheStore(RecordConverterFactory converterFactory, int maxFiles, boolean gzip) {
        this.converterFactory = converterFactory;
        this.maxFiles = maxFiles;
        this.caches = new HashMap<>(maxFiles * 4 / 3 + 1);
        this.gzip = gzip;
    }

    /**
     * Append a record to given file. If the file handle and writer are already open in this cache,
     * those will be used. Otherwise, the file will be opened and the file handle cached.
     *
     * @param file file to append data to
     * @param record data
     * @return true if the cache was used, false if a new file was opened.
     * @throws IOException when failing to open a file or writing to it.
     */
    public boolean writeRecord(File file, GenericRecord record) throws IOException {
        FileCache cache = caches.get(file);
        if (cache != null) {
            cache.writeRecord(record);
            return true;
        } else {
            ensureCapacity();

            File dir = file.getParentFile();
            if (!dir.exists()){
                if (dir.mkdirs()) {
                    logger.debug("Created directory: {}", dir.getAbsolutePath());
                } else {
                    logger.warn("FAILED to create directory: {}", dir.getAbsolutePath());
                }
            }

            cache = new FileCache(converterFactory, file, record, gzip);
            caches.put(file, cache);
            cache.writeRecord(record);
            return false;
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
                caches.remove(rmCache.getFile());
                rmCache.close();
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
            }
        } finally {
            caches.clear();
        }
    }

}
