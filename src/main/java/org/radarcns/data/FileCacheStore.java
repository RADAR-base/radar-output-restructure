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

package org.radarcns.data;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import static org.radarcns.util.ThrowingConsumer.tryCatch;

/**
 * Caches open file handles. If more than the limit is cached, the half of the files that were used
 * the longest ago cache are evicted from cache.
 */
public class FileCacheStore implements Flushable, Closeable {
    private static final Logger logger = LoggerFactory.getLogger(FileCacheStore.class);

    private final boolean gzip;
    private final boolean deduplicate;
    private final Path tmpDir;

    private RecordConverterFactory converterFactory;
    private final int maxFiles;
    private final Map<Path, FileCache> caches;

    public FileCacheStore(RecordConverterFactory converterFactory, int maxFiles, boolean gzip, boolean deduplicate, boolean stage) throws IOException {
        this.converterFactory = converterFactory;
        this.maxFiles = maxFiles;
        this.caches = new HashMap<>(maxFiles * 4 / 3 + 1);
        this.gzip = gzip;
        this.deduplicate = deduplicate;
        this.tmpDir = stage ? Files.createTempDirectory("restructurehdfs") : null;
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
    public WriteResponse writeRecord(Path path, GenericRecord record) throws IOException {
        FileCache cache = caches.get(path);
        if (cache != null) {
            if(cache.writeRecord(record)){
                return WriteResponse.CACHE_AND_WRITE;
            } else {
                // This is the case when cache is used but write is unsuccessful
                // because of different number columns in same topic
                return WriteResponse.CACHE_AND_NO_WRITE;
            }
        } else {
            ensureCapacity();

            Path dir = path.getParent();
            Files.createDirectories(dir);

            cache = new FileCache(converterFactory, path, record, gzip, tmpDir);
            caches.put(path, cache);
            if (cache.writeRecord(record)) {
                return WriteResponse.NO_CACHE_AND_WRITE;
            } else {
                // The file path was not in cache but the file exists and this write is
                // unsuccessful because of different number of columns
                return WriteResponse.NO_CACHE_AND_NO_WRITE;
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
            if (tmpDir != null) {
                Files.walk(tmpDir)
                        .sorted(Comparator.reverseOrder())
                        .forEach(tryCatch(Files::delete, (p, ex) -> logger.warn(
                                "Failed to remove temporary file {}: {}", p, ex)));
            }
        } finally {
            caches.clear();
        }
    }

    // Response codes for each write record case
    public enum WriteResponse {
        /** Cache hit and write was successful. */
        CACHE_AND_WRITE(true, true),
        /** Cache hit and write was unsuccessful because of a mismatch in number of columns. */
        CACHE_AND_NO_WRITE(true, false),
        /** Cache miss and write was successful. */
        NO_CACHE_AND_WRITE(false, true),
        /** Cache miss and write was unsuccessful because of a mismatch in number of columns. */
        NO_CACHE_AND_NO_WRITE(false, false);

        private final boolean successful;
        private final boolean cacheHit;

        /**
         * Write status.
         * @param cacheHit whether the cache was used to write.
         * @param successful whether the write was successful.
         */
        WriteResponse(boolean cacheHit, boolean successful) {
            this.cacheHit = cacheHit;
            this.successful = successful;
        }

        public boolean isSuccessful() {
            return successful;
        }

        public boolean isCacheHit() {
            return cacheHit;
        }
    }
}
