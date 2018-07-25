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

package org.radarcns.hdfs.data;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.radarcns.hdfs.FileStoreFactory;
import org.radarcns.hdfs.accounting.Accountant;
import org.radarcns.hdfs.config.RestructureSettings;
import org.radarcns.hdfs.util.TemporaryDirectory;
import org.radarcns.hdfs.util.ThrowingConsumer;
import org.radarcns.hdfs.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.radarcns.hdfs.data.FileCacheStore.WriteResponse.CACHE_AND_NO_WRITE;
import static org.radarcns.hdfs.data.FileCacheStore.WriteResponse.CACHE_AND_WRITE;
import static org.radarcns.hdfs.data.FileCacheStore.WriteResponse.NO_CACHE_AND_NO_WRITE;
import static org.radarcns.hdfs.data.FileCacheStore.WriteResponse.NO_CACHE_AND_WRITE;
import static org.radarcns.hdfs.util.ThrowingConsumer.tryCatch;

/**
 * Caches open file handles. If more than the limit is cached, the half of the files that were used
 * the longest ago cache are evicted from cache.
 */
public class FileCacheStore implements Flushable, Closeable {
    private static final Logger logger = LoggerFactory.getLogger(FileCacheStore.class);

    private final TemporaryDirectory tmpDir;

    private final Map<Path, FileCache> caches;
    private final FileStoreFactory factory;
    private final int maxCacheSize;
    private final Accountant accountant;
    private final Map<java.nio.file.Path, java.nio.file.Path> schemasAdded;

    public FileCacheStore(FileStoreFactory factory, Accountant accountant) throws IOException {
        this.factory = factory;
        RestructureSettings settings = factory.getSettings();
        this.maxCacheSize = settings.getCacheSize();
        this.caches = new HashMap<>(maxCacheSize * 4 / 3 + 1);
        this.tmpDir = new TemporaryDirectory(settings.getTempDir(), "file-cache-");
        this.accountant = accountant;
        this.schemasAdded = new HashMap<>();
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
    public WriteResponse writeRecord(String topic, Path path, GenericRecord record,
            Accountant.Transaction transaction) throws IOException {
        FileCache cache = caches.get(path);
        boolean hasCache = cache != null;
        if (!hasCache) {
            ensureCapacity();

            Path dir = path.getParent();
            Files.createDirectories(dir);

            try {
                long timeOpen = System.nanoTime();
                cache = new FileCache(factory, path, record, tmpDir.getPath(), accountant);
                Timer.getInstance().add("write.open", timeOpen);
                writeSchema(topic, path, record.getSchema());
            } catch (IOException ex) {
                logger.error("Could not open cache for {}", path, ex);
                return NO_CACHE_AND_NO_WRITE;
            }
            caches.put(path, cache);
        }

        try {
            if (cache.writeRecord(record, transaction)) {
                return hasCache ? CACHE_AND_WRITE : NO_CACHE_AND_WRITE;
            } else {
                // The file path was not in cache but the file exists and this write is
                // unsuccessful because of different number of columns
                return hasCache ? CACHE_AND_NO_WRITE : NO_CACHE_AND_NO_WRITE;
            }
        } catch (IOException ex) {
            logger.error("Failed to write record. Closing cache {}.", cache.getPath(), ex);
            cache.markError();
            caches.remove(cache.getPath());
            cache.close();
            return NO_CACHE_AND_NO_WRITE;
        }
    }

    private void writeSchema(String topic, Path path, Schema schema) throws IOException {
        long writeSchema = System.nanoTime();
        // Write was successful, finalize the write
        java.nio.file.Path schemaPath = path.resolveSibling("schema-" + topic + ".json");
        // First check if we already checked this path, because otherwise the storage.exists call
        // will take too much time.
        if (schemasAdded.putIfAbsent(schemaPath, schemaPath) == null) {
            StorageDriver storage = factory.getStorageDriver();

            if (!storage.exists(schemaPath)) {
                try (OutputStream out = storage.newOutputStream(schemaPath, false);
                     Writer writer = new OutputStreamWriter(out)) {
                    writer.write(schema.toString(true));
                }
            }
        }
        Timer.getInstance().add("write.schema", writeSchema);
    }

    /**
     * Ensure that a new filecache can be added. Evict files used longest ago from cache if needed.
     */
    private void ensureCapacity() throws IOException {
        if (caches.size() == maxCacheSize) {
            ArrayList<FileCache> cacheList = new ArrayList<>(caches.values());
            Collections.sort(cacheList);
            for (int i = 0; i < cacheList.size() / 2; i++) {
                FileCache rmCache = cacheList.get(i);
                caches.remove(rmCache.getPath());
                rmCache.close();
            }
            accountant.flush();
        }
    }

    @Override
    public void flush() throws IOException {
        try {
            allCaches(FileCache::close);
            accountant.flush();
        } finally {
            caches.clear();
        }
    }

    @Override
    public void close() throws IOException {
        flush();
        tmpDir.close();
    }

    private void allCaches(ThrowingConsumer<FileCache> cacheHandler) throws IOException {
        try {
            caches.values().parallelStream()
                    .forEach(tryCatch(cacheHandler, "Failed to update caches."));
        } catch (IllegalStateException ex) {
            throw (IOException) ex.getCause();
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
