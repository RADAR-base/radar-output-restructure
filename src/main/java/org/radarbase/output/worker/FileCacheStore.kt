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

package org.radarbase.output.worker

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.radarbase.output.FileStoreFactory
import org.radarbase.output.accounting.Accountant
import org.radarbase.output.util.TemporaryDirectory
import org.radarbase.output.util.Timer.time
import org.radarbase.output.worker.FileCacheStore.WriteResponse.NO_CACHE_AND_NO_WRITE
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.io.Flushable
import java.io.IOException
import java.nio.file.Path
import kotlin.io.path.createTempFile
import kotlin.io.path.outputStream

/**
 * Caches open file handles. If more than the limit is cached, the half of the files that were used
 * the longest ago cache are evicted from cache.
 */
class FileCacheStore @Throws(IOException::class)
constructor(private val factory: FileStoreFactory, private val accountant: Accountant) : Flushable, Closeable {
    private val tmpDir: TemporaryDirectory

    private val caches: MutableMap<Path, FileCache>
    private val maxCacheSize: Int
    private val schemasAdded: MutableMap<Path, Path>

    init {
        val config = factory.config
        this.maxCacheSize = config.worker.cacheSize
        this.caches = HashMap(maxCacheSize * 4 / 3 + 1)
        this.tmpDir = TemporaryDirectory(config.paths.temp, "file-cache-")
        this.schemasAdded = HashMap()
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
    @Throws(IOException::class)
    fun writeRecord(path: Path, record: GenericRecord, transaction: Accountant.Transaction): WriteResponse {
        val existingCache: FileCache? = caches[path]
        val fileCache = if (existingCache != null) {
            existingCache
        } else {
            ensureCapacity()

            val dir = path.parent
            factory.targetStorage.createDirectories(dir)

            try {
                time("write.open") { FileCache(factory, transaction.topicPartition.topic, path, record, tmpDir.path, accountant) }
                        .also {
                            writeSchema(transaction.topicPartition.topic, path, record.schema)
                            caches[path] = it
                        }
            } catch (ex: IOException) {
                logger.error("Could not open cache for {}", path, ex)
                return NO_CACHE_AND_NO_WRITE
            }
        }

        return try {
            WriteResponse.valueOf(
                    isCacheHit = existingCache != null,
                    isSuccessful = fileCache.writeRecord(record, transaction))
        } catch (ex: IOException) {
            logger.error("Failed to write record. Closing cache {}.", fileCache.path, ex)
            fileCache.markError()
            caches.remove(fileCache.path)
            fileCache.close()
            NO_CACHE_AND_NO_WRITE
        }
    }

    @Throws(IOException::class)
    private fun writeSchema(topic: String, path: Path, schema: Schema) = time("write.schema") {
        // Write was successful, finalize the write
        val schemaPath = path.resolveSibling("schema-$topic.json")
        // First check if we already checked this path, because otherwise the storage.exists call
        // will take too much time.
        if (schemasAdded.putIfAbsent(schemaPath, schemaPath) == null) {
            val storage = factory.targetStorage

            if (storage.status(schemaPath) == null) {
                val tmpSchemaPath = createTempFile(tmpDir.path, "schema-$topic", ".json")
                tmpSchemaPath.outputStream().use { out ->
                    out.write(schema.toString(true).toByteArray())
                }
                storage.store(tmpSchemaPath, schemaPath)
            }
        }
    }

    /**
     * Ensure that a new filecache can be added. Evict files used longest ago from cache if needed.
     */
    @Throws(IOException::class)
    private fun ensureCapacity() {
        if (caches.size == maxCacheSize) {
            val cacheList = ArrayList(caches.values)
                    .sorted()
            for (i in 0 until cacheList.size / 2) {
                val rmCache = cacheList[i]
                caches.remove(rmCache.path)
                rmCache.close()
            }
            accountant.flush()
        }
    }

    @Throws(IOException::class)
    override fun flush() {
        try {
            caches.values.parallelStream()
                    .forEach(FileCache::close)
            accountant.flush()
        } finally {
            caches.clear()
        }
    }

    @Throws(IOException::class)
    override fun close() {
        flush()
        tmpDir.close()
    }

    /**
     * Response codes for each write record case.
     */
    enum class WriteResponse(
            /** Whether the cache was used to write. */
            val isCacheHit: Boolean,
            /** Whether the write was successful. */
            val isSuccessful: Boolean
    ) {
        /** Cache hit and write was successful.  */
        CACHE_AND_WRITE(true, true),
        /** Cache hit and write was unsuccessful because of a mismatch in number of columns.  */
        CACHE_AND_NO_WRITE(true, false),
        /** Cache miss and write was successful.  */
        NO_CACHE_AND_WRITE(false, true),
        /** Cache miss and write was unsuccessful because of a mismatch in number of columns.  */
        NO_CACHE_AND_NO_WRITE(false, false);

        companion object {
            fun valueOf(isCacheHit: Boolean, isSuccessful: Boolean) = when {
                isSuccessful && isCacheHit -> CACHE_AND_WRITE
                isSuccessful -> NO_CACHE_AND_WRITE
                // The file path was not in cache but the file exists and this write is
                // unsuccessful because of different number of columns
                isCacheHit -> CACHE_AND_NO_WRITE
                else -> NO_CACHE_AND_NO_WRITE
            }
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(FileCacheStore::class.java)
    }
}
