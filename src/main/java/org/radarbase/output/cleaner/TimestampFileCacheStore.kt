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

package org.radarbase.output.cleaner

import org.apache.avro.generic.GenericRecord
import org.radarbase.output.FileStoreFactory
import org.radarbase.output.util.Timer.time
import java.io.FileNotFoundException
import java.io.IOException
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap

/**
 * Caches open file handles. If more than the limit is cached, the half of the files that were used
 * the longest ago cache are evicted from cache.
 */
class TimestampFileCacheStore(private val factory: FileStoreFactory) {
    private val caches: MutableMap<Path, TimestampFileCache>
    private val maxCacheSize: Int
    private val schemasAdded: MutableMap<Path, Path>

    init {
        val config = factory.config
        this.maxCacheSize = config.worker.cacheSize
        this.caches = ConcurrentHashMap(maxCacheSize * 4 / 3 + 1)
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
    suspend fun contains(path: Path, record: GenericRecord): FindResult {
        return try {
            val fileCache = caches[path]
                ?: time("cleaner.cache") {
                    ensureCapacity()
                    TimestampFileCache(factory, path).apply {
                        initialize()
                        caches[path] = this
                    }
                }

            time("cleaner.contains") {
                if (fileCache.contains(record)) FindResult.FOUND else FindResult.NOT_FOUND
            }
        } catch (ex: FileNotFoundException) {
            FindResult.FILE_NOT_FOUND
        } catch (ex: IllegalArgumentException) {
            FindResult.BAD_SCHEMA
        } catch (ex: IndexOutOfBoundsException) {
            FindResult.BAD_SCHEMA
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
                caches.remove(cacheList[i].path)
            }
        }
    }

    fun clear() {
        caches.clear()
    }

    enum class FindResult {
        FILE_NOT_FOUND,
        BAD_SCHEMA,
        NOT_FOUND,
        FOUND,
    }
}
