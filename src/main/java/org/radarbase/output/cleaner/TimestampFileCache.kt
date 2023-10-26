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
import org.radarbase.output.format.RecordConverterFactory
import org.radarbase.output.path.TargetPath
import org.radarbase.output.util.SuspendedCloseable.Companion.useSuspended
import org.radarbase.output.util.TimeUtil.getDate
import org.radarbase.output.util.TimeUtil.toDouble
import java.io.FileNotFoundException

/** Keeps path handles of a path.  */
class TimestampFileCache(
    private val factory: FileStoreFactory,
    /** File that the cache is maintaining.  */
    val targetPath: TargetPath,
) : Comparable<TimestampFileCache> {
    private val targetStorage = factory.targetManager[targetPath]
    private val path = targetPath.path
    private val converterFactory: RecordConverterFactory = factory.recordConverter
    private var lastUse: Long = 0
    private var header: Array<String>? = null
    private lateinit var times: Set<Double>

    suspend fun initialize() {
        targetStorage.status(path)
            ?.takeIf { it.size > 0 }
            ?: throw FileNotFoundException("Path $path not found on target $targetStorage")

        val readDates = targetStorage.newInputStream(path).useSuspended {
            converterFactory.readTimeSeconds(it, factory.compression)
        } ?: throw FileNotFoundException()

        header = readDates.first
        times = readDates.second.toSet()
    }

    fun contains(record: GenericRecord): Boolean {
        if (header != null) {
            val recordHeader = converterFactory.headerFor(record)
            require(recordHeader.contentEquals(header)) {
                "Header mismatch: record header ${recordHeader.contentToString()}" +
                    " does not match target header ${header.contentToString()}"
            }
        }

        val recordDate = getDate(
            record.get("key") as? GenericRecord,
            record.get("value") as? GenericRecord,
        )?.toDouble()

        return recordDate == null || recordDate in times
    }

    /**
     * Compares time that the filecaches were last used. If equal, it lexicographically compares
     * the absolute path of the path.
     * @param other FileCache to compare with.
     */
    override fun compareTo(other: TimestampFileCache): Int = comparator.compare(this, other)

    companion object {
        val comparator = compareBy(TimestampFileCache::lastUse, TimestampFileCache::targetPath)
    }
}
