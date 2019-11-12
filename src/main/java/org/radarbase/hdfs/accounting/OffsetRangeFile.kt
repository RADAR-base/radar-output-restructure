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

package org.radarbase.hdfs.accounting

import org.radarbase.hdfs.data.StorageDriver
import org.radarbase.hdfs.util.PostponedWriter
import org.radarbase.hdfs.util.ThrowingConsumer.tryCatch
import org.radarbase.hdfs.util.Timer.time
import org.slf4j.LoggerFactory
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern

/**
 * Accesses a OffsetRange file using the CSV format. On construction, this will create the file if
 * not present.
 */
class OffsetRangeFile(private val storage: StorageDriver, private val path: Path, startSet: OffsetRangeSet?) : PostponedWriter("offsets", 1, TimeUnit.SECONDS) {
    val offsets: OffsetRangeSet = startSet ?: OffsetRangeSet()

    fun add(range: OffsetRange) = offsets.add(range)

    fun addAll(rangeSet: OffsetRangeSet) = offsets.addAll(rangeSet)

    override fun doWrite() = time("accounting.offsets") {
        try {
            val tmpPath = createTempFile("offsets", ".csv")

            Files.newBufferedWriter(tmpPath).use { write ->
                write.append("offsetFrom,offsetTo,partition,topic\n")
                offsets.stream()
                        .forEach(tryCatch({ r ->
                            write.write(r.offsetFrom.toString())
                            write.write(','.toInt())
                            write.write(r.offsetTo.toString())
                            write.write(','.toInt())
                            write.write(r.partition.toString())
                            write.write(','.toInt())
                            write.write(r.topic)
                            write.write('\n'.toInt())
                        }, "Failed to write value"))
            }

            storage.store(tmpPath, path)
        } catch (e: IOException) {
            logger.error("Failed to write offsets: {}", e.toString())
        }
    }

    companion object {
        private val COMMA_PATTERN: Pattern = Pattern.compile(",")
        private val logger = LoggerFactory.getLogger(OffsetRangeFile::class.java)

        fun read(storage: StorageDriver, path: Path): OffsetRangeFile {
            try {
                if (storage.exists(path)) {
                    val set = OffsetRangeSet()
                    storage.newBufferedReader(path).use { br ->
                        // ignore header
                        br.readLine() ?: return@use

                        generateSequence { br.readLine() }
                                .forEach { line ->
                                    val cols = COMMA_PATTERN.split(line)
                                    var topic = cols[3]
                                    while (topic[0] == '"') {
                                        topic = topic.substring(1)
                                    }
                                    while (topic[topic.length - 1] == '"') {
                                        topic = topic.substring(0, topic.length - 1)
                                    }
                                    set.add(OffsetRange(
                                            topic,
                                            Integer.parseInt(cols[2]),
                                            java.lang.Long.parseLong(cols[0]),
                                            java.lang.Long.parseLong(cols[1])))
                                }
                    }
                    return OffsetRangeFile(storage, path, set)
                } else {
                    return OffsetRangeFile(storage, path, null)
                }
            } catch (ex: IOException) {
                logger.error("Error reading offsets file. Processing all offsets.")
                return OffsetRangeFile(storage, path, null)
            }

        }
    }
}
