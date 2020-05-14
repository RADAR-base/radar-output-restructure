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

package org.radarbase.output.accounting

import org.radarbase.output.target.TargetStorage
import org.radarbase.output.util.PostponedWriter
import org.radarbase.output.util.Timer.time
import org.slf4j.LoggerFactory
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.time.Instant
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern

/**
 * Accesses a OffsetRange file using the CSV format. On writing, this will create the file if
 * not present.
 */
class OffsetFilePersistence(
        private val targetStorage: TargetStorage
): OffsetPersistenceFactory {
    override fun read(path: Path): OffsetRangeSet? {
        return try {
            if (targetStorage.status(path) != null) {
                OffsetRangeSet().also { set ->
                    targetStorage.newBufferedReader(path).use { br ->
                        // ignore header
                        br.readLine() ?: return@use

                        generateSequence { br.readLine() }
                                .map(::parseLine)
                                .forEach(set::add)
                    }
                }
            } else null
        } catch (ex: IOException) {
            logger.error("Error reading offsets file. Processing all offsets.", ex)
            null
        }
    }

    override fun writer(
            path: Path,
            startSet: OffsetRangeSet?
    ): OffsetPersistenceFactory.Writer = FileWriter(path, startSet)

    private fun parseLine(line: String): TopicPartitionOffsetRange {
        val cols = COMMA_PATTERN.split(line)
        var topic = cols[3]
        while (topic[0] == '"') {
            topic = topic.substring(1)
        }
        while (topic[topic.length - 1] == '"') {
            topic = topic.substring(0, topic.length - 1)
        }
        val lastModified = if (cols.size >= 5) {
            Instant.parse(cols[4])
        } else Instant.now()

        return TopicPartitionOffsetRange(
                topic,
                cols[2].toInt(),
                cols[0].toLong(),
                cols[1].toLong(),
                lastModified)
    }

    companion object {
        private val COMMA_PATTERN: Pattern = Pattern.compile(",")
        private val logger = LoggerFactory.getLogger(OffsetFilePersistence::class.java)
    }

    private inner class FileWriter(
            private val path: Path,
            startSet: OffsetRangeSet?
    ): PostponedWriter("offsets", 1, TimeUnit.SECONDS),
            OffsetPersistenceFactory.Writer {
        override val offsets: OffsetRangeSet = startSet ?: OffsetRangeSet()

        override fun doWrite() = time("accounting.offsets") {
            try {
                val tmpPath = createTempFile("offsets", ".csv").toPath()

                Files.newBufferedWriter(tmpPath).use { writer ->
                    writer.append("offsetFrom,offsetTo,partition,topic\n")
                    offsets.forEach { topicPartition, offsetIntervals ->
                        offsetIntervals.forEach { offsetFrom, offsetTo, lastModified ->
                            writer.write(offsetFrom.toString())
                            writer.write(','.toInt())
                            writer.write(offsetTo.toString())
                            writer.write(','.toInt())
                            writer.write(topicPartition.partition.toString())
                            writer.write(','.toInt())
                            writer.write(topicPartition.topic)
                            writer.write(','.toInt())
                            writer.write(lastModified.toString())
                            writer.write('\n'.toInt())
                        }
                    }
                }

                targetStorage.store(tmpPath, path)
            } catch (e: IOException) {
                logger.error("Failed to write offsets: {}", e.toString())
            }
        }
    }
}
