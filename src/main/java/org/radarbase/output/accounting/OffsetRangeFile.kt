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

import org.radarbase.output.storage.StorageDriver
import org.slf4j.LoggerFactory
import java.io.IOException
import java.nio.file.Path
import java.util.regex.Pattern

/**
 * Accesses a OffsetRange file using the CSV format. On construction, this will create the file if
 * not present.
 */
class OffsetRangeFile(
        private val storage: StorageDriver
) {
    fun read(path: Path): OffsetRangeSet? {
        return try {
            if (storage.status(path) != null) {
                OffsetRangeSet().also { set ->
                    storage.newBufferedReader(path).use { br ->
                        // ignore header
                        br.readLine() ?: return@use

                        generateSequence { br.readLine() }
                                .map(::readLine)
                                .forEach(set::add)
                    }
                }
            } else null
        } catch (ex: IOException) {
            logger.error("Error reading offsets file. Processing all offsets.")
            null
        }
    }

    private fun readLine(line: String): OffsetRange {
        val cols = COMMA_PATTERN.split(line)
        var topic = cols[3]
        while (topic[0] == '"') {
            topic = topic.substring(1)
        }
        while (topic[topic.length - 1] == '"') {
            topic = topic.substring(0, topic.length - 1)
        }
        return OffsetRange(
                topic,
                cols[2].toInt(),
                cols[0].toLong(),
                cols[1].toLong())
    }

    companion object {
        private val COMMA_PATTERN: Pattern = Pattern.compile(",")
        private val logger = LoggerFactory.getLogger(OffsetRangeFile::class.java)
    }
}
