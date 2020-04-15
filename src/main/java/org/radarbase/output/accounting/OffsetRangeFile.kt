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

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.radarbase.output.util.PostponedWriter
import org.radarbase.output.util.Timer.time
import org.slf4j.LoggerFactory
import redis.clients.jedis.JedisPool
import java.io.IOException
import java.nio.file.Path
import java.util.concurrent.TimeUnit

/**
 * Accesses a OffsetRange file using the CSV format. On construction, this will create the file if
 * not present.
 */
class OffsetRangeFile(
        private val jedisPool: JedisPool,
        private val path: Path,
        startSet: OffsetRangeSet?) : PostponedWriter("offsets", 1, TimeUnit.SECONDS) {

    val offsets: OffsetRangeSet = startSet ?: OffsetRangeSet()

    fun add(range: OffsetRange) = offsets.add(range)

    fun addAll(rangeSet: OffsetRangeSet) = offsets.addAll(rangeSet)

    override fun doWrite(): Unit = time("accounting.offsets") {
        try {
            jedisPool.resource.use { jedis ->
                jedis.set(path.toString(), offsetWriter.writeValueAsString(offsets.toOffsetRangeList()))
            }
        } catch (e: IOException) {
            logger.error("Failed to write offsets: {}", e.toString())
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(OffsetRangeFile::class.java)

        fun read(jedisPool: JedisPool, path: Path): OffsetRangeFile {
            val startSet = try {
                jedisPool.resource.use { jedis ->
                    jedis[path.toString()]?.let { value ->
                        OffsetRangeSet().apply {
                            addAll(offsetReader.readValue<OffsetRangeSet.OffsetRangeList>(value))
                        }
                    }
                }
            } catch (ex: IOException) {
                logger.error("Error reading offsets file. Processing all offsets.")
                null
            }
            return OffsetRangeFile(jedisPool, path, startSet)
        }

        private val mapper = jacksonObjectMapper()
        private val offsetWriter = mapper.writerFor(OffsetRangeSet.OffsetRangeList::class.java)
        private val offsetReader = mapper.readerFor(OffsetRangeSet.OffsetRangeList::class.java)
    }
}
