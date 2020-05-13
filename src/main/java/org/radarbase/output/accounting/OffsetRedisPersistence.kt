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
import redis.clients.jedis.exceptions.JedisException
import java.io.IOException
import java.nio.file.Path
import java.util.concurrent.TimeUnit

/**
 * Accesses a OffsetRange json object a Redis entry.
 */
class OffsetRedisPersistence(
        private val redisPool: JedisPool
) : OffsetPersistenceFactory {

    override fun read(path: Path): OffsetRangeSet? {
        return try {
            redisPool.resource.use { jedis ->
                jedis[path.toString()]?.let { value ->
                    OffsetRangeSet().apply {
                        addAll(offsetReader.readValue<OffsetRangeSet.OffsetRangeList>(value))
                    }
                }
            }
        } catch (ex: JedisException) {
            logger.error("Error reading offsets file. Processing all offsets.")
            null
        }
    }

    override fun writer(
            path: Path,
            startSet: OffsetRangeSet?
    ): OffsetPersistenceFactory.Writer = RedisWriter(path, startSet)

    companion object {
        private val logger = LoggerFactory.getLogger(OffsetRedisPersistence::class.java)
        private val mapper = jacksonObjectMapper()
        private val offsetWriter = mapper.writerFor(OffsetRangeSet.OffsetRangeList::class.java)
        private val offsetReader = mapper.readerFor(OffsetRangeSet.OffsetRangeList::class.java)
    }

    private inner class RedisWriter(
            private val path: Path,
            startSet: OffsetRangeSet?
    ) : PostponedWriter("offsets", 1, TimeUnit.SECONDS),
            OffsetPersistenceFactory.Writer {
        override val offsets: OffsetRangeSet = startSet ?: OffsetRangeSet()

        override fun doWrite(): Unit = time("accounting.offsets") {
            try {
                redisPool.resource.use { jedis ->
                    jedis.set(path.toString(), offsetWriter.writeValueAsString(offsets.toOffsetRangeList()))
                }
            } catch (e: IOException) {
                logger.error("Failed to write offsets: {}", e.toString())
            }
        }
    }
}
