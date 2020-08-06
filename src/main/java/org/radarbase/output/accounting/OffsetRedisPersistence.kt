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

import com.fasterxml.jackson.databind.ObjectReader
import com.fasterxml.jackson.databind.ObjectWriter
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.radarbase.output.util.PostponedWriter
import org.radarbase.output.util.Timer.time
import org.slf4j.LoggerFactory
import java.io.IOException
import java.nio.file.Path
import java.util.concurrent.TimeUnit

/**
 * Accesses a OffsetRange json object a Redis entry.
 */
class OffsetRedisPersistence(
        private val redisHolder: RedisHolder
) : OffsetPersistenceFactory {

    override fun read(path: Path): OffsetRangeSet? {
        return try {
            redisHolder.execute { redis ->
                redis[path.toString()]?.let { value ->
                    redisOffsetReader.readValue<RedisOffsetRangeSet>(value)
                            .partitions
                            .fold(OffsetRangeSet(), { set, (topic, partition, ranges) ->
                                set.apply { addAll(TopicPartition(topic, partition), ranges) }
                            })
                }
            }
        } catch (ex: IOException) {
            logger.error("Error reading offsets from Redis: {}. Processing all offsets.", ex.toString())
            null
        }
    }

    override fun writer(
            path: Path,
            startSet: OffsetRangeSet?
    ): OffsetPersistenceFactory.Writer = RedisWriter(path, startSet)

    private inner class RedisWriter(
            private val path: Path,
            startSet: OffsetRangeSet?
    ) : PostponedWriter("offsets", 1, TimeUnit.SECONDS),
            OffsetPersistenceFactory.Writer {
        override val offsets: OffsetRangeSet = startSet ?: OffsetRangeSet()

        override fun doWrite(): Unit = time("accounting.offsets") {
            try {
                val offsets = RedisOffsetRangeSet(offsets.map { topicPartition, offsetIntervals ->
                    RedisOffsetIntervals(
                            topicPartition.topic,
                            topicPartition.partition,
                            offsetIntervals.toList())
                })

                redisHolder.execute { redis ->
                    redis.set(path.toString(), redisOffsetWriter.writeValueAsString(offsets))
                }
            } catch (e: IOException) {
                logger.error("Failed to write offsets to Redis: {}", e.toString())
            }
        }
    }

    companion object {
        data class RedisOffsetRangeSet(
                val partitions: List<RedisOffsetIntervals>)

        data class RedisOffsetIntervals(
                val topic: String,
                val partition: Int,
                val ranges: List<OffsetRangeSet.Range>)

        private val logger = LoggerFactory.getLogger(OffsetRedisPersistence::class.java)
        private val mapper = jacksonObjectMapper().apply {
            registerModule(JavaTimeModule())
            configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
        }
        val redisOffsetWriter: ObjectWriter = mapper.writerFor(RedisOffsetRangeSet::class.java)
        val redisOffsetReader: ObjectReader = mapper.readerFor(RedisOffsetRangeSet::class.java)
    }
}
