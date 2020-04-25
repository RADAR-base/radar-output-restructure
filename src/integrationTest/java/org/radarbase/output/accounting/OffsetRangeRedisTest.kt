package org.radarbase.output.accounting

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import redis.clients.jedis.JedisPool
import java.io.IOException
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Instant

class OffsetRangeRedisTest {
    private lateinit var testFile: Path
    private lateinit var redisPool: JedisPool
    private lateinit var offsetPersistence: OffsetPersistenceFactory
    private val lastModified = Instant.now()

    @BeforeEach
    @Throws(IOException::class)
    fun setUp() {
        testFile = Paths.get("test/topic")
        redisPool = JedisPool()
        offsetPersistence = OffsetRedisPersistence(redisPool)
    }

    @AfterEach
    fun tearDown() {
        redisPool.resource.use { it.del(testFile.toString()) }
    }

    @Test
    @Throws(IOException::class)
    fun readEmpty() {
        assertNull(offsetPersistence.read(testFile))

        // will create on write
        offsetPersistence.writer(testFile).close()

        assertEquals(true, offsetPersistence.read(testFile)?.isEmpty)

        redisPool.resource.use { it.del(testFile.toString()) }

        assertNull(offsetPersistence.read(testFile))
    }

    @Test
    @Throws(IOException::class)
    fun write() {
        offsetPersistence.writer(testFile).use { rangeFile ->
            rangeFile.add(TopicPartitionOffsetRange.parseFilename("a+0+0+1", lastModified))
            rangeFile.add(TopicPartitionOffsetRange.parseFilename("a+0+1+2", lastModified))
        }

        val set = offsetPersistence.read(testFile)
        assertNotNull(set)
        requireNotNull(set)
        assertTrue(set.contains(TopicPartitionOffsetRange.parseFilename("a+0+0+1", lastModified)))
        assertTrue(set.contains(TopicPartitionOffsetRange.parseFilename("a+0+1+2", lastModified)))
        assertTrue(set.contains(TopicPartitionOffsetRange.parseFilename("a+0+0+2", lastModified)))
        assertFalse(set.contains(TopicPartitionOffsetRange.parseFilename("a+0+0+3", lastModified)))
        assertFalse(set.contains(TopicPartitionOffsetRange.parseFilename("a+0+2+3", lastModified)))
        assertFalse(set.contains(TopicPartitionOffsetRange.parseFilename("a+1+0+1", lastModified)))
        assertFalse(set.contains(TopicPartitionOffsetRange.parseFilename("b+0+0+1", lastModified)))
    }

    @Test
    @Throws(IOException::class)
    fun cleanUp() {
        offsetPersistence.writer(testFile).use { rangeFile ->
            rangeFile.add(TopicPartitionOffsetRange.parseFilename("a+0+0+1", lastModified))
            rangeFile.add(TopicPartitionOffsetRange.parseFilename("a+0+1+2", lastModified))
            rangeFile.add(TopicPartitionOffsetRange.parseFilename("a+0+4+4", lastModified))
        }

        redisPool.resource.use { redis ->
            val range = jacksonObjectMapper().readValue(redis.get(testFile.toString()), OffsetRedisPersistence.Companion.RedisOffsetRangeSet::class.java)
            assertEquals(OffsetRedisPersistence.Companion.RedisOffsetRangeSet(listOf(
                    OffsetRedisPersistence.Companion.RedisOffsetIntervals("a", 0, listOf(
                            OffsetRangeSet.Range(0, 2, lastModified),
                            OffsetRangeSet.Range(4, 4, lastModified)))
            )), range)
        }

        val rangeSet = offsetPersistence.read(testFile)
        assertEquals(2, rangeSet?.size(TopicPartition("a", 0)))
    }
}
