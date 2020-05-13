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

class OffsetRangeRedisTest {
    private lateinit var testFile: Path
    private lateinit var redisPool: JedisPool
    private lateinit var offsetPersistence: OffsetPersistenceFactory

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
            rangeFile.add(OffsetRange.parseFilename("a+0+0+1"))
            rangeFile.add(OffsetRange.parseFilename("a+0+1+2"))
        }

        val set = offsetPersistence.read(testFile)
        assertNotNull(set)
        requireNotNull(set)
        assertTrue(set.contains(OffsetRange.parseFilename("a+0+0+1")))
        assertTrue(set.contains(OffsetRange.parseFilename("a+0+1+2")))
        assertTrue(set.contains(OffsetRange.parseFilename("a+0+0+2")))
        assertFalse(set.contains(OffsetRange.parseFilename("a+0+0+3")))
        assertFalse(set.contains(OffsetRange.parseFilename("a+0+2+3")))
        assertFalse(set.contains(OffsetRange.parseFilename("a+1+0+1")))
        assertFalse(set.contains(OffsetRange.parseFilename("b+0+0+1")))
    }

    @Test
    @Throws(IOException::class)
    fun cleanUp() {
        offsetPersistence.writer(testFile).use { rangeFile ->
            rangeFile.add(OffsetRange.parseFilename("a+0+0+1"))
            rangeFile.add(OffsetRange.parseFilename("a+0+1+2"))
            rangeFile.add(OffsetRange.parseFilename("a+0+4+4"))
        }

        redisPool.resource.use { redis ->
            val range = jacksonObjectMapper().readValue(redis.get(testFile.toString()), OffsetRangeSet.OffsetRangeList::class.java)
            assertEquals(OffsetRangeSet.OffsetRangeList(listOf(
                    OffsetRangeSet.OffsetRangeList.TopicPartitionRange("a", 0, listOf(
                            OffsetRangeSet.Range(0, 2),
                            OffsetRangeSet.Range(4, 4)))
            )), range)
        }

        val rangeSet = offsetPersistence.read(testFile)
        assertEquals(2, rangeSet?.size(TopicPartition("a", 0)))
    }
}
