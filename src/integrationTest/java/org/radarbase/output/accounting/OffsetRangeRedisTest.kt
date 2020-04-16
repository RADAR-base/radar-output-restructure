package org.radarbase.output.accounting

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import redis.clients.jedis.JedisPool
import java.io.IOException
import java.nio.file.Path
import java.nio.file.Paths

class OffsetRangeRedisTest {
    private lateinit var testFile: Path
    private lateinit var redisPool: JedisPool
    private lateinit var offsetReader: OffsetRangeSerialization.Reader

    @BeforeEach
    @Throws(IOException::class)
    fun setUp() {
        testFile = Paths.get("test/topic")
        redisPool = JedisPool()
        offsetReader = OffsetRangeRedis.RedisReader(redisPool)
    }

    @AfterEach
    fun tearDown() {
        redisPool.resource.use { it.del(testFile.toString()) }
    }

    @Test
    @Throws(IOException::class)
    fun readEmpty() {
        Assertions.assertTrue(offsetReader.read(testFile).offsets.isEmpty)

        redisPool.resource.use { it.del(testFile.toString()) }

        // will create on write
        Assertions.assertTrue(offsetReader.read(testFile).offsets.isEmpty)
    }

    @Test
    @Throws(IOException::class)
    fun write() {
        offsetReader.read(testFile).use { rangeFile ->
            rangeFile.add(OffsetRange.parseFilename("a+0+0+1"))
            rangeFile.add(OffsetRange.parseFilename("a+0+1+2"))
        }

        val set = offsetReader.read(testFile).use { it.offsets }
        Assertions.assertTrue(set.contains(OffsetRange.parseFilename("a+0+0+1")))
        Assertions.assertTrue(set.contains(OffsetRange.parseFilename("a+0+1+2")))
        Assertions.assertTrue(set.contains(OffsetRange.parseFilename("a+0+0+2")))
        Assertions.assertFalse(set.contains(OffsetRange.parseFilename("a+0+0+3")))
        Assertions.assertFalse(set.contains(OffsetRange.parseFilename("a+0+2+3")))
        Assertions.assertFalse(set.contains(OffsetRange.parseFilename("a+1+0+1")))
        Assertions.assertFalse(set.contains(OffsetRange.parseFilename("b+0+0+1")))
    }

    @Test
    @Throws(IOException::class)
    fun cleanUp() {
        offsetReader.read(testFile).use { rangeFile ->
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

        val rangeSet = offsetReader.read(testFile).use { it.offsets }
        assertEquals(2, rangeSet.size(TopicPartition("a", 0)))
    }
}
