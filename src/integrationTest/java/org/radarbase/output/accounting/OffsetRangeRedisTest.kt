package org.radarbase.output.accounting

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.radarbase.output.accounting.OffsetRedisPersistence.Companion.redisOffsetReader
import org.radarbase.output.util.SuspendedCloseable.Companion.useSuspended
import redis.clients.jedis.JedisPool
import java.io.IOException
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Instant

@OptIn(ExperimentalCoroutinesApi::class)
class OffsetRangeRedisTest {
    private lateinit var testFile: Path
    private lateinit var redisHolder: RedisHolder
    private lateinit var offsetPersistence: OffsetPersistenceFactory
    private val lastModified = Instant.now()

    @BeforeEach
    @Throws(IOException::class)
    fun setUp() {
        testFile = Paths.get("test/topic")
        redisHolder = RedisHolder(JedisPool())
        offsetPersistence = OffsetRedisPersistence(redisHolder)
    }

    @AfterEach
    fun tearDown() {
        runTest {
            redisHolder.execute { it.del(testFile.toString()) }
        }
    }

    @Test
    @Throws(IOException::class)
    fun readEmpty() = runTest {
        assertNull(offsetPersistence.read(testFile))

        // will create on write
        offsetPersistence.writer(this@runTest, testFile).closeAndJoin()

        assertEquals(true, offsetPersistence.read(testFile)?.isEmpty)

        redisHolder.execute { it.del(testFile.toString()) }

        assertNull(offsetPersistence.read(testFile))
    }

    @Test
    @Throws(IOException::class)
    fun write() = runTest {
        offsetPersistence.writer(this@runTest, testFile).useSuspended { rangeFile ->
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
    fun cleanUp() = runTest {
        offsetPersistence.writer(this@runTest, testFile).useSuspended { rangeFile ->
            rangeFile.add(TopicPartitionOffsetRange.parseFilename("a+0+0+1", lastModified))
            rangeFile.add(TopicPartitionOffsetRange.parseFilename("a+0+1+2", lastModified))
            rangeFile.add(TopicPartitionOffsetRange.parseFilename("a+0+4+4", lastModified))
        }

        redisHolder.execute { redis ->
            val range =
                redisOffsetReader.readValue<OffsetRedisPersistence.Companion.RedisOffsetRangeSet>(
                    redis.get(testFile.toString())
                )
            assertEquals(
                OffsetRedisPersistence.Companion.RedisOffsetRangeSet(
                    listOf(
                        OffsetRedisPersistence.Companion.RedisOffsetIntervals(
                            topic = "a",
                            partition = 0,
                            ranges = listOf(
                                OffsetRangeSet.Range(0, 2, lastModified),
                                OffsetRangeSet.Range(4, 4, lastModified),
                            ),
                        ),
                    ),
                ),
                range
            )
        }

        val rangeSet = offsetPersistence.read(testFile)
        assertEquals(2, rangeSet?.size(TopicPartition("a", 0)))
    }
}
