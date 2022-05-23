package org.radarbase.output.accounting

import kotlinx.coroutines.test.runTest
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.not
import org.hamcrest.Matchers.nullValue
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.radarbase.output.util.SuspendedCloseable.Companion.useSuspended
import redis.clients.jedis.JedisPool

internal class RedisRemoteLockManagerTest {
    private lateinit var redisHolder: RedisHolder
    private lateinit var lockManager1: RemoteLockManager
    private lateinit var lockManager2: RemoteLockManager

    @BeforeEach
    fun setUp() {
        redisHolder = RedisHolder(JedisPool())
        lockManager1 = RedisRemoteLockManager(redisHolder, "locks")
        lockManager2 = RedisRemoteLockManager(redisHolder, "locks")
    }

    @AfterEach
    fun tearDown() {
        redisHolder.close()
    }

    @Test
    fun testExclusiveLock() = runTest {
        lockManager1.acquireLock("t").useSuspended { l1 ->
            assertThat(l1, not(nullValue()))
            lockManager2.acquireLock("t").useSuspended { l2 ->
                assertThat(l2, nullValue())
            }
        }
    }

    @Test
    fun testGranularityLock() = runTest {
        lockManager1.acquireLock("t1").useSuspended { l1 ->
            assertThat(l1, not(nullValue()))
            lockManager2.acquireLock("t2").useSuspended { l2 ->
                assertThat(l2, not(nullValue()))
            }
        }
    }

    @Test
    fun testNonOverlappingLock() = runTest {
        lockManager1.acquireLock("t").useSuspended { l1 ->
            assertThat(l1, not(nullValue()))
        }
        lockManager2.acquireLock("t").useSuspended { l2 ->
            assertThat(l2, not(nullValue()))
        }
    }

    @Test
    fun testNonOverlappingLockSameManager() = runTest {
        lockManager1.acquireLock("t").useSuspended { l1 ->
            assertThat(l1, not(nullValue()))
        }
        lockManager1.acquireLock("t").useSuspended { l2 ->
            assertThat(l2, not(nullValue()))
        }
    }
}
