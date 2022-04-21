package org.radarbase.output.accounting

import org.slf4j.LoggerFactory
import redis.clients.jedis.params.SetParams
import java.util.*
import kotlin.time.Duration.Companion.days

class RedisRemoteLockManager(
    private val redisHolder: RedisHolder,
    private val keyPrefix: String
) : RemoteLockManager {
    private val uuid: String = UUID.randomUUID().toString()

    init {
        logger.info("Managing locks as ID {}", uuid)
    }

    override suspend fun acquireLock(name: String): RemoteLockManager.RemoteLock? {
        val lockKey = "$keyPrefix/$name.lock"
        return redisHolder.execute { redis ->
            redis.set(lockKey, uuid, setParams)?.let {
                RemoteLock(lockKey)
            }
        }
    }

    private inner class RemoteLock(
            private val lockKey: String
    ) : RemoteLockManager.RemoteLock {
        override suspend fun closeAndJoin() {
            redisHolder.execute { redis ->
                if (redis.get(lockKey) == uuid) {
                    redis.del(lockKey)
                }
            }
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(RedisRemoteLockManager::class.java)
        private val setParams = SetParams()
                .nx() // only set if not already set
                .px(1.days.inWholeMilliseconds) // limit the duration of a lock to 24 hours
    }
}
