package org.radarbase.output.accounting

import org.slf4j.LoggerFactory
import redis.clients.jedis.JedisPool
import redis.clients.jedis.params.SetParams
import java.time.Duration
import java.util.*

class RedisRemoteLockManager(
        private val redisPool: JedisPool,
        private val keyPrefix: String
) : RemoteLockManager {
    private val uuid: String = UUID.randomUUID().toString()

    init {
        logger.info("Managing locks as ID {}", uuid)
    }

    override fun acquireLock(name: String): RemoteLockManager.RemoteLock? {
        val lockKey = "$keyPrefix/$name.lock"
        redisPool.resource.use { redis ->
            return redis.set(lockKey, uuid, setParams)?.let {
                RemoteLock(lockKey)
            }
        }
    }

    private inner class RemoteLock(
            private val lockKey: String
    ) : RemoteLockManager.RemoteLock {
        override fun close() {
            redisPool.resource.use { redis ->
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
                .px(Duration.ofDays(1).toMillis()) // limit the duration of a lock to 24 hours
    }
}
