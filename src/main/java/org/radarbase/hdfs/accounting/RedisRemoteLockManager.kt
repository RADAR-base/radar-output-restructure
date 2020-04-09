package org.radarbase.hdfs.accounting

import org.slf4j.LoggerFactory
import redis.clients.jedis.JedisPool
import redis.clients.jedis.params.SetParams
import java.time.Duration
import java.util.*

class RedisRemoteLockManager(
        private val jedisPool: JedisPool,
        private val keyPrefix: String
) : RemoteLockManager {
    private val uuid: String = UUID.randomUUID().toString()

    init {
        logger.info("Managing locks as ID {}", uuid)
    }

    override fun acquireTopicLock(topic: String): RemoteLockManager.RemoteLock? {
        val lockKey = "$keyPrefix/$topic.lock"
        jedisPool.resource.use { jedis ->
            return jedis.set(lockKey, uuid, setParams)?.let {
                RemoteLock(lockKey)
            }
        }
    }

    private inner class RemoteLock(
            private val lockKey: String
    ) : RemoteLockManager.RemoteLock {
        override fun close() {
            jedisPool.resource.use { jedis ->
                if (jedis.get(lockKey) == uuid) {
                    jedis.del(lockKey)
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
