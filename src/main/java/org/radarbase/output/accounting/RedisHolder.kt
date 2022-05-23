package org.radarbase.output.accounting

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool
import redis.clients.jedis.exceptions.JedisException
import java.io.Closeable
import java.io.IOException

class RedisHolder(
    private val jedisPool: JedisPool,
) : Closeable {
    @Throws(IOException::class)
    suspend fun <T> execute(routine: suspend (Jedis) -> T): T = withContext(Dispatchers.IO) {
        try {
            jedisPool.resource.use {
                routine(it)
            }
        } catch (ex: JedisException) {
            throw IOException(ex)
        }
    }

    override fun close() {
        jedisPool.close()
    }
}
