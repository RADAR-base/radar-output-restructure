package org.radarbase.output.config

import org.radarbase.output.config.RestructureConfig.Companion.copyEnv
import java.net.URI

/** Redis configuration. */
data class RedisConfig(
    /**
     * Full Redis URI. The protocol should be redis for plain text and rediss for TLS. It
     * should contain at least a hostname and port, but it may also include username and
     * password.
     */
    val uri: URI = URI.create("redis://localhost:6379"),
    /**
     * Prefix to use for creating a lock of a topic.
     */
    val lockPrefix: String = "radar-output/lock",
) {
    fun withEnv(): RedisConfig = this
        .copyEnv("REDIS_URI") { copy(uri = URI.create(it)) }
}
