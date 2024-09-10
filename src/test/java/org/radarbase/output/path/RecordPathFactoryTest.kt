package org.radarbase.output.path

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.radarbase.output.config.PathConfig
import org.radarbase.output.config.PathFormatterConfig
import org.radarbase.output.config.ResourceConfig
import org.radarbase.output.config.S3Config

internal class RecordPathFactoryTest {

    @Test
    fun testInit() {
        var properties = mapOf("key1" to "value1", "key2" to "value2")

        val pathConfig = PathConfig(
            factory = "org.radarbase.output.path.FormattedPathFactory",
            properties = properties,
            path = PathFormatterConfig(
                format = "\${topic}/\${projectId}/\${userId}/\${sourceId}/\${filename}",
                plugins = "fixed",
            ),
        )

        val targetConfig = S3Config(
            endpoint = "http://localhost:9000",
            accessToken = "minioadmin",
            secretKey = "minioadmin",
            bucket = "target",
        )

        val factory = pathConfig.createFactory(
            ResourceConfig("s3", s3 = targetConfig),
            "test-extension",
            topics = mapOf(),
        )

        properties = buildMap {
            putAll(properties)
            putIfAbsent("extension", "test-extension")
        }

        assertEquals(properties, factory.pathConfig.path.properties)
        assertEquals(properties, factory.pathConfig.path.properties)
    }
}
