package org.radarbase.output.config

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.kotlin.readValue
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.nio.file.Files
import java.nio.file.Paths

internal object YAMLConfigLoader {
    val mapper = ObjectMapper(YAMLFactory())
            .registerKotlinModule()

    inline fun <reified T: Any> load(path: String?, defaultPath: String, initializer: () -> T): T {
        return if (path != null) {
            requireNotNull(loadFromPath<T>(path)) { "Config file $path does not exist" }
        } else {
            loadFromPath(defaultPath) ?: initializer()
        }
    }

    inline fun <reified T> loadFromPath(path: String): T? {
        val filePath = Paths.get(path)
        return if (Files.exists(filePath)) {
            mapper.readValue<T>(filePath.toFile())
                    .also { logger.info("Loaded config from $path") }
        } else {
            val url = RestructureConfig::class.java.getResource(path)
                    ?: RestructureConfig::class.java.getResource("/$path")

            url?.let { mapper.readValue<T>(url) }
                    ?.also { logger.info("Loaded config from classpath $path") }
        }
    }

    val logger: Logger = LoggerFactory.getLogger(YAMLConfigLoader::class.java)
}
