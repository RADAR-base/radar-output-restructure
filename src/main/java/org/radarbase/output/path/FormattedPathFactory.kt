/*
 * Copyright 2018 The Hyve
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.radarbase.output.path

import org.apache.avro.generic.GenericRecord
import org.radarbase.output.config.TopicConfig
import org.slf4j.LoggerFactory
import java.nio.file.Path
import java.time.Instant
import kotlin.reflect.jvm.jvmName

open class FormattedPathFactory : RecordPathFactory() {
    private lateinit var format: String
    private lateinit var plugins: List<PathFormatterPlugin>
    private lateinit var formatter: PathFormatter
    private lateinit var properties: Map<String, String>
    private var topicFormatters: Map<String, PathFormatter> = emptyMap()

    override fun init(properties: Map<String, String>) {
        super.init(properties)

        format = properties["format"]
            ?: run {
                logger.warn("Path format not provided, using '{}' instead", DEFAULT_FORMAT)
                DEFAULT_FORMAT
            }
        val pluginClassNames = properties["plugins"]
            ?: run {
                logger.warn("Path format plugins not provided, using '{}' instead", DEFAULT_FORMAT_PLUGINS)
                DEFAULT_FORMAT_PLUGINS
            }

        plugins = instantiatePlugins(pluginClassNames, properties)
        formatter = PathFormatter(format, plugins)
    }

    private fun instantiatePlugins(
        pluginClassNames: String,
        properties: Map<String, String>,
    ): List<PathFormatterPlugin> = pluginClassNames
        .trim()
        .split("\\s+".toRegex())
        .mapNotNull { it.toPathFormatterPlugin() }
        .onEach { it.init(properties) }

    override fun addTopicConfiguration(topicConfig: Map<String, TopicConfig>) {
        topicFormatters = topicConfig
            .filter { (_, config) -> config.pathProperties.isNotEmpty() }
            .mapValues { (_, config) ->
                val topicFormat = config.pathProperties.getOrDefault("format", format)
                val pluginClassNames = config.pathProperties["plugins"]
                val topicPlugins = if (pluginClassNames != null) {
                    instantiatePlugins(pluginClassNames, properties + config.pathProperties)
                } else plugins

                PathFormatter(topicFormat, topicPlugins)
            }
    }

    override fun getRelativePath(
        topic: String,
        key: GenericRecord,
        value: GenericRecord,
        time: Instant?,
        attempt: Int,
    ): Path = (topicFormatters[topic] ?: formatter)
        .format(PathFormatParameters(topic, key, value, time, attempt, extension, this::getTimeBin))

    override fun getCategory(
        key: GenericRecord,
        value: GenericRecord,
    ): String = sanitizeId(key.get("sourceId"), "unknown-source")

    companion object {
        internal const val DEFAULT_FORMAT = "\${projectId}/\${userId}/\${topic}/\${filename}"
        internal const val DEFAULT_FORMAT_PLUGINS = "fixed time key value"
        private val logger = LoggerFactory.getLogger(FormattedPathFactory::class.java)

        private fun String.toPathFormatterPlugin(): PathFormatterPlugin? = when (this) {
            "fixed" -> FixedPathFormatterPlugin()
            "time" -> TimePathFormatterPlugin()
            "key" -> KeyPathFormatterPlugin()
            "value" -> ValuePathFormatterPlugin()
            else -> {
                try {
                    Class.forName(this).getConstructor()
                        .newInstance() as PathFormatterPlugin
                } catch (ex: ReflectiveOperationException) {
                    logger.error("Failed to instantiate plugin {}", this)
                    null
                } catch (ex: ClassCastException) {
                    logger.error(
                        "Failed to instantiate plugin {}, it does not extend {}",
                        this,
                        PathFormatterPlugin::class.jvmName
                    )
                    null
                }
            }
        }
    }
}
