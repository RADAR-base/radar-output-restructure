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
    private lateinit var formatter: PathFormatter
    private lateinit var config: PathFormatterConfig
    private var topicFormatters: Map<String, PathFormatter> = emptyMap()

    override fun init(properties: Map<String, String>) {
        super.init(properties)

        this.config = DEFAULTS.withValues(properties)
        formatter = config.toPathFormatter()
        logger.info("Formatting path with {}", formatter)
    }

    override fun addTopicConfiguration(topicConfig: Map<String, TopicConfig>) {
        topicFormatters = topicConfig
            .filter { (_, config) -> config.pathProperties.isNotEmpty() }
            .mapValues { (_, config) ->
                this.config.withValues(config.pathProperties)
                    .toPathFormatter()
            }
            .onEach { (topic, formatter) ->
                logger.info("Formatting path of topic {} with {}", topic, formatter)
            }
    }

    override fun getRelativePath(
        topic: String,
        key: GenericRecord,
        value: GenericRecord,
        time: Instant?,
        attempt: Int,
    ): Path = (topicFormatters[topic] ?: formatter)
        .format(PathFormatParameters(topic, key, value, time, attempt, extension))

    companion object {
        private fun PathFormatterConfig.toPathFormatter(): PathFormatter {
            return PathFormatter(format, createPlugins())
        }

        internal val DEFAULTS = PathFormatterConfig(
            format = "\${projectId}/\${userId}/\${topic}/\${filename}",
            pluginNames = "fixed time key value",
        )
        private val logger = LoggerFactory.getLogger(FormattedPathFactory::class.java)

        internal fun String.toPathFormatterPlugin(): PathFormatterPlugin? = when (this) {
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

        data class PathFormatterConfig(
            val format: String,
            val pluginNames: String,
            val properties: Map<String, String> = mapOf(),
        ) {
            fun createPlugins(): List<PathFormatterPlugin> = pluginNames
                .trim()
                .split("\\s+".toRegex())
                .mapNotNull { it.toPathFormatterPlugin() }
                .onEach { it.init(properties) }

            fun withValues(values: Map<String, String>): PathFormatterConfig {
                val newProperties = HashMap(properties).apply {
                    putAll(values)
                }
                val format = newProperties.remove("format") ?: this.format
                val pluginNames = newProperties.remove("plugins") ?: this.pluginNames

                return PathFormatterConfig(format, pluginNames, newProperties)
            }
        }
    }
}
