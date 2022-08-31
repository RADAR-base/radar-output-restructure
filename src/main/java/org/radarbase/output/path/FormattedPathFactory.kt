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
    private lateinit var properties: Map<String, String>
    private var topicFormatters: Map<String, PathFormatter> = emptyMap()

    override fun init(properties: Map<String, String>) {
        super.init(properties)

        this.properties = DEFAULTS + properties
        formatter = createFormatter(this.properties)
        logger.info("Formatting path with {}", formatter)
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
                createFormatter(properties + config.pathProperties)
            }
            .onEach { (topic, formatter) ->
                logger.info("Formatting path of topic {} with {}", topic, formatter)
            }
    }

    private fun createFormatter(properties: Map<String, String>): PathFormatter {
        val format = checkNotNull(properties["format"])
        val pluginClassNames = checkNotNull(properties["plugins"])
        val plugins = instantiatePlugins(pluginClassNames, properties)

        return PathFormatter(format, plugins)
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
        internal val DEFAULTS = mapOf(
            "format" to "\${projectId}/\${userId}/\${topic}/\${filename}",
            "plugins" to "fixed time key value",
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
    }
}
