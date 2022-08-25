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

open class FormattedPathFactory : RecordPathFactory() {
    private lateinit var formatter: PathFormatter
    private var topicFormatters: Map<String, PathFormatter> = emptyMap()

    override fun init(properties: Map<String, String>) {
        super.init(properties)

        val format = properties["format"]
            ?: run {
                logger.warn("Path format not provided, using {} instead", DEFAULT_FORMAT)
                DEFAULT_FORMAT
            }
        formatter = PathFormatter(format)
    }

    override fun addTopicConfiguration(topicConfig: Map<String, TopicConfig>) {
        topicFormatters = buildMap {
            topicConfig.forEach { (topic, config) ->
                config.pathFormat
                    ?.let { PathFormatter(it) }
                    ?.let { put(topic, it) }
            }
        }
    }

    override fun getRelativePath(
        topic: String,
        key: GenericRecord,
        value: GenericRecord,
        time: Instant?,
        attempt: Int,
    ): Path = (topicFormatters[topic] ?: formatter)
        .format(topic, key, value, time, attempt, extension, ::getTimeBin)

    override fun getCategory(
        key: GenericRecord,
        value: GenericRecord,
    ): String = sanitizeId(key.get("sourceId"), "unknown-source")

    companion object {
        private const val DEFAULT_FORMAT = "\${projectId}/\${userId}/\${topic}/\${filename}"
        private val logger = LoggerFactory.getLogger(FormattedPathFactory::class.java)
    }
}
