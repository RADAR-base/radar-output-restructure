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

import org.radarbase.output.config.BucketFormatterConfig
import org.radarbase.output.config.PathConfig
import org.radarbase.output.config.PathFormatterConfig
import org.radarbase.output.config.TopicConfig
import org.slf4j.LoggerFactory

open class FormattedPathFactory : RecordPathFactory() {
    private lateinit var pathFormatter: PathFormatter
    private var topicFormatters: Map<String, PathFormatter> = emptyMap()
    private var bucketFormatter: PathFormatter? = null
    private lateinit var disabledBucketRegexes: List<Regex>
    private lateinit var defaultBucketName: String

    override fun init(
        extension: String,
        config: PathConfig,
        topics: Map<String, TopicConfig>,
    ) {
        super.init(extension, config, topics)
        pathFormatter = pathConfig.path.toPathFormatter()
        bucketFormatter = pathConfig.bucket?.toBucketFormatter()
        disabledBucketRegexes = pathConfig.bucket
            ?.disabledFormats
            ?.map { it.toRegex(RegexOption.IGNORE_CASE) }
            ?: emptyList()
        defaultBucketName = pathConfig.bucket
            ?.defaultName
            ?: "radar-output-storage"

        logger.info("Formatting path with {}", pathFormatter)
    }

    override suspend fun bucket(pathParameters: PathFormatParameters?): String? {
        val formatter = bucketFormatter ?: return null
        pathParameters ?: return pathConfig.bucket?.defaultName
        val format = formatter.format(pathParameters)
        return if (disabledBucketRegexes.any { it.matches(format) }) {
            defaultBucketName
        } else {
            format
        }
    }

    override fun addTopicConfiguration(topicConfig: Map<String, TopicConfig>) {
        topicFormatters = buildMap {
            topicConfig.forEach { (topic, config) ->
                val topicFormatConfig = pathConfig.path.copy(config.pathProperties)
                if (topicFormatConfig != pathConfig.path) {
                    val formatter = topicFormatConfig.toPathFormatter()
                    logger.info("Formatting path of topic {} with {}", topic, formatter)
                    put(topic, formatter)
                }
            }
        }
    }

    override suspend fun relativePath(
        pathParameters: PathFormatParameters
    ): String = (topicFormatters[pathParameters.topic] ?: pathFormatter)
        .format(pathParameters)

    companion object {
        private fun PathFormatterConfig.toPathFormatter(): PathFormatter = PathFormatter(
            format,
            plugins.toPathFormatterPlugins(properties),
        )

        private fun BucketFormatterConfig.toBucketFormatter(): PathFormatter = PathFormatter(
            format,
            plugins.toPathFormatterPlugins(properties),
            checkMinimalDistinction = false,
        )

        private val logger = LoggerFactory.getLogger(FormattedPathFactory::class.java)
    }
}
