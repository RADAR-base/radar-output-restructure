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

import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericRecordBuilder
import org.radarbase.output.config.PathConfig
import org.radarbase.output.config.TopicConfig
import org.radarbase.output.util.TimeUtil
import java.nio.file.Path
import java.nio.file.Paths
import java.util.regex.Pattern

abstract class RecordPathFactory {
    lateinit var pathConfig: PathConfig
        private set

    open fun init(
        extension: String,
        config: PathConfig,
        topics: Map<String, TopicConfig> = emptyMap(),
    ) {
        this.pathConfig = config.copy(
            output = if (config.output.isAbsolute) {
                rootPath.relativize(config.output)
            } else {
                config.output
            },
            path = config.path.copy(
                properties = buildMap(config.path.properties.size + 1) {
                    putAll(config.path.properties)
                    putIfAbsent("extension", extension)
                }
            )
        )
        this.addTopicConfiguration(topics)
    }

    /**
     * Get the organization of given record in given topic.
     * @param topic Kafka topic name
     * @param record record with possible key and value fields containing records
     * @param attempt number of previous attempts to write given record. This increases if previous
     * paths already existed and are incompatible.
     * @return organization of given record
     */
    open suspend fun getRecordPath(
        topic: String,
        record: GenericRecord,
        attempt: Int,
    ): Path {
        val keyField = requireNotNull(record.get("key")) { "Failed to process $record; no key present" }
        val valueField = requireNotNull(record.get("value") as? GenericRecord) { "Failed to process $record; no value present" }

        val keyRecord: GenericRecord = if (keyField is GenericRecord) {
            keyField
        } else {
            GenericRecordBuilder(observationKeySchema).apply {
                set("projectId", valueField.getOrNull("projectId"))
                set("userId", keyField.toString())
                set("sourceId", valueField.getOrNull("sourceId") ?: "unknown")
            }.build()
        }

        val params = PathFormatParameters(
            topic = topic,
            key = keyRecord,
            value = valueField,
            time = TimeUtil.getDate(keyRecord, valueField),
            attempt = attempt,
        )

        return coroutineScope {
            val bucketJob = async { bucket(params) }
            val pathJob = async { relativePath(params) }

            val path = pathConfig.output.resolve(pathJob.await())
            val bucket = bucketJob.await()
            if (bucket != null) {
                Paths.get(bucket).resolve(path)
            } else {
                path
            }
        }
    }

    abstract suspend fun bucket(pathParameters: PathFormatParameters?): String?

    /**
     * Get the relative path corresponding to given record on given topic.
     * @param pathParameters Parameters needed to determine the path
     * @return relative path corresponding to given parameters.
     */
    abstract suspend fun relativePath(
        pathParameters: PathFormatParameters,
    ): String

    companion object {
        private val ILLEGAL_CHARACTER_PATTERN = Pattern.compile("[^a-zA-Z0-9_-]+")
        private val rootPath = Paths.get("/")

        fun sanitizeId(id: Any?, defaultValue: String): String = id
            ?.let { ILLEGAL_CHARACTER_PATTERN.matcher(it.toString()).replaceAll("") }
            ?.takeIf { it.isNotEmpty() }
            ?: defaultValue

        private val observationKeySchema = Schema.Parser().parse(
            """
            {
              "namespace": "org.radarcns.kafka",
              "type": "record",
              "name": "ObservationKey",
              "doc": "Key of an observation.",
              "fields": [
                {"name": "projectId", "type": ["null", "string"], "doc": "Project identifier. Null if unknown or the user is not enrolled in a project.", "default": null},
                {"name": "userId", "type": "string", "doc": "User Identifier created during the enrolment."},
                {"name": "sourceId", "type": "string", "doc": "Unique identifier associated with the source."}
              ]
            }
            """.trimIndent()
        )

        fun GenericRecord.getFieldOrNull(fieldName: String): Schema.Field? {
            return schema.fields
                .find { it.name().equals(fieldName, ignoreCase = true) }
        }

        fun GenericRecord.getOrNull(fieldName: String): Any? = getFieldOrNull(fieldName)
            ?.let { get(it.pos()) }
    }

    protected open fun addTopicConfiguration(topicConfig: Map<String, TopicConfig>) = Unit
}
