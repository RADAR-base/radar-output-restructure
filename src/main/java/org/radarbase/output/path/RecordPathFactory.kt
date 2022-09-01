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

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericRecordBuilder
import org.radarbase.output.FileStoreFactory
import org.radarbase.output.Plugin
import org.radarbase.output.config.TopicConfig
import org.radarbase.output.util.TimeUtil
import org.slf4j.LoggerFactory
import java.nio.file.Path
import java.time.Instant
import java.util.regex.Pattern

abstract class RecordPathFactory : Plugin {
    lateinit var root: Path
    lateinit var extension: String
    lateinit var fileStoreFactory: FileStoreFactory

    /**
     * Get the organization of given record in given topic.
     * @param topic Kafka topic name
     * @param record record with possible key and value fields containing records
     * @param attempt number of previous attempts to write given record. This increases if previous
     * paths already existed and are incompatible.
     * @return organization of given record
     */
    open fun getRecordPath(
        topic: String,
        record: GenericRecord,
        attempt: Int,
    ): Path {
        val keyField = record.get("key")
        val valueField = record.get("value") as? GenericRecord

        if (keyField == null || valueField == null) {
            logger.error("Failed to process {}", record)
            throw IllegalArgumentException("Failed to process $record; no key or value")
        }

        val keyRecord: GenericRecord = if (keyField is GenericRecord) {
            keyField
        } else {
            GenericRecordBuilder(observationKeySchema).apply {
                set("projectId", valueField.getOrNull("projectId"))
                set("userId", keyField.toString())
                set("sourceId", valueField.getOrNull("sourceId") ?: "unknown")
            }.build()
        }

        val time = TimeUtil.getDate(keyRecord, valueField)

        val relativePath = getRelativePath(topic, keyRecord, valueField, time, attempt)
        return root.resolve(relativePath)
    }

    /**
     * Get the relative path corresponding to given record on given topic.
     * @param topic Kafka topic name
     * @param key record key
     * @param value record value
     * @param time time contained in the record
     * @param attempt number of previous attempts to write given record. This increases if previous
     * paths already existed and are incompatible.
     * @return relative path corresponding to given parameters.
     */
    abstract fun getRelativePath(
        topic: String,
        key: GenericRecord,
        value: GenericRecord,
        time: Instant?,
        attempt: Int,
    ): Path

    companion object {
        private val logger = LoggerFactory.getLogger(RecordPathFactory::class.java)
        private val ILLEGAL_CHARACTER_PATTERN = Pattern.compile("[^a-zA-Z0-9_-]+")

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

    open fun addTopicConfiguration(topicConfig: Map<String, TopicConfig>) = Unit
}
