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
import org.apache.avro.Schema.Type
import org.apache.avro.generic.GenericRecord
import org.radarbase.output.Plugin
import org.slf4j.LoggerFactory
import java.nio.file.Path
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneOffset.UTC
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import java.util.regex.Pattern

abstract class RecordPathFactory : Plugin {
    lateinit var root: Path
    lateinit var extension: String

    protected open var timeBinFormat: DateTimeFormatter = HOURLY_TIME_BIN_FORMAT

    override fun init(properties: Map<String, String>) {
        super.init(properties)
        properties["timeBinFormat"]?.let {
            try {
                timeBinFormat = DateTimeFormatter
                        .ofPattern(it)
                        .withZone(UTC)
            } catch (ex: IllegalArgumentException) {
                logger.error("Cannot use time bin format {}, using {} instad", it, timeBinFormat, ex)
            }
        }
    }

    /**
     * Get the organization of given record in given topic.
     * @param topic Kafka topic name
     * @param record record with possible key and value fields containing records
     * @param attempt number of previous attempts to write given record. This increases if previous
     * paths already existed and are incompatible.
     * @return organization of given record
     */
    open fun getRecordOrganization(topic: String,
                              record: GenericRecord, attempt: Int): RecordOrganization {
        val keyField = record.get("key") as? GenericRecord
        val valueField = record.get("value") as? GenericRecord

        if (keyField == null || valueField == null) {
            logger.error("Failed to process {}", record)
            throw IllegalArgumentException("Failed to process $record; no key or value")
        }

        val time = getDate(keyField, valueField)

        val relativePath = getRelativePath(topic, keyField, valueField, time, attempt)
        val outputPath = root.resolve(relativePath)
        val category = getCategory(keyField, valueField)
        return RecordOrganization(outputPath, category, time)
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
    abstract fun getRelativePath(topic: String, key: GenericRecord,
                                 value: GenericRecord, time: Instant?, attempt: Int): Path

    /**
     * Get the category of a record, representing a partitioning for a given topic and user.
     * @param key record key
     * @param value record value
     * @return category name.
     */
    abstract fun getCategory(key: GenericRecord, value: GenericRecord): String

    open fun getTimeBin(time: Instant?): String = time
            ?.let { timeBinFormat.format(time)}
            ?: "unknown_date"

    /**
     * Organization of a record.
     */
    data class RecordOrganization(
            /** Path that the record should be stored in. */
            val path: Path,
            /** Category or partition that the record belongs to. */
            val category: String,
            /** Time contained in the record, if any. */
            val time: Instant?)

    companion object {
        private val logger = LoggerFactory.getLogger(RecordPathFactory::class.java)
        private val ILLEGAL_CHARACTER_PATTERN = Pattern.compile("[^a-zA-Z0-9_-]+")

        val HOURLY_TIME_BIN_FORMAT: DateTimeFormatter = DateTimeFormatter
                .ofPattern("yyyyMMdd_HH'00'")
                .withZone(UTC)

        /**
         * Get the date contained in given records
         * @param keyField key field of the record
         * @param valueField value field of the record
         * @return date contained in the values of either record, or `null` if not found or
         * it cannot be parsed.
         */
        fun getDate(keyField: GenericRecord?,
                    valueField: GenericRecord?): Instant? {
            var timeField: Schema.Field?

            if (valueField != null) {
                timeField = valueField.schema.getField("time")
                if (timeField != null && timeField.schema().type == Type.DOUBLE) {
                    val time = valueField.get(timeField.pos()) as Double
                    // Convert from millis to date and apply dateFormat
                    return Instant.ofEpochMilli((time * 1000.0).toLong())
                }
            }

            if (keyField != null) {
                timeField = keyField.schema.getField("timeStart")

                if (timeField != null && timeField.schema().type == Type.DOUBLE) {
                    val time = keyField.get(timeField.pos()) as Double
                    // Convert from millis to date and apply dateFormat
                    return Instant.ofEpochMilli((time * 1000.0).toLong())
                }

                // WindowedKey
                timeField = keyField.schema.getField("start")
                if (timeField != null && timeField.schema().type == Type.LONG) {
                    return Instant.ofEpochMilli(keyField.get("start") as Long)
                }
            }

            if (valueField != null) {
                var result = parseDateTime(valueField)
                if (result != null) {
                    return result
                }
                result = parseDate(valueField)
                if (result != null) {
                    return result
                }
            }

            return null
        }

        /**
         * Parse the dateTime field of a record, if present.
         *
         * @param record record that may contain a dateTime field
         * @return `Instant` representing the dateTime or `null` if the field cannot be
         * found or parsed.
         */
        fun parseDateTime(record: GenericRecord): Instant? {
            // dateTime
            val timeField = record.schema.getField("dateTime")
            if (timeField != null && timeField.schema().type == Type.STRING) {
                val dateTime = record.get(timeField.pos()).toString()
                try {
                    return if (dateTime[dateTime.length - 1] == 'Z') {
                        Instant.parse(dateTime)
                    } else {
                        LocalDateTime.parse(dateTime).toInstant(UTC)
                    }
                } catch (ex: DateTimeParseException) {
                    // try next data type
                }

            }

            return null
        }

        /**
         * Parse the date field of a record, if present.
         *
         * @param record record that may contain a date field
         * @return `Instant` representing the start of given date or `null` if the field
         * cannot be found or parsed.
         */
        fun parseDate(record: GenericRecord): Instant? {
            // dateTime
            val timeField = record.schema.getField("date")
            if (timeField != null && timeField.schema().type == Type.STRING) {
                val date = record.get(timeField.pos()).toString()
                try {
                    return LocalDate.parse(date).atStartOfDay(UTC).toInstant()
                } catch (ex: DateTimeParseException) {
                    // no other options
                }

            }

            return null
        }

        fun sanitizeId(id: Any?, defaultValue: String): String = id
                ?.let { ILLEGAL_CHARACTER_PATTERN.matcher(it.toString()).replaceAll("") }
                ?.takeIf { it.isNotEmpty() }
                ?: defaultValue
    }
}
