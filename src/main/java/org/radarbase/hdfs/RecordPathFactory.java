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

package org.radarbase.hdfs;

import static java.time.ZoneOffset.UTC;

import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class RecordPathFactory implements Plugin {
    private static final Logger logger = LoggerFactory.getLogger(RecordPathFactory.class);
    private static final Pattern ILLEGAL_CHARACTER_PATTERN = Pattern.compile("[^a-zA-Z0-9_-]+");

    public static final DateTimeFormatter HOURLY_TIME_BIN_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd_HH'00'")
            .withZone(UTC);

    private Path root;
    private String extension;

    /**
     * Get the organization of given record in given topic.
     * @param topic Kafka topic name
     * @param record record with possible key and value fields containing records
     * @param attempt number of previous attempts to write given record. This increases if previous
     *                paths already existed and are incompatible.
     * @return organization of given record
     */
    @Nonnull
    public RecordOrganization getRecordOrganization(@Nonnull String topic,
            @Nonnull GenericRecord record, int attempt) {
        GenericRecord keyField = (GenericRecord) record.get("key");
        GenericRecord valueField = (GenericRecord) record.get("value");

        if (keyField == null || valueField == null) {
            logger.error("Failed to process {}", record);
            throw new IllegalArgumentException("Failed to process " + record + "; no key or value");
        }

        Instant time = RecordPathFactory.getDate(keyField, valueField);

        Path relativePath = getRelativePath(topic, keyField, valueField, time, attempt);
        Path outputPath = getRoot().resolve(relativePath);
        String category = getCategory(keyField, valueField);
        return new RecordOrganization(outputPath, category, time);
    }

    /**
     * Get the relative path corresponding to given record on given topic.
     * @param topic Kafka topic name
     * @param key record key
     * @param value record value
     * @param time time contained in the record
     * @param attempt number of previous attempts to write given record. This increases if previous
     *                paths already existed and are incompatible.
     * @return relative path corresponding to given parameters.
     */
    @Nonnull
    public abstract Path getRelativePath(@Nonnull String topic, @Nullable GenericRecord key,
            @Nullable GenericRecord value, @Nullable Instant time, int attempt);

    /**
     * Get the category of a record, representing a partitioning for a given topic and user.
     * @param key record key
     * @param value record value
     * @return category name.
     */
    @Nonnull
    public abstract String getCategory(GenericRecord key, GenericRecord value);

    public Path getRoot() {
        return this.root;
    }

    public void setRoot(Path rootDirectory) {
        this.root = rootDirectory;
    }

    public String getExtension() {
        return this.extension;
    }

    public void setExtension(String extension) {
        this.extension = extension;
    }

    public DateTimeFormatter getTimeBinFormat() {
        return HOURLY_TIME_BIN_FORMAT;
    }

    @Nonnull
    public String getTimeBin(@Nullable Instant time) {
        return time == null ? "unknown_date" : getTimeBinFormat().format(time);
    }

    /**
     * Organization of a record.
     */
    public static class RecordOrganization {
        private final Path path;
        private final Instant time;
        private final String category;

        /**
         * Organization of a record.
         *
         * @param path path that the record should be stored in.
         * @param category category or partition that the record belongs to
         * @param time time contained in the record, if any
         */
        public RecordOrganization(@Nonnull Path path, @Nonnull String category,
                @Nullable Instant time) {
            this.path = path;
            this.time = time;
            this.category = category;
        }

        @Nonnull
        public Path getPath() {
            return path;
        }

        @Nullable
        public Instant getTime() {
            return time;
        }

        @Nonnull
        public String getCategory() {
            return category;
        }
    }

    /**
     * Get the date contained in given records
     * @param keyField key field of the record
     * @param valueField value field of the record
     * @return date contained in the values of either record, or {@code null} if not found or
     *         it cannot be parsed.
     */
    @Nullable
    public static Instant getDate(@Nullable GenericRecord keyField,
            @Nullable GenericRecord valueField) {
        Schema.Field timeField;

        if (valueField != null) {
            timeField = valueField.getSchema().getField("time");
            if (timeField != null && timeField.schema().getType() == Type.DOUBLE) {
                double time = (Double) valueField.get(timeField.pos());
                // Convert from millis to date and apply dateFormat
                return Instant.ofEpochMilli((long) (time * 1000d));
            }
        }

        if (keyField != null) {
            timeField = keyField.getSchema().getField("timeStart");

            if (timeField != null && timeField.schema().getType() == Type.DOUBLE) {
                double time = (Double) keyField.get(timeField.pos());
                // Convert from millis to date and apply dateFormat
                return Instant.ofEpochMilli((long) (time * 1000d));
            }

            // WindowedKey
            timeField = keyField.getSchema().getField("start");
            if (timeField != null && timeField.schema().getType() == Type.LONG) {
                return Instant.ofEpochMilli((Long) keyField.get("start"));
            }
        }

        if (valueField != null) {
            Instant result = parseDateTime(valueField);
            if (result != null) {
                return result;
            }
            result = parseDate(valueField);
            if (result != null) {
                return result;
            }
        }

        return null;
    }

    /**
     * Parse the dateTime field of a record, if present.
     *
     * @param record record that may contain a dateTime field
     * @return {@code Instant} representing the dateTime or {@code null} if the field cannot be
     * found or parsed.
     */
    @Nullable
    public static Instant parseDateTime(@Nonnull GenericRecord record) {
        // dateTime
        Field timeField = record.getSchema().getField("dateTime");
        if (timeField != null && timeField.schema().getType() == Type.STRING) {
            String dateTime = record.get(timeField.pos()).toString();
            try {
                if (dateTime.charAt(dateTime.length() - 1) == 'Z') {
                    return Instant.parse(dateTime);
                } else {
                    return LocalDateTime.parse(dateTime).toInstant(UTC);
                }
            } catch (DateTimeParseException ex) {
                // try next data type
            }
        }

        return null;
    }

    /**
     * Parse the date field of a record, if present.
     *
     * @param record record that may contain a date field
     * @return {@code Instant} representing the start of given date or {@code null} if the field
     * cannot be found or parsed.
     */
    @Nullable
    public static Instant parseDate(@Nonnull GenericRecord record) {
        // dateTime
        Field timeField = record.getSchema().getField("date");
        if (timeField != null && timeField.schema().getType() == Type.STRING) {
            String date = record.get(timeField.pos()).toString();
            try {
                return LocalDate.parse(date).atStartOfDay(UTC).toInstant();
            } catch (DateTimeParseException ex) {
                // no other options
            }
        }

        return null;
    }

    public static String sanitizeId(Object id, String defaultValue) {
        if (id == null) {
            return defaultValue;
        }
        String idString = ILLEGAL_CHARACTER_PATTERN.matcher(id.toString()).replaceAll("");
        if (idString.isEmpty()) {
            return defaultValue;
        } else {
            return idString;
        }
    }
}
