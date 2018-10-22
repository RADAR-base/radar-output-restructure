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

package org.radarcns.hdfs;

import static java.time.ZoneOffset.UTC;

import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.regex.Pattern;
import org.apache.avro.Schema;
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

    public RecordOrganization getRecordOrganization(String topic, GenericRecord record, int attempt) {
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

    public abstract Path getRelativePath(String topic, GenericRecord key, GenericRecord value, Instant time, int attempt);

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

    public boolean isTopicPartitioned() {
        return true;
    }

    public DateTimeFormatter getTimeBinFormat() {
        return HOURLY_TIME_BIN_FORMAT;
    }

    public String getTimeBin(Instant time) {
        return time == null ? "unknown_date" : getTimeBinFormat().format(time);
    }

    public static class RecordOrganization {
        private final Path path;
        private final Instant time;
        private final String category;

        public RecordOrganization(Path path, String category, Instant time) {
            this.path = path;
            this.time = time;
            this.category = category;
        }

        public Path getPath() {
            return path;
        }

        public Instant getTime() {
            return time;
        }

        public String getCategory() {
            return category;
        }
    }

    public static Instant getDate(GenericRecord keyField, GenericRecord valueField) {
        Schema.Field timeField = valueField.getSchema().getField("time");
        if (timeField != null && timeField.schema().getType() == Type.DOUBLE) {
            double time = (Double) valueField.get(timeField.pos());
            // Convert from millis to date and apply dateFormat
            return Instant.ofEpochMilli((long) (time * 1000d));
        }
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

        // dateTime
        timeField = valueField.getSchema().getField("dateTime");
        if (timeField != null && timeField.schema().getType() == Type.STRING) {
            String dateTime = valueField.get(timeField.pos()).toString();
            try {
                return Instant.parse(dateTime);
            } catch (DateTimeParseException ex) {
                // try local date
            }
            try {
                return LocalDateTime.parse(dateTime).toInstant(UTC);
            } catch (DateTimeParseException ex) {
                // no other options
            }
        }

        timeField = valueField.getSchema().getField("date");
        if (timeField != null && timeField.schema().getType() == Type.STRING) {
            String date = valueField.get(timeField.pos()).toString();
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
