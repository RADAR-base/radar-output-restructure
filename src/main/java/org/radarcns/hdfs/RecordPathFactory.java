package org.radarcns.hdfs;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.nio.file.Path;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.regex.Pattern;

import static java.time.ZoneOffset.UTC;

public interface RecordPathFactory extends Plugin {
    DateTimeFormatter HOURLY_TIME_BIN_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd_HH'00'")
            .withZone(UTC);
    Pattern ILLEGAL_CHARACTER_PATTERN = Pattern.compile("[^a-zA-Z0-9_-]+");

    RecordOrganization getRecordPath(String topic, GenericRecord record, int attempt);

    Path getRoot();

    void setRoot(Path rootDirectory);

    String getExtension();

    void setExtension(String extension);

    DateTimeFormatter getTimeBinFormat();

    default String getTimeBin(Instant time) {
        return time == null ? "unknown_date" : getTimeBinFormat().format(time);
    }

    default String createFilename(Instant time, int attempt) {
        String fileSuffix = (attempt == 0 ? "" : "_" + attempt) + getExtension();
        return getTimeBin(time) + fileSuffix;
    }

    class RecordOrganization {
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

    static Instant getDate(GenericRecord keyField, GenericRecord valueField) {
        Schema.Field timeField = valueField.getSchema().getField("time");
        if (timeField != null) {
            double time = (Double) valueField.get(timeField.pos());
            // Convert from millis to date and apply dateFormat
            return Instant.ofEpochMilli((long) (time * 1000d));
        }

        // WindowedKey
        timeField = keyField.getSchema().getField("start");
        if (timeField == null) {
            return null;
        }
        return Instant.ofEpochMilli((Long) keyField.get("start"));
    }

    static String sanitizeId(Object id, String defaultValue) {
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
