package org.radarcns.hdfs;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

import static org.radarcns.hdfs.RecordPathFactory.sanitizeId;

public class ObservationKeyPathFactory implements RecordPathFactory {
    private static final Logger logger = LoggerFactory.getLogger(ObservationKeyPathFactory.class);

    private String extension = "";
    private Path root = Paths.get(System.getProperty("user.dir"));

    @Override
    public RecordOrganization getRecordPath(String topic, GenericRecord record, int attempt) {
        GenericRecord keyField = (GenericRecord) record.get("key");
        GenericRecord valueField = (GenericRecord) record.get("value");

        if (keyField == null || valueField == null) {
            logger.error("Failed to process {}", record);
            throw new IllegalArgumentException("Failed to process " + record + "; no key or value");
        }

        Instant time = RecordPathFactory.getDate(keyField, valueField);

        String projectId = sanitizeId(keyField.get("projectId"), "unknown-project");
        String userId = sanitizeId(keyField.get("userId"), "unknown-user");
        String outputFileName = createFilename(time, attempt);

        Path outputPath = this.root.resolve(
                Paths.get(projectId, userId, topic, outputFileName));
        String category = sanitizeId(keyField.get("sourceId"), "unknown-source");
        return new RecordOrganization(outputPath, category, time);
    }

    @Override
    public Path getRoot() {
        return this.root;
    }

    @Override
    public void setRoot(Path rootDirectory) {
        this.root = rootDirectory;
    }

    @Override
    public String getExtension() {
        return this.extension;
    }

    @Override
    public void setExtension(String extension) {
        this.extension = extension;
    }

    @Override
    public DateTimeFormatter getTimeBinFormat() {
        return HOURLY_TIME_BIN_FORMAT;
    }
}
