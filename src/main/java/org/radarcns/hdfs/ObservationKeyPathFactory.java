package org.radarcns.hdfs;

import org.apache.avro.generic.GenericRecord;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;

public class ObservationKeyPathFactory extends RecordPathFactory {
    @Override
    public Path getRelativePath(String topic, GenericRecord key, GenericRecord value, Instant time, int attempt) {
        String projectId = sanitizeId(key.get("projectId"), "unknown-project");
        String userId = sanitizeId(key.get("userId"), "unknown-user");

        String attemptSuffix = attempt == 0 ? "" : "_" + attempt;
        String outputFileName = getTimeBin(time) + attemptSuffix + getExtension();

        return Paths.get(projectId, userId, topic, outputFileName);
    }

    @Override
    public String getCategory(GenericRecord key, GenericRecord value) {
        return sanitizeId(key.get("sourceId"), "unknown-source");
    }
}
