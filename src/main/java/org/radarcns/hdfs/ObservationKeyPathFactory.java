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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import org.apache.avro.generic.GenericRecord;

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
