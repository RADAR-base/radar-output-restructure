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

package org.radarbase.hdfs

import java.nio.file.Path
import java.nio.file.Paths
import java.time.Instant
import org.apache.avro.generic.GenericRecord

open class ObservationKeyPathFactory : RecordPathFactory() {
    override fun getRelativePath(topic: String, key: GenericRecord,
                                 value: GenericRecord, time: Instant?, attempt: Int): Path {
        val projectId = sanitizeId(key.get("projectId"), "unknown-project")
        val userId = sanitizeId(key.get("userId"), "unknown-user")

        val attemptSuffix = if (attempt == 0) "" else "_$attempt"
        val outputFileName = getTimeBin(time) + attemptSuffix + extension

        return Paths.get(projectId, userId, topic, outputFileName)
    }

    override fun getCategory(key: GenericRecord, value: GenericRecord): String {
        return sanitizeId(key.get("sourceId"), "unknown-source")
    }
}
