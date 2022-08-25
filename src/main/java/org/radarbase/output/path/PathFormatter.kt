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

import org.apache.avro.generic.GenericRecord
import org.radarbase.output.path.RecordPathFactory.Companion.sanitizeId
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Instant
import java.time.ZoneOffset.UTC
import java.time.format.DateTimeFormatter

class PathFormatter(
    private val format: String,
) {
    private val timeParameters: Map<String, DateTimeFormatter>
    private val keyParameters: Map<String, List<String>>
    private val valueParameters: Map<String, List<String>>
    private val fixedParameters: Set<String>

    init {
        val foundParameters = "\\$\\{([^}]*)}".toRegex()
            .findAll(format)
            .map { it.groupValues[1] }
            .toSet()

        timeParameters = foundParameters
            .filter { it.startsWith("time:") }
            .associateWith { p ->
                DateTimeFormatter
                    .ofPattern(p.removePrefix("time:"))
                    .withZone(UTC)
            }

        keyParameters = foundParameters
            .filter { it.startsWith("key:") }
            .associateWith { it.removePrefix("key:").split('.') }

        valueParameters = foundParameters
            .filter { it.startsWith("value:") }
            .associateWith { it.removePrefix("value:").split('.') }

        fixedParameters = buildSet {
            addAll(foundParameters)
            removeAll(timeParameters.keys)
            removeAll(keyParameters.keys)
            removeAll(valueParameters.keys)
        }

        val unsupportedParameters = fixedParameters.filterNot { it in supportedFixedParameters }
        require(unsupportedParameters.isEmpty()) {
            "Cannot use path format $format: unknown parameters $unsupportedParameters." +
                " Legal parameter names are time formats (e.g., \${time:YYYYmmDD}" +
                " or the following: $supportedFixedParameters"
        }
        require("topic" in fixedParameters) { "Path must include topic parameter." }
        require("filename" in fixedParameters || ("extension" in fixedParameters && "attempt" in fixedParameters)) {
            "Path must include filename parameter or extension and attempt parameters."
        }
    }

    fun format(
        topic: String,
        key: GenericRecord,
        value: GenericRecord,
        time: Instant?,
        attempt: Int,
        extension: String,
        computeTimeBin: (time: Instant?) -> String,
    ): Path {
        val attemptSuffix = if (attempt == 0) "" else "_$attempt"

        val templatedParameters = mutableMapOf(
            "projectId" to sanitizeId(key.get("projectId"), "unknown-project"),
            "userId" to sanitizeId(key.get("userId"), "unknown-user"),
            "sourceId" to sanitizeId(key.get("sourceId"), "unknown-source"),
            "topic" to topic,
            "filename" to computeTimeBin(time) + attemptSuffix + extension,
            "attempt" to attemptSuffix,
            "extension" to extension,
        )

        timeParameters.mapValuesTo(templatedParameters) { (_, formatter) ->
            sanitizeId(time?.let { formatter.format(it) }, "unknown-time")
        }
        keyParameters.mapValuesTo(templatedParameters) { (_, index) ->
            sanitizeId(key.lookup(index), "unknown-key")
        }
        valueParameters.mapValuesTo(templatedParameters) { (_, index) ->
            sanitizeId(value.lookup(index), "unknown-value")
        }

        val path = templatedParameters.asSequence()
            .fold(format) { p, (name, value) ->
                p.replace("\${$name}", value)
            }

        return Paths.get(path)
    }

    companion object {
        private val supportedFixedParameters = setOf(
            "filename",
            "topic",
            "projectId",
            "userId",
            "sourceId",
            "attempt",
            "extension",
        )

        private fun GenericRecord.lookup(index: List<String>): Any? =
            index.fold<String, Any?>(this) { r, item ->
                r?.let { (it as? GenericRecord)?.get(item) }
            }
    }
}
