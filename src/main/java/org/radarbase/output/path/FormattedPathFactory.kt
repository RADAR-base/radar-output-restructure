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
import org.slf4j.LoggerFactory
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Instant
import java.time.ZoneOffset.UTC
import java.time.format.DateTimeFormatter

open class FormattedPathFactory : RecordPathFactory() {
    lateinit var format: String
    lateinit var timeParameters: Map<String, DateTimeFormatter>

    override fun init(properties: Map<String, String>) {
        super.init(properties)

        format = properties["format"]
            ?: run {
                logger.warn("Path format not provided, using {} instead", DEFAULT_FORMAT)
                DEFAULT_FORMAT
            }

        val parameters = "\\$\\{([^}]*)}".toRegex()
            .findAll(format)
            .map { it.groupValues[1] }
            .toSet()

        timeParameters = parameters
            .filter { it.startsWith("time:") }
            .associateWith { p ->
                DateTimeFormatter
                    .ofPattern(p.removePrefix("time:"))
                    .withZone(UTC)
            }

        val parameterNames = knownParameters + timeParameters.keys

        val illegalParameters = parameters.filterNot { it in parameterNames }
        if (illegalParameters.isNotEmpty()) {
            throw IllegalArgumentException(
                "Cannot use path format $format: unknown parameters $illegalParameters." +
                    " Legal parameter names are time formats (e.g., \${time:YYYYmmDD}" +
                    " or the following: $knownParameters",
            )
        }
        if ("topic" !in parameters) {
            throw IllegalArgumentException("Path must include topic parameter.")
        }
        if ("filename" !in parameters && ("extension" !in parameters || "attempt" !in parameters)) {
            throw IllegalArgumentException(
                "Path must include filename parameter or extension and attempt parameters."
            )
        }
    }

    override fun getRelativePath(
        topic: String,
        key: GenericRecord,
        value: GenericRecord,
        time: Instant?,
        attempt: Int,
    ): Path {
        val attemptSuffix = if (attempt == 0) "" else "_$attempt"

        val templatedParameters = mutableMapOf(
            "projectId" to sanitizeId(key.get("projectId"), "unknown-project"),
            "userId" to sanitizeId(key.get("userId"), "unknown-user"),
            "sourceId" to sanitizeId(key.get("sourceId"), "unknown-source"),
            "topic" to topic,
            "filename" to getTimeBin(time) + attemptSuffix + extension,
            "attempt" to attemptSuffix,
            "extension" to extension,
        )

        templatedParameters += if (time != null) {
            timeParameters.mapValues { (_, formatter) -> formatter.format(time) }
        } else {
            timeParameters.mapValues { "unknown-time" }
        }

        val path = templatedParameters.asSequence()
            .fold(format) { p, (name, value) ->
                p.replace("\${$name}", value)
            }

        return Paths.get(path)
    }

    override fun getCategory(key: GenericRecord, value: GenericRecord): String {
        return sanitizeId(key.get("sourceId"), "unknown-source")
    }

    companion object {
        private const val DEFAULT_FORMAT = "\${projectId}/\${userId}/\${topic}/\${filename}"

        private val knownParameters = setOf(
            "filename",
            "topic",
            "projectId",
            "userId",
            "sourceId",
            "attempt",
            "extension",
        )

        private val logger = LoggerFactory.getLogger(FormattedPathFactory::class.java)
    }
}
