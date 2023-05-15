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

import org.slf4j.LoggerFactory

class PathFormatter(
    private val format: String,
    private val plugins: List<PathFormatterPlugin>,
    checkMinimalDistinction: Boolean = true,
) {
    private val parameterLookups: Map<String, suspend PathFormatParameters.() -> String>

    init {
        require(format.isNotBlank()) { "Path format may not be an empty string" }
        val foundParameters = "\\$\\{([^}]*)}".toRegex()
            .findAll(format)
            .mapTo(HashSet()) { it.groupValues[1] }

        parameterLookups = buildMap {
            plugins.forEach { plugin ->
                putAll(
                    try {
                        plugin.createLookupTable(foundParameters)
                    } catch (ex: IllegalArgumentException) {
                        logger.error("Cannot parse path format {}, illegal format parameter found by plugin {}", format, plugin.name, ex)
                        throw ex
                    },
                )
            }
        }
        val unsupportedParameters = foundParameters - parameterLookups.keys
        require(unsupportedParameters.isEmpty()) {
            val allowedFormats = plugins.map { it.allowedFormats }
            "Cannot use path format $format: unknown parameters $unsupportedParameters." +
                " Legal parameter names are parameters $allowedFormats"
        }
        if (checkMinimalDistinction) {
            require("topic" in parameterLookups) { "Path must include topic parameter." }
            require(
                "filename" in parameterLookups ||
                    ("extension" in parameterLookups && "attempt" in parameterLookups),
            ) {
                "Path must include filename parameter or extension and attempt parameters."
            }
        }
    }

    suspend fun format(
        parameters: PathFormatParameters,
    ): String = parameterLookups.asSequence()
        .fold(format) { p, (name, lookup) ->
            p.replace("\${$name}", parameters.lookup())
        }

    override fun toString(): String = "PathFormatter{" +
        "format=$format," +
        "plugins=${plugins.map { it.name }}}"

    companion object {
        private val logger = LoggerFactory.getLogger(PathFormatter::class.java)
    }
}
