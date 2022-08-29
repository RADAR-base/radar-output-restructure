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
import java.nio.file.Path
import java.nio.file.Paths

class PathFormatter(
    private val format: String,
    plugins: List<PathFormatterPlugin>
) {
    private val parameterLookups: Map<String, PathFormatParameters.() -> String>

    init {
        val foundParameters = "\\$\\{([^}]*)}".toRegex()
            .findAll(format)
            .map { it.groupValues[1] }
            .toSet()

        parameterLookups = buildMap {
            plugins.forEach { plugin ->
                putAll(
                    try {
                        plugin.createLookupTable(foundParameters)
                    } catch (ex: IllegalArgumentException) {
                        logger.error("Cannot parse path format {}, illegal format parameter found by plugin {}", format, plugin.javaClass, ex)
                        throw ex
                    }
                )
            }
        }
        val unsupportedParameters = foundParameters - parameterLookups.keys
        require(unsupportedParameters.isEmpty()) {
            val allowedFormats = plugins.map { it.allowedFormats }
            "Cannot use path format $format: unknown parameters $unsupportedParameters." +
                " Legal parameter names are parameters $allowedFormats"
        }
        require("topic" in parameterLookups) { "Path must include topic parameter." }
        require(
            "filename" in parameterLookups ||
                ("extension" in parameterLookups && "attempt" in parameterLookups)
        ) {
            "Path must include filename parameter or extension and attempt parameters."
        }
    }

    fun format(
        parameters: PathFormatParameters,
    ): Path {
        val path = parameterLookups.asSequence()
            .fold(format) { p, (name, lookup) ->
                p.replace("\${$name}", parameters.lookup())
            }

        return Paths.get(path)
    }

    companion object {
        private val logger = LoggerFactory.getLogger(PathFormatter::class.java)
    }
}
