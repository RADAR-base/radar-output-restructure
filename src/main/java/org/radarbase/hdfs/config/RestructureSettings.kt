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

package org.radarbase.hdfs.config

import org.radarbase.hdfs.Application.Companion.CACHE_SIZE_DEFAULT
import java.io.IOException
import java.io.UncheckedIOException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

class RestructureSettings private constructor(builder: Builder) {
    val compression: String? = builder.compression
    val isDeduplicate: Boolean = builder.doDeduplicate
    val format: String? = builder.format
    val cacheSize: Int = builder.cacheSize
    val tempDir: Path = builder.tempDir!!
    val outputPath: Path = builder.outputPath
    val numThreads: Int = builder.numThreads
    val maxFilesPerTopic: Int = builder.maxFilesPerTopic
    val excludeTopics: List<String>? = builder.excludeTopics

    class Builder(outputPath: String) {
        var numThreads = 1
            set(value) {
                require(value >= 1) { "Number of threads must be at least 1" }
                field = value
            }
        var compression: String? = null
        var doDeduplicate: Boolean = false
        var format: String? = null
        var cacheSize = CACHE_SIZE_DEFAULT
            set(value) {
                require(value >= 1) { "Cannot use cache size smaller than 1" }
                field = value
            }

        var tempDir: Path? = null
        val outputPath: Path = Paths.get(outputPath.replace("/+$".toRegex(), ""))
        var maxFilesPerTopic: Int = 0
        var excludeTopics: List<String>? = null

        fun build(): RestructureSettings {
            compression = compression ?: "identity"
            format = format ?: "csv"
            tempDir = tempDir ?: try {
                        Files.createTempDirectory("radar-hdfs-restructure")
                    } catch (ex: IOException) {
                        throw UncheckedIOException("Cannot create temporary directory", ex)
                    }
            excludeTopics = excludeTopics ?: emptyList()

            return RestructureSettings(this)
        }
    }
}
