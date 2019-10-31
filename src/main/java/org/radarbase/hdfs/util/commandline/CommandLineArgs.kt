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

package org.radarbase.hdfs.util.commandline

import com.beust.jcommander.Parameter
import com.beust.jcommander.validators.PositiveInteger
import org.radarbase.hdfs.config.RestructureConfig.Companion.RESTRUCTURE_CONFIG_FILE_NAME
import java.util.*

class CommandLineArgs {
    @Parameter(description = "<input_path_1> [<input_path_2> ...]", variableArity = true)
    var inputPaths: List<String>? = null

    @Parameter(names = ["-F", "--config-file"], description = "Config file. By default, $RESTRUCTURE_CONFIG_FILE_NAME is tried.")
    var configFile: String? = null

    @Parameter(names = ["-f", "--format"], description = "Format to use when converting the files. JSON and CSV are available by default.")
    var format: String? = null

    @Parameter(names = ["-c", "--compression"], description = "Compression to use when converting the files. Gzip is available by default.")
    var compression: String? = null

    // Default set to false because causes loss of records from Biovotion data. https://github.com/RADAR-base/Restructure-HDFS-topic/issues/16
    @Parameter(names = ["-d", "--deduplicate"], description = "Boolean to define if to use deduplication or not.")
    var deduplicate: Boolean? = null

    @Parameter(names = ["-n", "--nameservice"], description = "The HDFS name services to connect to. Eg - '<HOST>' for single configurations or <CLUSTER_ID> for high availability web services.", validateWith = [NonEmptyValidator::class])
    var hdfsName: String? = null

    @Parameter(names = ["-o", "--output-directory"], description = "The output folder where the files are to be extracted.", validateWith = [NonEmptyValidator::class])
    var outputDirectory: String? = null

    @Parameter(names = ["-h", "--help"], help = true, description = "Display the usage of the program with available options.")
    var help: Boolean = false

    @Parameter(names = ["-t", "--num-threads"], description = "Number of threads to use for processing", validateWith = [PositiveInteger::class])
    var numThreads: Int? = null

    @Parameter(names = ["--timer"], description = "Enable timers")
    var enableTimer = false

    @Parameter(names = ["--tmp-dir"], description = "Temporary staging directory")
    var tmpDir: String? = null

    @Parameter(names = ["-s", "--cache-size"], description = "Number of files to keep in cache in a single thread.", validateWith = [PositiveInteger::class])
    var cacheSize: Int? = null

    @Parameter(names = ["--max-files-per-topic"], description = "Maximum number of records to process, per topic. Set below 1 to disable this option.")
    var maxFilesPerTopic: Int? = null

    @Parameter(names = ["-S", "--service"], description = "Run the output generation as a service")
    var asService: Boolean? = null

    @Parameter(names = ["-i", "--interval"], description = "Polling interval when running as a service (seconds)")
    var pollInterval: Long? = null
}