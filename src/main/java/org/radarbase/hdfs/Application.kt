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

import com.beust.jcommander.JCommander
import com.beust.jcommander.ParameterException
import org.apache.hadoop.fs.Path
import org.radarbase.hdfs.accounting.Accountant
import org.radarbase.hdfs.accounting.HdfsRemoteLockManager
import org.radarbase.hdfs.accounting.RemoteLockManager
import org.radarbase.hdfs.config.MutableRestructureConfig
import org.radarbase.hdfs.config.RestructureConfig
import org.radarbase.hdfs.data.Compression
import org.radarbase.hdfs.data.FileCacheStore
import org.radarbase.hdfs.data.RecordConverterFactory
import org.radarbase.hdfs.data.StorageDriver
import org.radarbase.hdfs.util.ProgressBar.Companion.format
import org.radarbase.hdfs.util.Timer
import org.radarbase.hdfs.util.commandline.CommandLineArgs
import org.slf4j.LoggerFactory
import java.io.IOException
import java.nio.file.Files
import java.text.NumberFormat
import java.time.Duration
import java.time.Instant
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.Temporal
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.system.exitProcess

/** Main application.  */
class Application(
        override val config: RestructureConfig,
        private val isService: Boolean
) : FileStoreFactory {
    override val pathFactory: RecordPathFactory = config.pathFactory.factory
    override val recordConverter: RecordConverterFactory = config.formatFactory.factory[config.format]
    override val compression: Compression = config.compressionFactory.factory[config.compression]
    override val storageDriver: StorageDriver = config.storageDriver.factory
    private val lockPath: Path = Path(config.hdfs.lockDirectory)

    override val remoteLockManager: RemoteLockManager
        @Throws(IOException::class)
        get() = HdfsRemoteLockManager(
                lockPath.getFileSystem(config.hdfs.configuration),
                lockPath)

    init {
        pathFactory.apply {
            extension = recordConverter.extension + compression.extension
            root = config.outputPath
        }
    }

    @Throws(IOException::class)
    override fun newFileCacheStore(accountant: Accountant): FileCacheStore {
        return FileCacheStore(this, accountant)
    }

    fun start() {
        System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism",
                (config.numThreads - 1).toString())

        try {
            Files.createDirectories(config.tempPath)
        } catch (ex: IOException) {
            logger.error("Failed to create temporary directory")
            return
        }

        if (isService) {
            logger.info("Running as a Service with poll interval of {} seconds", config.interval)
            logger.info("Press Ctrl+C to exit...")
            val executorService = Executors.newSingleThreadScheduledExecutor()

            executorService.scheduleAtFixedRate(::runRestructure,
                    config.interval / 4, config.interval, TimeUnit.SECONDS)

            try {
                Thread.sleep(java.lang.Long.MAX_VALUE)
            } catch (e: InterruptedException) {
                logger.info("Interrupted, shutting down...")
                executorService.shutdownNow()
                try {
                    executorService.awaitTermination(java.lang.Long.MAX_VALUE, TimeUnit.SECONDS)
                    Thread.currentThread().interrupt()
                } catch (ex: InterruptedException) {
                    logger.info("Interrupted again...")
                }
            }
        } else {
            runRestructure()
        }
    }

    private fun runRestructure() {
        val timeStart = Instant.now()
        try {
            RadarHdfsRestructure(this).use { restructure ->
                for (input in config.inputPaths) {
                    logger.info("In:  {}", input)
                    logger.info("Out: {}", pathFactory.root)
                    restructure.process(input.toString())
                }

                val numberFormat = NumberFormat.getNumberInstance()
                logger.info("Processed {} files and {} records in {}",
                        numberFormat.format(restructure.processedFileCount),
                        numberFormat.format(restructure.processedRecordsCount),
                        timeStart.durationSince().format())
            }
        } catch (ex: IOException) {
            logger.error("Processing failed", ex)
        } catch (e: InterruptedException) {
            logger.error("Processing interrupted")
        } finally {
            // Print timings and reset the timings for the next iteration.
            println(Timer)
            Timer.reset()
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(Application::class.java)
        const val CACHE_SIZE_DEFAULT = 100

        private fun Temporal.durationSince() = Duration.between(this, Instant.now())

        @JvmStatic
        fun main(args: Array<String>) {
            val commandLineArgs = CommandLineArgs()
            val parser = JCommander.newBuilder().addObject(commandLineArgs).build()

            parser.programName = "radar-hdfs-restructure"
            try {
                parser.parse(*args)
            } catch (ex: ParameterException) {
                logger.error(ex.message)
                parser.usage()
                exitProcess(1)
            }

            if (commandLineArgs.help) {
                parser.usage()
                exitProcess(0)
            }

            logger.info("Starting at {}...",
                    DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(LocalDateTime.now()))

            // Enable singleton timer statements in the code.
            Timer.isEnabled = commandLineArgs.enableTimer

            val application = try {
                val restructureConfig = MutableRestructureConfig.load(commandLineArgs.configFile).apply {
                    commandLineArgs.compression?.let { compression = it }
                    commandLineArgs.cacheSize?.let { cacheSize = it }
                    commandLineArgs.format?.let { format = it }
                    commandLineArgs.deduplicate?.let { deduplicate = it }
                    commandLineArgs.tmpDir?.let { tempPath = it }
                    commandLineArgs.numThreads?.let { numThreads = it }
                    commandLineArgs.maxFilesPerTopic?.let { maxFilesPerTopic = it }
                    commandLineArgs.pollInterval?.let { interval = it }
                    inputPaths = commandLineArgs.inputPaths
                    commandLineArgs.outputDirectory?.let { outputPath = it }
                    commandLineArgs.hdfsName?.let { hdfs.name = it}
                }.toRestructureConfig()

                Application(restructureConfig, commandLineArgs.asService)
            } catch (ex: IllegalArgumentException) {
                logger.error("HDFS High availability name node configuration is incomplete." + " Configure --namenode-1, --namenode-2 and --namenode-ha")
                exitProcess(1)
            } catch (ex: IOException) {
                logger.error("Failed to initialize plugins", ex)
                exitProcess(1)
            } catch (e: IllegalStateException) {
                logger.error("Cannot process configuration", e)
                exitProcess(1)
            }

            application.start()
        }
    }
}
