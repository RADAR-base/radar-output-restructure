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
import org.radarbase.hdfs.config.HdfsSettings
import org.radarbase.hdfs.config.RestructureSettings
import org.radarbase.hdfs.data.*
import org.radarbase.hdfs.util.ProgressBar.Companion.formatTime
import org.radarbase.hdfs.util.Timer
import org.radarbase.hdfs.util.commandline.CommandLineArgs
import org.slf4j.LoggerFactory
import java.io.IOException
import java.io.UncheckedIOException
import java.nio.file.Files
import java.nio.file.Paths
import java.text.SimpleDateFormat
import java.time.Duration
import java.time.Instant
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.system.exitProcess

/** Main application.  */
class Application private constructor(builder: Builder) : FileStoreFactory {

    override val storageDriver: StorageDriver
    override val recordConverter: RecordConverterFactory
    override val compression: Compression
    override val hdfsSettings: HdfsSettings
    override val pathFactory: RecordPathFactory
    private val inputPaths: List<String>?
    override val settings: RestructureSettings
    private val pollInterval: Int
    private val isService: Boolean
    private var hdfsReader: RadarHdfsRestructure? = null
    private val lockPath: Path?

    override val remoteLockManager: RemoteLockManager
        @Throws(IOException::class)
        get() = HdfsRemoteLockManager(
                lockPath!!.getFileSystem(hdfsSettings.configuration),
                lockPath)

    init {
        this.storageDriver = builder.storageDriver!!
        this.settings = builder.settings
        this.isService = builder.asService
        this.pollInterval = builder.pollInterval

        recordConverter = builder.formatFactory!![settings.format!!]
        compression = builder.compressionFactory!![settings.compression!!]

        pathFactory = builder.pathFactory!!
        val extension = recordConverter.extension + compression.extension
        this.pathFactory.extension = extension
        this.pathFactory.root = settings.outputPath

        this.inputPaths = builder.inputPaths

        hdfsSettings = builder.hdfsSettings
        this.lockPath = builder.lockPath
    }

    @Throws(IOException::class)
    override fun newFileCacheStore(accountant: Accountant): FileCacheStore {
        return FileCacheStore(this, accountant)
    }

    fun start() {
        System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism",
                (settings.numThreads - 1).toString())

        try {
            Files.createDirectories(settings.tempDir)
        } catch (ex: IOException) {
            logger.error("Failed to create temporary directory")
            return
        }

        val executorService = Executors.newSingleThreadScheduledExecutor()
        executorService.execute { hdfsReader = RadarHdfsRestructure(this) }

        if (isService) {
            logger.info("Running as a Service with poll interval of {} seconds", pollInterval)
            logger.info("Press Ctrl+C to exit...")
            executorService.scheduleAtFixedRate({ this.runRestructure() },
                    (pollInterval / 4).toLong(), pollInterval.toLong(), TimeUnit.SECONDS)

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
            executorService.execute { this.runRestructure() }
            executorService.shutdown()
            try {
                executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS)
            } catch (e: InterruptedException) {
                logger.info("Interrupted, shutting down now...")
                Thread.currentThread().interrupt()
            }
        }
    }

    private fun runRestructure() {
        val timeStart = Instant.now()
        try {
            for (input in inputPaths!!) {
                logger.info("In:  {}", input)
                logger.info("Out: {}", pathFactory.root)
                hdfsReader!!.start(input)
            }
        } catch (ex: IOException) {
            logger.error("Processing failed", ex)
        } catch (e: InterruptedException) {
            logger.error("Processing interrupted")
        }

        logger.info("Processed {} files and {} records",
                hdfsReader!!.processedFileCount, hdfsReader!!.processedRecordsCount)
        logger.info("Time taken: {}", formatTime(Duration.between(timeStart, Instant.now())))
    }

    class Builder(val settings: RestructureSettings, val hdfsSettings: HdfsSettings) {
        var storageDriver: StorageDriver? = null
        var pathFactory: RecordPathFactory? = null
        var compressionFactory: CompressionFactory? = null
        var formatFactory: FormatFactory? = null
        val properties = HashMap<String, String>()
        var inputPaths: List<String>? = null
        var asService: Boolean = false
        var pollInterval: Int = 0
        var lockPath: Path? = null

        @Throws(IOException::class)
        fun build(): Application {
            pathFactory = (pathFactory ?: ObservationKeyPathFactory())
                    .also { it.init(properties) }
            storageDriver = (storageDriver ?: LocalStorageDriver())
                    .also { it.init(properties) }
            compressionFactory = (compressionFactory ?: CompressionFactory())
                    .also { it.init(properties) }
            formatFactory = (formatFactory ?: FormatFactory())
                    .also { it.init(properties) }

            return Application(this)
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(Application::class.java)
        const val CACHE_SIZE_DEFAULT = 100

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

            logger.info(SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Date()))
            logger.info("Starting...")

            Timer.isEnabled = commandLineArgs.enableTimer

            if (commandLineArgs.enableTimer) {
                Runtime.getRuntime().addShutdownHook(Thread(
                        { println(Timer) }, "Timer"))
            }

            val application: Application

            try {
                val settings = RestructureSettings.Builder(commandLineArgs.outputDirectory).apply {
                    compression = commandLineArgs.compression
                    cacheSize = commandLineArgs.cacheSize
                    format = commandLineArgs.format
                    doDeduplicate = commandLineArgs.deduplicate
                    tempDir = commandLineArgs.tmpDir?.let { Paths.get(it) }
                    numThreads = commandLineArgs.numThreads
                    maxFilesPerTopic = commandLineArgs.maxFilesPerTopic
                    excludeTopics = commandLineArgs.excludeTopics
                }.build()

                val hdfsSettings = HdfsSettings.Builder(commandLineArgs.hdfsName).apply {
                    hdfsHighAvailability(commandLineArgs.hdfsHa,
                            commandLineArgs.hdfsUri1, commandLineArgs.hdfsUri2)
                }.build()

                application = Builder(settings, hdfsSettings).apply {
                    lockPath = Path(commandLineArgs.lockDirectory)
                    pathFactory = commandLineArgs.pathFactory as RecordPathFactory?
                    compressionFactory = commandLineArgs.compressionFactory as CompressionFactory?
                    formatFactory = commandLineArgs.formatFactory as FormatFactory?
                    storageDriver = commandLineArgs.storageDriver as StorageDriver?
                    properties += commandLineArgs.properties
                    inputPaths=  commandLineArgs.inputPaths
                    asService = commandLineArgs.asService
                    pollInterval = commandLineArgs.pollInterval
                }.build()
            } catch (ex: IllegalArgumentException) {
                logger.error("HDFS High availability name node configuration is incomplete." + " Configure --namenode-1, --namenode-2 and --namenode-ha")
                exitProcess(1)
            } catch (ex: UncheckedIOException) {
                logger.error("Failed to create temporary directory " + commandLineArgs.tmpDir)
                exitProcess(1)
            } catch (ex: IOException) {
                logger.error("Failed to initialize plugins", ex)
                exitProcess(1)
            } catch (e: ClassCastException) {
                logger.error("Cannot find factory", e)
                exitProcess(1)
            }

            application.start()
        }
    }
}
