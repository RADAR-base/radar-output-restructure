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

package org.radarbase.output

import com.beust.jcommander.JCommander
import com.beust.jcommander.ParameterException
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.Semaphore
import org.radarbase.output.accounting.*
import org.radarbase.output.cleaner.SourceDataCleaner
import org.radarbase.output.compression.Compression
import org.radarbase.output.config.CommandLineArgs
import org.radarbase.output.config.RestructureConfig
import org.radarbase.output.format.RecordConverterFactory
import org.radarbase.output.path.RecordPathFactory
import org.radarbase.output.source.SourceStorage
import org.radarbase.output.source.SourceStorageFactory
import org.radarbase.output.target.TargetStorage
import org.radarbase.output.target.TargetStorageFactory
import org.radarbase.output.util.Timer
import org.radarbase.output.worker.FileCacheStore
import org.radarbase.output.worker.Job
import org.radarbase.output.worker.RadarKafkaRestructure
import org.slf4j.LoggerFactory
import redis.clients.jedis.JedisPool
import java.io.IOException
import java.text.NumberFormat
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.atomic.LongAdder
import kotlin.io.path.createDirectories
import kotlin.system.exitProcess

/** Main application.  */
class Application(
    config: RestructureConfig,
) : FileStoreFactory {

    override val config = config.apply { validate() }
    override val recordConverter: RecordConverterFactory = config.format.createConverter()
    override val compression: Compression = config.compression.createCompression()
    override val pathFactory: RecordPathFactory = config.paths.createFactory().apply {
        fileStoreFactory = this@Application
        extension = recordConverter.extension + compression.extension
        root = config.paths.output
        addTopicConfiguration(config.topics)
    }

    private val sourceStorageFactory = SourceStorageFactory(config.source, config.paths.temp)
    override val sourceStorage: SourceStorage
        get() = sourceStorageFactory.createSourceStorage()

    override val targetStorage: TargetStorage =
        TargetStorageFactory(config.target).createTargetStorage()

    override val redisHolder: RedisHolder = RedisHolder(JedisPool(config.redis.uri))
    override val remoteLockManager: RemoteLockManager = RedisRemoteLockManager(
        redisHolder,
        config.redis.lockPrefix,
    )

    override val offsetPersistenceFactory: OffsetPersistenceFactory =
        OffsetRedisPersistence(redisHolder)

    override val workerSemaphore = Semaphore(config.worker.numThreads * 2)

    private val jobs: List<Job>

    init {
        val serviceMutex = Mutex()
        jobs = listOfNotNull(
            RadarKafkaRestructure.job(config, serviceMutex),
            SourceDataCleaner.job(config, serviceMutex),
        )
    }

    @Throws(IOException::class)
    override fun newFileCacheStore(accountant: Accountant) = FileCacheStore(this, accountant)

    fun start() {
        System.setProperty(
            "kotlinx.coroutines.scheduler.max.pool.size",
            config.worker.numThreads.toString(),
        )
        System.setProperty(
            "kotlinx.coroutines.scheduler.core.pool.size",
            config.worker.numThreads.toString(),
        )

        try {
            config.paths.temp.createDirectories()
        } catch (ex: IOException) {
            logger.error("Failed to create temporary directory")
            return
        }

        runBlocking {
            launch { targetStorage.initialize() }
        }

        if (config.service.enable) {
            runService()
        } else {
            runBlocking {
                jobs.forEach { job ->
                    launch { job.run(this@Application) }
                }
            }
        }
    }

    private fun runService() {
        logger.info(
            "Running as a Service with poll interval of {} seconds",
            config.service.interval,
        )
        logger.info("Press Ctrl+C to exit...")

        runBlocking {
            jobs.forEach { job ->
                launch { job.schedule(this@Application) }
            }
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(Application::class.java)
        const val CACHE_SIZE_DEFAULT = 100

        internal fun LongAdder.format(): String =
            NumberFormat.getNumberInstance().format(sum())

        private fun parseArgs(args: Array<String>): CommandLineArgs {
            val commandLineArgs = CommandLineArgs()
            JCommander.newBuilder()
                .addObject(commandLineArgs)
                .programName("radar-output-restructure")
                .build().run {
                    try {
                        parse(*args)
                    } catch (ex: ParameterException) {
                        logger.error(ex.message)
                        usage()
                        exitProcess(1)
                    }

                    if (commandLineArgs.help) {
                        usage()
                        exitProcess(0)
                    }
                }

            return commandLineArgs
        }

        @JvmStatic
        fun main(args: Array<String>) {
            val commandLineArgs = parseArgs(args)

            logger.info(
                "Starting at {}...",
                DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(LocalDateTime.now()),
            )

            // Enable singleton timer statements in the code.
            Timer.isEnabled = commandLineArgs.enableTimer

            val application = try {
                Application(
                    RestructureConfig
                        .load(commandLineArgs.configFile)
                        .withEnv()
                        .apply {
                            addArgs(commandLineArgs)
                            validate()
                        }
                )
            } catch (ex: IllegalArgumentException) {
                logger.error("Illegal argument", ex)
                exitProcess(1)
            } catch (ex: IOException) {
                logger.error("Failed to initialize plugins", ex)
                exitProcess(1)
            } catch (e: IllegalStateException) {
                logger.error("Invalid configuration: {}", e.message)
                exitProcess(1)
            }

            application.start()
        }
    }
}
