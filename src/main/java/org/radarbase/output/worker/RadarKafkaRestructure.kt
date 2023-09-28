/*
 * Copyright 2017 The Hyve
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

package org.radarbase.output.worker

import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withContext
import org.radarbase.output.Application.Companion.format
import org.radarbase.output.FileStoreFactory
import org.radarbase.output.accounting.Accountant
import org.radarbase.output.accounting.AccountantImpl
import org.radarbase.output.accounting.OffsetRangeSet
import org.radarbase.output.config.RestructureConfig
import org.radarbase.output.source.StorageIndex
import org.radarbase.output.source.TopicFileList
import org.radarbase.output.util.SuspendedCloseable.Companion.useSuspended
import org.radarbase.output.util.TimeUtil.durationSince
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.io.IOException
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Duration
import java.util.concurrent.atomic.LongAdder
import kotlin.coroutines.coroutineContext

/**
 * Performs the following actions
 * - Recursively scans target directory for any avro files
 *    - Deduces the topic name from two directories up
 *    - Continue until all files have been scanned
 * - In separate threads, start worker for all topics
 *    - Acquire a lock before processing to avoid multiple processing of files
 */
class RadarKafkaRestructure(
    private val fileStoreFactory: FileStoreFactory,
) : Closeable {
    private val sourceStorage = fileStoreFactory.sourceStorage

    private val lockManager = fileStoreFactory.remoteLockManager

    private val excludeTopics: Set<String>
    private val maxFilesPerTopic: Int
    private val minimumFileAge: Duration

    private val supervisor = SupervisorJob()

    init {
        val config = fileStoreFactory.config
        excludeTopics = config.topics
            .mapNotNullTo(HashSet()) { (topic, conf) ->
                topic.takeIf { conf.exclude }
            }

        val workerConfig = config.worker
        maxFilesPerTopic = workerConfig.maxFilesPerTopic ?: Int.MAX_VALUE
        minimumFileAge = Duration.ofSeconds(workerConfig.minimumFileAge.coerceAtLeast(0L))
    }

    val processedFileCount = LongAdder()
    val processedRecordsCount = LongAdder()

    @Throws(IOException::class, InterruptedException::class)
    suspend fun process(directoryName: String, storageIndex: StorageIndex) {
        // Get files and directories
        val absolutePath = Paths.get(directoryName)

        logger.info("Scanning topics...")

        val paths = topicPaths(storageIndex, absolutePath)

        logger.info("{} topics found", paths.size)

        withContext(coroutineContext + supervisor) {
            paths.forEach { p ->
                launch {
                    try {
                        val (fileCount, recordCount) = fileStoreFactory.workerSemaphore.withPermit {
                            mapTopic(storageIndex, p)
                        }
                        processedFileCount.add(fileCount)
                        processedRecordsCount.add(recordCount)
                    } catch (ex: Throwable) {
                        logger.warn("Failed to map topic {}", p, ex)
                    }
                }
            }
        }
    }

    private suspend fun mapTopic(storageIndex: StorageIndex, topicPath: Path): ProcessingStatistics {
        val topic = topicPath.fileName.toString()

        return try {
            val statistics = lockManager.tryWithLock(topic) {
                coroutineScope {
                    AccountantImpl(fileStoreFactory, topic).useSuspended { accountant ->
                        accountant.initialize(this)
                        startWorker(storageIndex, topic, topicPath, accountant, accountant.offsets)
                    }
                }
            }
            if (statistics == null) {
                logger.info("Skipping topic {}. It is locked by another process", topicPath)
            }
            statistics
        } catch (ex: IOException) {
            logger.error("Failed to map files of topic {}", topic, ex)
            null
        } ?: ProcessingStatistics(0L, 0L)
    }

    private suspend fun startWorker(
        storageIndex: StorageIndex,
        topic: String,
        topicPath: Path,
        accountant: Accountant,
        seenFiles: OffsetRangeSet,
    ): ProcessingStatistics {
        return RestructureWorker(
            sourceStorage,
            accountant,
            fileStoreFactory,
        ).useSuspended { worker ->
            try {
                val topicPaths = TopicFileList(
                    topic,
                    sourceStorage.listTopicFiles(storageIndex, topic, topicPath, maxFilesPerTopic) { f ->
                        !seenFiles.contains(f.range) &&
                            f.lastModified.durationSince() >= minimumFileAge
                    },
                )

                if (topicPaths.numberOfFiles > 0) {
                    worker.processPaths(topicPaths)
                }
            } catch (ex: Exception) {
                logger.error("Failed to map files of topic {}", topic, ex)
            }

            ProcessingStatistics(worker.processedFileCount, worker.processedRecordsCount)
        }
    }

    override fun close() {
        supervisor.cancel()
    }

    private suspend fun topicPaths(storageIndex: StorageIndex, root: Path): List<Path> =
        sourceStorage.listTopics(storageIndex, root, excludeTopics)
            // different services start on different topics to decrease lock contention
            .shuffled()

    private data class ProcessingStatistics(
        val fileCount: Long,
        val recordCount: Long,
    )

    companion object {
        private val logger = LoggerFactory.getLogger(RadarKafkaRestructure::class.java)

        fun job(config: RestructureConfig, serviceMutex: Mutex): Job? = if (config.worker.enable) {
            Job("restructure", config.service.interval, ::runRestructure, serviceMutex)
        } else {
            null
        }

        private suspend fun runRestructure(factory: FileStoreFactory) {
            RadarKafkaRestructure(factory).useSuspended { restructure ->
                for ((input, index) in factory.storageIndexManagers) {
                    index.update()
                    logger.info("In:  {}", input)
                    logger.info(
                        "Out: bucket {} (default {}) - path {}",
                        factory.pathFactory.pathConfig.bucket?.format,
                        factory.pathFactory.pathConfig.bucket?.defaultName,
                        factory.pathFactory.pathConfig.path.format,
                    )
                    restructure.process(input.toString(), index.storageIndex)
                }

                logger.info(
                    "Processed {} files and {} records",
                    restructure.processedFileCount.format(),
                    restructure.processedRecordsCount.format(),
                )
            }
        }
    }
}
