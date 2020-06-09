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

import org.radarbase.output.FileStoreFactory
import org.radarbase.output.accounting.Accountant
import org.radarbase.output.accounting.OffsetRangeSet
import org.radarbase.output.source.TopicFileList
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.io.IOException
import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.LongAdder

/**
 * Performs the following actions
 * - Recursively scans target directory for any avro files
 *    - Deduces the topic name from two directories up
 *    - Continue until all files have been scanned
 * - In separate threads, start worker for all topics
 *    - Acquire a lock before processing to avoid multiple processing of files
 */
class RadarKafkaRestructure(
        private val fileStoreFactory: FileStoreFactory
): Closeable {
    private val maxFilesPerTopic: Int = fileStoreFactory.config.worker.maxFilesPerTopic ?: Int.MAX_VALUE
    private val sourceStorage = fileStoreFactory.sourceStorage

    private val lockManager = fileStoreFactory.remoteLockManager
    private val excludeTopics: Set<String> = fileStoreFactory.config.topics
            .mapNotNullTo(HashSet()) { (topic, conf) ->
                topic.takeIf { conf.exclude }
            }

    private val isClosed = AtomicBoolean(false)

    val processedFileCount = LongAdder()
    val processedRecordsCount = LongAdder()

    @Throws(IOException::class, InterruptedException::class)
    fun process(directoryName: String) {
        // Get files and directories
        val absolutePath = Paths.get(directoryName)

        logger.info("Scanning topics...")

        val paths = topicPaths(absolutePath)

        logger.info("{} topics found", paths.size)

        paths.parallelStream()
                .forEach { p ->
                    try {
                        val (fileCount, recordCount) = mapTopic(p)
                        processedFileCount.add(fileCount)
                        processedRecordsCount.add(recordCount)
                    } catch (ex: Exception) {
                        logger.warn("Failed to map topic", ex)
                    }
                }
    }

    private fun mapTopic(topicPath: Path): ProcessingStatistics {
        if (isClosed.get()) {
            return ProcessingStatistics(0L, 0L)
        }

        val topic = topicPath.fileName.toString()

        return try {
            lockManager.acquireTopicLock(topic)?.use {
                Accountant(fileStoreFactory, topic).use { accountant ->
                    startWorker(topic, topicPath, accountant, accountant.offsets)
                }
            }
        } catch (ex: IOException) {
            logger.error("Failed to map files of topic {}", topic, ex)
            null
        } ?: ProcessingStatistics(0L, 0L)
    }

    private fun startWorker(
            topic: String,
            topicPath: Path,
            accountant: Accountant,
            seenFiles: OffsetRangeSet): ProcessingStatistics {
        return RestructureWorker(sourceStorage, accountant, fileStoreFactory, isClosed).use { worker ->
            try {
                val topicPaths = TopicFileList(topic, sourceStorage.walker.walkRecords(topic, topicPath)
                        .filter { f -> !seenFiles.contains(f.range) }
                        .take(maxFilesPerTopic)
                        .toList())

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
        isClosed.set(true)
    }

    private fun topicPaths(root: Path): List<Path> = sourceStorage.walker.walkTopics(root, excludeTopics)
                .toMutableList()
                // different services start on different topics to decrease lock contention
                .also { it.shuffle() }

    private data class ProcessingStatistics(
            val fileCount: Long,
            val recordCount: Long)

    companion object {
        private val logger = LoggerFactory.getLogger(RadarKafkaRestructure::class.java)
    }
}
