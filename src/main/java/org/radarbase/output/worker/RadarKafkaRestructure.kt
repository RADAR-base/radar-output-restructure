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
import org.radarbase.output.source.TopicFile
import org.radarbase.output.source.TopicFileList
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.io.IOException
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Instant
import java.time.temporal.ChronoUnit
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
    private val kafkaStorage = fileStoreFactory.sourceStorage

    private val lockManager = fileStoreFactory.remoteLockManager
    private val excludeTopics: Set<String> = fileStoreFactory.config.topics
            .mapNotNullTo(HashSet()) { (topic, conf) ->
                topic.takeIf { conf.exclude }
            }

    private val excludeDeleteTopics: Set<String> = fileStoreFactory.config.topics
            .mapNotNullTo(HashSet()) { (topic, conf) ->
                topic.takeIf { conf.excludeFromDelete }
            }

    private val deleteThreshold: Instant? = fileStoreFactory.config.service.deleteAfterDays
            .takeIf { it > 0 }
            ?.let { Instant.now().apply { minus(it.toLong(), ChronoUnit.DAYS) } }

    private val isClosed = AtomicBoolean(false)

    val processedFileCount = LongAdder()
    val processedRecordsCount = LongAdder()
    val deletedFileCount = LongAdder()

    @Throws(IOException::class, InterruptedException::class)
    fun process(directoryName: String) {
        // Get files and directories
        val absolutePath = Paths.get(directoryName)

        logger.info("Scanning topics...")

        val paths = getTopicPaths(absolutePath)

        logger.info("{} topics found", paths.size)

        paths.parallelStream()
                .forEach { p ->
                    try {
                        val (fileCount, recordCount, deleteCount) = mapTopic(p)
                        processedFileCount.add(fileCount)
                        processedRecordsCount.add(recordCount)
                        deletedFileCount.add(deleteCount.toLong())
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
                    val seenFiles = accountant.offsets

                    val statistics = startWorker(topic, topicPath, accountant, seenFiles)
                    if (deleteThreshold != null && topic !in excludeDeleteTopics) {
                        statistics.deleteCount = deleteOldFiles(topic, topicPath, seenFiles)
                    }
                    statistics
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
        return RestructureWorker(kafkaStorage, accountant, fileStoreFactory, isClosed).use { worker ->
            try {
                val topicPaths = TopicFileList(topic, findRecordPaths(topic, topicPath)
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

    private fun deleteOldFiles(
            topic: String,
            topicPath: Path,
            seenFiles: OffsetRangeSet
    ) = findRecordPaths(topic, topicPath)
            .filter { f -> f.lastModified.isBefore(deleteThreshold) &&
                    // ensure that there is a file with a larger offset also
                    // processed, so the largest offset is never removed.
                    if (f.range.range.to != null) {
                        seenFiles.contains(f.range.copy(range = f.range.range.copy(to = f.range.range.to + 1)))
                    } else {
                        seenFiles.contains(f.range.topicPartition, f.range.range.from, f.range.range.lastProcessed)
                    }
            }
            .take(maxFilesPerTopic)
            .map { kafkaStorage.delete(it.path) }
            .count()

    private fun findTopicPaths(path: Path): Sequence<Path> {
        val fileStatuses = kafkaStorage.list(path)
        val avroFile = fileStatuses.find {  !it.isDirectory && it.path.fileName.toString().endsWith(".avro", true) }

        return if (avroFile != null) {
            sequenceOf(avroFile.path.parent.parent)
        } else {
            fileStatuses.asSequence()
                    .filter { it.isDirectory && it.path.fileName.toString() != "+tmp" }
                    .flatMap { findTopicPaths(it.path) }
        }
    }

    private fun findRecordPaths(topic: String, path: Path): Sequence<TopicFile> = kafkaStorage.list(path)
            .flatMap { status ->
                val filename = status.path.fileName.toString()
                when {
                    status.isDirectory && filename != "+tmp" -> findRecordPaths(topic, status.path)
                    filename.endsWith(".avro") -> sequenceOf(TopicFile(topic, status.path, status.lastModified))
                    else -> emptySequence()
                }
            }

    override fun close() {
        isClosed.set(true)
    }

    private fun getTopicPaths(path: Path): List<Path> = findTopicPaths(path)
                .distinct()
                .filter { it.fileName.toString() !in excludeTopics }
                .toMutableList()
                // different services start on different topics to decrease lock contention
                .also { it.shuffle() }

    private data class ProcessingStatistics(
            val fileCount: Long,
            val recordCount: Long,
            var deleteCount: Int = 0)

    companion object {
        private val logger = LoggerFactory.getLogger(RadarKafkaRestructure::class.java)
    }
}
