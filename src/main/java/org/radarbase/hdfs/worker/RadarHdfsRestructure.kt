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

package org.radarbase.hdfs.worker

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.radarbase.hdfs.FileStoreFactory
import org.radarbase.hdfs.accounting.Accountant
import org.radarbase.hdfs.accounting.OffsetRange
import org.radarbase.hdfs.path.RecordPathFactory
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.io.IOException
import java.lang.Exception
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.LongAdder
import java.util.stream.Collectors
import java.util.stream.Stream

class RadarHdfsRestructure(private val fileStoreFactory: FileStoreFactory): Closeable {
    private val conf: Configuration = fileStoreFactory.config.hdfs.configuration
    private val pathFactory: RecordPathFactory = fileStoreFactory.pathFactory
    private val maxFilesPerTopic: Long = fileStoreFactory.config.worker.maxFilesPerTopic?.toLong()
            ?: java.lang.Long.MAX_VALUE

    private val lockManager = fileStoreFactory.remoteLockManager
    private val excludeTopics: Set<String> = fileStoreFactory.config.topics
            .filter { (_, conf) -> conf.exclude }
            .map { (topic, _) -> topic }
            .toSet()

    private val isClosed = AtomicBoolean(false)

    val processedFileCount = LongAdder()
    val processedRecordsCount = LongAdder()

    @Throws(IOException::class, InterruptedException::class)
    fun process(directoryName: String) {
        // Get files and directories
        val path = Path(directoryName)
        val fs = path.getFileSystem(conf)
        val absolutePath = fs.getFileStatus(path).path  // get absolute file

        logger.info("Scanning topics...")

        val paths = getTopicPaths(fs, absolutePath)

        logger.info("{} topics found", paths.size)

        paths.parallelStream()
                .forEach { p ->
                    try {
                        val (fileCount, recordCount) = mapTopic(fs, p)
                        processedFileCount.add(fileCount)
                        processedRecordsCount.add(recordCount)
                    } catch (ex: Exception) {
                        logger.warn("Failed to map topic", ex)
                    }
                }
    }

    private fun mapTopic(fs: FileSystem, topicPath: Path): ProcessingStatistics {
        if (isClosed.get()) {
            return ProcessingStatistics(0L, 0L)
        }

        val topic = topicPath.name

        return try {
            lockManager.acquireTopicLock(topic)?.use { _ ->
                Accountant(fileStoreFactory, topic).use { accountant ->
                    RestructureWorker(pathFactory, fs, accountant, fileStoreFactory, isClosed).use { worker ->
                        try {
                            val seenFiles = accountant.offsets
                            val topicPaths = TopicFileList(topic, findRecordPaths(fs, topicPath)
                                    .map { f -> TopicFile(topic, f) }
                                    .filter { f -> !seenFiles.contains(f.range) }
                                    .limit(maxFilesPerTopic))

                            if (topicPaths.numberOfFiles > 0) {
                                worker.processPaths(topicPaths)
                            }
                        } catch (ex: Exception) {
                            logger.error("Failed to map files of topic {}", topic, ex)
                        }

                        ProcessingStatistics(worker.processedFileCount, worker.processedRecordsCount)
                    }
                }
            }
        } catch (ex: IOException) {
            logger.error("Failed to map files of topic {}", topic, ex)
            null
        } ?: ProcessingStatistics(0L, 0L)
    }

    override fun close() {
        isClosed.set(true)
    }

    private fun getTopicPaths(fs: FileSystem, path: Path): List<Path> = findTopicPaths(fs, path)
                .distinct()
                .filter { f -> !excludeTopics.contains(f.name) }
                .collect(Collectors.toList())
                .also { it.shuffle() }

    private fun findTopicPaths(fs: FileSystem, path: Path): Stream<Path> {
        val fileStatuses = fs.listStatus(path)
        val avroFile = fileStatuses.find { it.isFile && it.path.name.endsWith(".avro") }
        return if (avroFile != null) {
            Stream.of(avroFile.path.parent.parent)
        } else {
            Stream.of(*fileStatuses)
                    .filter { it.isDirectory && it.path.name != "+tmp" }
                    .flatMap { findTopicPaths(fs, it.path) }
        }
    }

    private fun findRecordPaths(fs: FileSystem, path: Path): Stream<Path> = Stream.of(*fs.listStatus(path))
            .flatMap { fileStatus ->
                val p = fileStatus.path
                val filename = p.name
                when {
                    fileStatus.isDirectory && filename != "+tmp" -> findRecordPaths(fs, p)
                    filename.endsWith(".avro") -> Stream.of(p)
                    else -> Stream.empty()
                }
            }

    internal class TopicFileList(val topic: String, files: Stream<TopicFile>) {
        val files: List<TopicFile> = files.collect(Collectors.toList())
        val numberOfOffsets: Long = this.files.stream()
                .mapToLong { it.size }
                .sum()

        val numberOfFiles: Int = this.files.size
    }

    internal class TopicFile(val topic: String, val path: Path) {
        val range: OffsetRange = OffsetRange.parseFilename(path.name)

        val size: Long = 1 + range.offsetTo - range.offsetFrom
    }

    private data class ProcessingStatistics(val fileCount: Long, val recordCount: Long)

    companion object {
        private val logger = LoggerFactory.getLogger(RadarHdfsRestructure::class.java)
    }
}
