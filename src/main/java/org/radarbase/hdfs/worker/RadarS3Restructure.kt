///*
// * Copyright 2017 The Hyve
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.radarbase.hdfs.worker
//
//import io.minio.MinioClient
//import org.radarbase.hdfs.FileStoreFactory
//import org.radarbase.hdfs.accounting.Accountant
//import org.radarbase.hdfs.accounting.OffsetRange
//import org.radarbase.hdfs.config.S3Config
//import org.radarbase.hdfs.path.RecordPathFactory
//import org.slf4j.LoggerFactory
//import java.io.Closeable
//import java.io.IOException
//import java.lang.Exception
//import java.nio.file.Path
//import java.nio.file.Paths
//import java.util.concurrent.atomic.AtomicBoolean
//import java.util.concurrent.atomic.LongAdder
//import java.util.stream.Collectors
//import java.util.stream.Stream
//import kotlin.streams.asStream
//
///**
// * Performs the folling actions
// * - Scans target directory for any
// */
//class RadarS3Restructure(private val fileStoreFactory: FileStoreFactory): Closeable {
//    private val conf: S3Config = requireNotNull(fileStoreFactory.config.s3)
//    private val pathFactory: RecordPathFactory = fileStoreFactory.pathFactory
//    private val maxFilesPerTopic: Long = fileStoreFactory.config.worker.maxFilesPerTopic?.toLong()
//            ?: java.lang.Long.MAX_VALUE
//
//    private val lockManager = fileStoreFactory.remoteLockManager
//    private val excludeTopics: Set<String> = fileStoreFactory.config.topics
//            .filter { (_, conf) -> conf.exclude }
//            .map { (topic, _) -> topic }
//            .toSet()
//
//    private val isClosed = AtomicBoolean(false)
//
//    val processedFileCount = LongAdder()
//    val processedRecordsCount = LongAdder()
//
//    private val s3Client: MinioClient
//
//    init {
//        s3Client = MinioClient(conf.endpoint, conf.accessToken, conf.secretKey)
//    }
//
//    @Throws(IOException::class, InterruptedException::class)
//    fun process(directoryName: String) {
//        // Get files and directories
//        val absolutePath = directoryName
//
//        logger.info("Scanning topics...")
//
//        val paths = getTopicPaths(absolutePath)
//
//        logger.info("{} topics found", paths.size)
//
//        paths.parallelStream()
//                .forEach { p ->
//                    try {
//                        val (fileCount, recordCount) = mapTopic(fs, p)
//                        processedFileCount.add(fileCount)
//                        processedRecordsCount.add(recordCount)
//                    } catch (ex: Exception) {
//                        logger.warn("Failed to map topic", ex)
//                    }
//                }
//    }
//
//    private fun mapTopic(topicPath: Path): ProcessingStatistics {
//        if (isClosed.get()) {
//            return ProcessingStatistics(0L, 0L)
//        }
//
//        val topic = topicPath.fileName.toString()
//
//        return try {
//            lockManager.acquireTopicLock(topic)?.use { _ ->
//                Accountant(fileStoreFactory, topic).use { accountant ->
//                    RestructureWorker(pathFactory, s3Client, accountant, fileStoreFactory, isClosed).use { worker ->
//                        try {
//                            val seenFiles = accountant.offsets
//                            val topicPaths = TopicFileList(topic, findRecordPaths(fs, topicPath)
//                                    .map { f -> TopicFile(topic, f) }
//                                    .filter { f -> !seenFiles.contains(f.range) }
//                                    .limit(maxFilesPerTopic))
//
//                            if (topicPaths.numberOfFiles > 0) {
//                                worker.processPaths(topicPaths)
//                            }
//                        } catch (ex: Exception) {
//                            logger.error("Failed to map files of topic {}", topic, ex)
//                        }
//
//                        ProcessingStatistics(worker.processedFileCount, worker.processedRecordsCount)
//                    }
//                }
//            }
//        } catch (ex: IOException) {
//            logger.error("Failed to map files of topic {}", topic, ex)
//            null
//        } ?: ProcessingStatistics(0L, 0L)
//    }
//
//    override fun close() {
//        isClosed.set(true)
//    }
//
//    private fun getTopicPaths(path: String): List<Path> = findTopicPaths(path)
//                .distinct()
//                .filter { f -> !excludeTopics.contains(f.fileName.toString()) }
//                .collect(Collectors.toList())
//                .also { it.shuffle() }
//
//    private fun findTopicPaths(path: String): Stream<Path> {
//        val fileStatuses = s3Client.listObjects(conf.bucket, path)
//
//        val avroFile = fileStatuses.asSequence()
//                .map { it.get() }
//                .find {  !it.isDir && it.objectName().endsWith(".avro") }
//
//        return if (avroFile != null) {
//            Stream.of(Paths.get(avroFile.objectName()).parent.parent)
//        } else {
//            fileStatuses.asSequence().asStream()
//                    .map { it.get() }
//                    .filter { it.isDir && !it.objectName().endsWith("/+tmp") }
//                    .flatMap { findTopicPaths(it.objectName()) }
//        }
//    }
//
//    private fun findRecordPaths(path: Path): Stream<Path> = s3Client.listObjects(conf.bucket, path.toString())
//            .asSequence()
//            .asStream()
//            .flatMap { result ->
//                val item = result.get()
//                val p = Paths.get(item.objectName())
//                val filename = p.fileName.toString()
//                when {
//                    item.isDir && filename != "+tmp" -> findRecordPaths(p)
//                    filename.endsWith(".avro") -> Stream.of(p)
//                    else -> Stream.empty()
//                }
//            }
//
//    internal class TopicFileList(val topic: String, files: Stream<TopicFile>) {
//        val files: List<TopicFile> = files.collect(Collectors.toList())
//        val numberOfOffsets: Long = this.files.stream()
//                .mapToLong { it.size }
//                .sum()
//
//        val numberOfFiles: Int = this.files.size
//    }
//
//    internal class TopicFile(val topic: String, val path: Path) {
//        val range: OffsetRange = OffsetRange.parseFilename(path.name)
//        val size: Long = 1 + range.offsetTo - range.offsetFrom
//    }
//
//    private data class ProcessingStatistics(val fileCount: Long, val recordCount: Long)
//
//    companion object {
//        private val logger = LoggerFactory.getLogger(RadarS3Restructure::class.java)
//    }
//}
