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

package org.radarbase.hdfs

import com.fasterxml.jackson.databind.JsonMappingException
import java.io.IOException
import java.io.UncheckedIOException
import java.text.NumberFormat
import java.util.concurrent.Executors
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.LongAdder
import java.util.stream.Collectors
import java.util.stream.Stream
import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.FsInput
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.radarbase.hdfs.accounting.Accountant
import org.radarbase.hdfs.accounting.OffsetRange
import org.radarbase.hdfs.accounting.OffsetRangeSet
import org.radarbase.hdfs.accounting.RemoteLockManager
import org.radarbase.hdfs.accounting.TopicPartition
import org.radarbase.hdfs.data.FileCacheStore
import org.radarbase.hdfs.util.ProgressBar
import org.radarbase.hdfs.util.ReadOnlyFunctionalValue
import org.radarbase.hdfs.util.Timer
import org.slf4j.LoggerFactory
import kotlin.math.roundToLong

class RadarHdfsRestructure(private val fileStoreFactory: FileStoreFactory) {
    private val numThreads: Int = fileStoreFactory.settings.numThreads
    private val conf: Configuration = fileStoreFactory.hdfsSettings.configuration
    private val pathFactory: RecordPathFactory = fileStoreFactory.pathFactory
    private val maxFilesPerTopic: Long = fileStoreFactory.settings.maxFilesPerTopic.toLong()
            .takeIf { it >= 1 }
            ?: java.lang.Long.MAX_VALUE

    private val excludeTopics: List<String>? = fileStoreFactory.settings.excludeTopics

    var processedFileCount = LongAdder()
        private set
    var processedRecordsCount = LongAdder()
        private set

    @Volatile
    private var lockManager: RemoteLockManager? = null

    @Throws(IOException::class, InterruptedException::class)
    fun start(directoryName: String) {
        // Get files and directories
        val path = Path(directoryName)
        val fs = path.getFileSystem(conf)
        val absolutePath = fs.getFileStatus(path).path  // get absolute file

        val paths = getTopicPaths(fs, absolutePath)

        logger.info("Processing topics:{}", paths.stream()
                .map { p -> "\n  - " + p.name }
                .collect(Collectors.joining()))

        processedFileCount = LongAdder()
        processedRecordsCount = LongAdder()
        lockManager = fileStoreFactory.remoteLockManager

        val executor = Executors.newWorkStealingPool(this.numThreads)

        paths.forEach { p ->
            executor.execute {
                val topic = p.name
                try {
                    lockManager!!.acquireTopicLock(topic)!!.use {
                        Accountant(fileStoreFactory, topic).use { accountant ->
                            // Get filenames to process
                            val seenFiles = accountant.offsets
                            val topicPaths = TopicFileList(walk(fs, p)
                                    .filter { f -> f.name.endsWith(".avro") }
                                    .map { f -> TopicFile(topic, f) }
                                    .filter { f -> !seenFiles.contains(f.range) }
                                    .limit(maxFilesPerTopic))

                            processPaths(topicPaths, accountant)
                        }
                    }
                } catch (ex: IOException) {
                    logger.error("Failed to map files of topic {}", topic, ex)
                }
            }
        }

        executor.shutdown()
        executor.awaitTermination(java.lang.Long.MAX_VALUE, TimeUnit.SECONDS)
    }

    private fun getTopicPaths(fs: FileSystem, path: Path): List<Path> {
        return findTopicPaths(fs, path)
                .distinct()
                .filter { f -> !excludeTopics!!.contains(f.name) }
                .collect(Collectors.toList())
                .also { it.shuffle() }
    }

    private fun walk(fs: FileSystem, path: Path): Stream<Path> {
        val files: Array<FileStatus>
        try {
            files = fs.listStatus(path)
        } catch (e: IOException) {
            throw UncheckedIOException(e)
        }

        return Stream.of(*files).parallel()
                .flatMap { f ->
                    when {
                        f.path.name == "+tmp" -> Stream.empty()
                        f.isDirectory -> walk(fs, f.path)
                        else -> Stream.of(f.path)
                    }
                }
    }

    private fun findTopicPaths(fs: FileSystem, path: Path): Stream<Path> {
        val files: Array<FileStatus>
        try {
            files = fs.listStatus(path)
        } catch (e: IOException) {
            throw UncheckedIOException(e)
        }

        return Stream.of(*files)
                .parallel()
                .flatMap<Path> { f ->
                    val p = f.path
                    val filename = p.name
                    when {
                        filename == "+tmp" -> Stream.empty()
                        f.isDirectory -> findTopicPaths(fs, p)
                        filename.endsWith(".avro") -> Stream.of(p.parent.parent)
                        else -> Stream.empty()
                    }
                }
                .distinct()
    }

    private fun processPaths(topicPaths: TopicFileList, accountant: Accountant) {
        val numFiles = topicPaths.numberOfFiles
        val numOffsets = topicPaths.numberOfOffsets

        val topic = topicPaths.files[0].topic

        logger.info("Processing topic {}: converting {} files with {} records",
                topic, numFiles, NumberFormat.getNumberInstance().format(numOffsets))

        val seenOffsets = accountant.offsets
                .withFactory { ReadOnlyFunctionalValue(it) }

        // TODO: proper progressbar management
        val progressBar = ProgressBar(topic, numOffsets, 50, 500, TimeUnit.MILLISECONDS)
        progressBar.update(0)

        // Actually process the files
        val size = NumberFormat.getNumberInstance().format(topicPaths.numberOfOffsets)
        logger.info("Processing {} records for topic {}", size, topic)
        val batchSize = (BATCH_SIZE * ThreadLocalRandom.current().nextDouble(0.75, 1.25)).roundToLong()
        var currentSize: Long = 0
        try {
            fileStoreFactory.newFileCacheStore(accountant).use { cache ->
                for (file in topicPaths.files) {
                    try {
                        this.processFile(file, cache, progressBar, seenOffsets)
                    } catch (exc: JsonMappingException) {
                        logger.error("Cannot map values", exc)
                    }

                    processedFileCount.increment()

                    currentSize += file.size
                    if (currentSize >= batchSize) {
                        currentSize = 0
                        cache.flush()
                    }
                }
            }
        } catch (ex: IOException) {
            logger.error("Failed to process file", ex)
        } catch (ex: UncheckedIOException) {
            logger.error("Failed to process file", ex)
        } catch (ex: IllegalStateException) {
            logger.warn("Shutting down")
        }

        progressBar.update(numOffsets)
    }

    @Throws(IOException::class)
    private fun processFile(file: TopicFile, cache: FileCacheStore,
                            progressBar: ProgressBar, seenOffsets: OffsetRangeSet) {
        logger.debug("Reading {}", file.path)

        // Read and parseFilename avro file
        val input = FsInput(file.path, conf)

        // processing zero-length files may trigger a stall. See:
        // https://github.com/RADAR-base/Restructure-HDFS-topic/issues/3
        if (input.length() == 0L) {
            logger.warn("File {} has zero length, skipping.", file.path)
            return
        }

        val timer = Timer

        var timeRead = System.nanoTime()
        val dataFileReader = DataFileReader(input,
                GenericDatumReader<GenericRecord>())

        var tmpRecord: GenericRecord? = null
        var offset = file.range.offsetFrom
        while (dataFileReader.hasNext()) {
            val record = dataFileReader.next(tmpRecord)
                    .also { tmpRecord = it }
            timer.add("read", timeRead)

            val timeAccount = System.nanoTime()
            val alreadyContains = seenOffsets.contains(file.range.topicPartition, offset)
            timer.add("accounting.create", timeAccount)
            if (!alreadyContains) {
                // Get the fields
                this.writeRecord(file.range.topicPartition, record, cache, offset, 0)
            }
            processedRecordsCount.increment()
            progressBar.update(processedRecordsCount.sum())

            offset++
            timeRead = System.nanoTime()
        }
    }

    @Throws(IOException::class)
    private fun writeRecord(topicPartition: TopicPartition, record: GenericRecord?,
                            cache: FileCacheStore, offset: Long, suffix: Int) {
        var currentSuffix = suffix
        val (path) = pathFactory.getRecordOrganization(
                topicPartition.topic, record!!, currentSuffix)

        val timer = Timer

        val timeAccount = System.nanoTime()
        val transaction = Accountant.Transaction(topicPartition, offset)
        timer.add("accounting.create", timeAccount)

        // Write data
        val timeWrite = System.nanoTime()
        val response = cache.writeRecord(
                path, record, transaction)
        timer.add("write", timeWrite)

        if (!response.isSuccessful) {
            // Write was unsuccessful due to different number of columns,
            // try again with new file name
            writeRecord(topicPartition, record, cache, offset, ++currentSuffix)
        }
    }

    private class TopicFileList(files: Stream<TopicFile>) {
        val files: List<TopicFile> = files.collect(Collectors.toList())
        val numberOfOffsets: Long = this.files.stream()
                .mapToLong { it.size }
                .sum()

        val numberOfFiles: Int = this.files.size
    }

    private class TopicFile(val topic: String, val path: Path) {
        val range: OffsetRange = OffsetRange.parseFilename(path.name)

        val size: Long = 1 + range.offsetTo - range.offsetFrom
    }

    companion object {
        private val logger = LoggerFactory.getLogger(RadarHdfsRestructure::class.java)

        /** Number of offsets to process in a single task.  */
        private const val BATCH_SIZE: Long = 500000
    }
}
