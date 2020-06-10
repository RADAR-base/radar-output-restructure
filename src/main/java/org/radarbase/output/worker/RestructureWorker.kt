package org.radarbase.output.worker

import com.fasterxml.jackson.databind.JsonMappingException
import org.apache.avro.file.DataFileReader
import org.apache.avro.file.SeekableInput
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import org.radarbase.output.FileStoreFactory
import org.radarbase.output.accounting.Accountant
import org.radarbase.output.accounting.OffsetRangeSet
import org.radarbase.output.source.SourceStorage
import org.radarbase.output.source.TopicFile
import org.radarbase.output.source.TopicFileList
import org.radarbase.output.path.RecordPathFactory
import org.radarbase.output.util.ProgressBar
import org.radarbase.output.util.ReadOnlyFunctionalValue
import org.radarbase.output.util.Timer.time
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.io.IOException
import java.io.UncheckedIOException
import java.text.NumberFormat
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.math.roundToLong

internal class RestructureWorker(
        storage: SourceStorage,
        private val accountant: Accountant,
        fileStoreFactory: FileStoreFactory,
        private val closed: AtomicBoolean
): Closeable {
    var processedFileCount: Long = 0
    var processedRecordsCount: Long = 0
    private val reader = storage.createReader()
    private val pathFactory: RecordPathFactory = fileStoreFactory.pathFactory
    private val batchSize = fileStoreFactory.config.worker.cacheOffsetsSize

    private val cacheStore = fileStoreFactory.newFileCacheStore(accountant)

    fun processPaths(topicPaths: TopicFileList) {
        val numFiles = topicPaths.numberOfFiles
        val numOffsets = topicPaths.numberOfOffsets
        val totalProgress = numOffsets ?: numFiles.toLong()

        val topic = topicPaths.topic

        val numberFormat = NumberFormat.getNumberInstance()

        if (numOffsets == null) {
            logger.info("Processing topic {}: converting {} files",
                    topic, numberFormat.format(numFiles))
        } else {
            logger.info("Processing topic {}: converting {} files with {} records",
                    topic, numberFormat.format(numFiles), numberFormat.format(numOffsets))
        }

        val seenOffsets = accountant.offsets
                .withFactory { ReadOnlyFunctionalValue(it) }

        val progressBar = ProgressBar(topic, totalProgress, 50, 5, TimeUnit.SECONDS)
        progressBar.update(0)

        val batchSize = generateBatchSize()
        var currentSize = 0L
        var currentFile = 0L
        try {
            for (file in topicPaths.files) {
                if (closed.get()) {
                    break
                }
                val processedSize = try {
                    this.processFile(file, progressBar, seenOffsets)
                            .also { size ->
                                val expectedSize = file.range.range.size
                                if (expectedSize != null && size != expectedSize) {
                                    logger.warn("File {} contains {} records instead of expected {}",
                                            file.path, size, expectedSize)
                                }
                            }
                } catch (exc: JsonMappingException) {
                    logger.error("Cannot map values", exc)
                    0L
                }

                currentFile += 1
                currentSize += processedSize
                if (currentSize >= batchSize) {
                    currentSize = 0
                    cacheStore.flush()
                }

                processedFileCount++
                if (numOffsets == null) {
                    progressBar.update(currentFile)
                }
            }
        } catch (ex: IOException) {
            logger.error("Failed to process file", ex)
        } catch (ex: UncheckedIOException) {
            logger.error("Failed to process file", ex)
        } catch (ex: IllegalStateException) {
            logger.warn("Shutting down")
        }

        progressBar.update(totalProgress, force = true)
    }

    @Throws(IOException::class)
    private fun processFile(file: TopicFile,
                            progressBar: ProgressBar, seenOffsets: OffsetRangeSet): Long {
        logger.debug("Reading {}", file.path)

        val offset = file.range.range.from

        return reader.newInput(file).use { input ->
            // processing zero-length files may trigger a stall. See:
            // https://github.com/RADAR-base/Restructure-HDFS-topic/issues/3
            if (input.length() == 0L) {
                logger.warn("File {} has zero length, skipping.", file.path)
                return 0L
            }
            val transaction = Accountant.Transaction(file.range.topicPartition, offset, file.lastModified)
            extractRecords(input) { records ->
                val recordsInFile = records.mapIndexed { relativeOffset, record ->
                    transaction.offset = offset + relativeOffset
                    val alreadyContains = time("accounting.check") {
                        seenOffsets.contains(file.range.topicPartition, transaction.offset, transaction.lastModified)
                    }
                    if (!alreadyContains) {
                        // Get the fields
                        this.writeRecord(transaction, record)
                    }
                    processedRecordsCount++
                    if (file.size != null) {
                        progressBar.update(processedRecordsCount)
                    }
                }.count().toLong()

                recordsInFile
            }
        }
    }

    @Throws(IOException::class)
    private fun writeRecord(
            transaction: Accountant.Transaction,
            record: GenericRecord) {
        var currentSuffix = 0
        do {
            val (path) = pathFactory.getRecordOrganization(
                    transaction.topicPartition.topic, record, currentSuffix)

            // Write data
            val response = time("write") {
                cacheStore.writeRecord(path, record, transaction)
            }

            currentSuffix += 1
        } while (!response.isSuccessful)
    }

    private fun generateBatchSize(): Long {
        val modifier = ThreadLocalRandom.current().nextDouble(0.75, 1.25)
        return (batchSize * modifier).roundToLong()
    }

    override fun close() {
        reader.close()
        cacheStore.close()
    }

    companion object {
        private val logger = LoggerFactory.getLogger(RestructureWorker::class.java)

        fun <T> extractRecords(input: SeekableInput, processing: (Sequence<GenericRecord>) -> T): T {
            var tmpRecord: GenericRecord? = null

            return DataFileReader(input, GenericDatumReader<GenericRecord>()).use { reader ->
                processing(generateSequence {
                    time("read") {
                        if (reader.hasNext()) reader.next(tmpRecord) else null
                    }
                }.onEach { tmpRecord = it })
            }
        }
    }
}
