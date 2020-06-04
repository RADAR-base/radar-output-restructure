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

    private val cacheStore = fileStoreFactory.newFileCacheStore(accountant)

    fun processPaths(topicPaths: TopicFileList) {
        val numFiles = topicPaths.numberOfFiles
        val numOffsets = topicPaths.numberOfOffsets

        val topic = topicPaths.topic

        val numberFormat = NumberFormat.getNumberInstance()

        logger.info("Processing topic {}: converting {} files with {} records",
                topic, numberFormat.format(numFiles), numberFormat.format(numOffsets))

        val seenOffsets = accountant.offsets
                .withFactory { ReadOnlyFunctionalValue(it) }

        val progressBar = ProgressBar(topic, numOffsets, 50, 5, TimeUnit.SECONDS)
        progressBar.update(0)

        val batchSize = (BATCH_SIZE * ThreadLocalRandom.current().nextDouble(0.75, 1.25)).roundToLong()
        var currentSize: Long = 0
        try {
            for (file in topicPaths.files) {
                if (closed.get()) {
                    break
                }
                try {
                    this.processFile(file, progressBar, seenOffsets)
                } catch (exc: JsonMappingException) {
                    logger.error("Cannot map values", exc)
                }

                processedFileCount++
                progressBar.update(currentSize)

                currentSize += file.size
                if (currentSize >= batchSize) {
                    currentSize = 0
                    cacheStore.flush()
                }
            }
        } catch (ex: IOException) {
            logger.error("Failed to process file", ex)
        } catch (ex: UncheckedIOException) {
            logger.error("Failed to process file", ex)
        } catch (ex: IllegalStateException) {
            logger.warn("Shutting down")
        }

        progressBar.update(numOffsets, force = true)
    }

    @Throws(IOException::class)
    private fun processFile(file: TopicFile,
                            progressBar: ProgressBar, seenOffsets: OffsetRangeSet) {
        logger.debug("Reading {}", file.path)

        val offset = file.range.range.from

        reader.newInput(file).use { input ->
            // processing zero-length files may trigger a stall. See:
            // https://github.com/RADAR-base/Restructure-HDFS-topic/issues/3
            if (input.length() == 0L) {
                logger.warn("File {} has zero length, skipping.", file.path)
                return
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
                    progressBar.update(processedRecordsCount)
                }.count().toLong()

                if (recordsInFile != file.range.range.size) {
                    logger.warn("File {} contains {} records instead of expected {}",
                            file.path, recordsInFile, file.range.range.size)
                }
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

    override fun close() {
        reader.close()
        cacheStore.close()
    }

    companion object {
        private val logger = LoggerFactory.getLogger(RestructureWorker::class.java)

        /** Number of offsets to process in a single task.  */
        private const val BATCH_SIZE: Long = 500000

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
