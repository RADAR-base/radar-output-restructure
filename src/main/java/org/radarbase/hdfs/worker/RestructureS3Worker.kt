//package org.radarbase.hdfs.worker
//
//import com.fasterxml.jackson.databind.JsonMappingException
//import io.minio.MinioClient
//import org.apache.avro.file.DataFileReader
//import org.apache.avro.generic.GenericDatumReader
//import org.apache.avro.generic.GenericRecord
//import org.apache.avro.mapred.FsInput
//import org.apache.hadoop.fs.FileSystem
//import org.radarbase.hdfs.FileStoreFactory
//import org.radarbase.hdfs.accounting.Accountant
//import org.radarbase.hdfs.accounting.OffsetRangeSet
//import org.radarbase.hdfs.accounting.TopicPartition
//import org.radarbase.hdfs.config.S3Config
//import org.radarbase.hdfs.path.RecordPathFactory
//import org.radarbase.hdfs.util.ProgressBar
//import org.radarbase.hdfs.util.ReadOnlyFunctionalValue
//import org.radarbase.hdfs.util.Timer.time
//import org.slf4j.LoggerFactory
//import java.io.Closeable
//import java.io.IOException
//import java.io.UncheckedIOException
//import java.text.NumberFormat
//import java.util.concurrent.ThreadLocalRandom
//import java.util.concurrent.TimeUnit
//import java.util.concurrent.atomic.AtomicBoolean
//import kotlin.math.roundToLong
//
//internal class RestructureS3Worker(
//        private val pathFactory: RecordPathFactory,
//        private val s3Client: MinioClient,
//        private val accountant: Accountant,
//        fileStoreFactory: FileStoreFactory,
//        private val closed: AtomicBoolean
//): Closeable {
//    private val conf: S3Config = requireNotNull(fileStoreFactory.config.s3)
//    var processedFileCount: Long = 0
//    var processedRecordsCount: Long = 0
//
//    private val cacheStore = fileStoreFactory.newFileCacheStore(accountant)
//
//    fun processPaths(topicPaths: RadarS3Restructure.TopicFileList) {
//        val numFiles = topicPaths.numberOfFiles
//        val numOffsets = topicPaths.numberOfOffsets
//
//        val topic = topicPaths.topic
//
//        val numberFormat = NumberFormat.getNumberInstance()
//
//        logger.info("Processing topic {}: converting {} files with {} records",
//                topic, numberFormat.format(numFiles), numberFormat.format(numOffsets))
//
//        val seenOffsets = accountant.offsets
//                .withFactory { ReadOnlyFunctionalValue(it) }
//
//        val progressBar = ProgressBar(topic, numOffsets, 50, 5, TimeUnit.SECONDS)
//        progressBar.update(0)
//
//        val batchSize = (BATCH_SIZE * ThreadLocalRandom.current().nextDouble(0.75, 1.25)).roundToLong()
//        var currentSize: Long = 0
//        try {
//            for (file in topicPaths.files) {
//                if (closed.get()) {
//                    break
//                }
//                try {
//                    this.processFile(file, progressBar, seenOffsets)
//                } catch (exc: JsonMappingException) {
//                    logger.error("Cannot map values", exc)
//                }
//
//                processedFileCount++
//                progressBar.update(currentSize)
//
//                currentSize += file.size
//                if (currentSize >= batchSize) {
//                    currentSize = 0
//                    cacheStore.flush()
//                }
//            }
//        } catch (ex: IOException) {
//            logger.error("Failed to process file", ex)
//        } catch (ex: UncheckedIOException) {
//            logger.error("Failed to process file", ex)
//        } catch (ex: IllegalStateException) {
//            logger.warn("Shutting down")
//        }
//
//        progressBar.update(numOffsets)
//    }
//
//    @Throws(IOException::class)
//    private fun processFile(file: RadarHdfsRestructure.TopicFile,
//                            progressBar: ProgressBar, seenOffsets: OffsetRangeSet) {
//        logger.debug("Reading {}", file.path)
//
//        val offset = file.range.offsetFrom
//
//        s3Client.getObject(conf.bucket, file.path.toString(), )
//
//        extractRecords(file.path) { relativeOffset, record ->
//            val currentOffset = offset + relativeOffset
//            val alreadyContains = time("accounting.check") {
//                seenOffsets.contains(file.range.topicPartition, currentOffset)
//            }
//            if (!alreadyContains) {
//                // Get the fields
//                this.writeRecord(file.range.topicPartition, record, currentOffset)
//            }
//            processedRecordsCount++
//            progressBar.update(processedRecordsCount)
//        }
//    }
//
//    private fun extractRecords(input: FsInput, processing: (Int, GenericRecord) -> Unit) {
//        var tmpRecord: GenericRecord? = null
//
//        DataFileReader(input, GenericDatumReader<GenericRecord>()).use {
//            reader -> generateSequence {
//            time("read") {
//                if (reader.hasNext()) reader.next(tmpRecord) else null
//            }
//        }.onEach { tmpRecord = it }.forEachIndexed(processing)
//        }
//    }
//
//    @Throws(IOException::class)
//    private fun writeRecord(topicPartition: TopicPartition, record: GenericRecord,
//                            offset: Long, suffix: Int = 0) {
//        var currentSuffix = suffix
//        val (path) = pathFactory.getRecordOrganization(
//                topicPartition.topic, record, currentSuffix)
//
//        val transaction = time("accounting.create") {
//            Accountant.Transaction(topicPartition, offset)
//        }
//
//        // Write data
//        val response = time("write") {
//            cacheStore.writeRecord(path, record, transaction)
//        }
//
//        if (!response.isSuccessful) {
//            // Write was unsuccessful due to different number of columns,
//            // try again with new file name
//            writeRecord(topicPartition, record, offset, ++currentSuffix)
//        }
//    }
//
//    override fun close() {
//        cacheStore.close()
//    }
//
//    companion object {
//        private val logger = LoggerFactory.getLogger(RestructureS3Worker::class.java)
//
//        /** Number of offsets to process in a single task.  */
//        private const val BATCH_SIZE: Long = 500000
//    }
//}
