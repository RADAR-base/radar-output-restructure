package org.radarbase.output.cleaner

import org.apache.avro.generic.GenericRecord
import org.radarbase.output.FileStoreFactory
import org.radarbase.output.source.SourceStorage
import org.radarbase.output.source.TopicFile
import org.radarbase.output.worker.RestructureWorker
import org.slf4j.LoggerFactory
import java.io.IOException

class TimestampExtractionCheck(
        sourceStorage: SourceStorage,
        fileStoreFactory: FileStoreFactory
) : ExtractionCheck {
    private val cacheStore = TimestampFileCacheStore(fileStoreFactory)
    private val reader = sourceStorage.createReader()
    private val pathFactory = fileStoreFactory.pathFactory
    private val batchSize = fileStoreFactory.config.worker.cacheOffsetsSize

    private var cachedRecords = 0L

    override fun isExtracted(file: TopicFile): Boolean {
        val result = reader.newInput(file).use { input ->
            // processing zero-length files may trigger a stall. See:
            // https://github.com/RADAR-base/Restructure-HDFS-topic/issues/3
            if (input.length() == 0L) {
                logger.warn("File {} has zero length, skipping.", file.path)
                return false
            }
            RestructureWorker.extractRecords(input) { records ->
                records.all { record ->
                    cachedRecords += 1
                    containsRecord(file.topic, record)
                }
            }
        }
        if (cachedRecords > batchSize) {
            cachedRecords = 0L
            cacheStore.clear()
        }
        return result
    }

    override fun close() {
        reader.close()
    }


    private fun containsRecord(topic: String, record: GenericRecord): Boolean {
        var suffix = 0

        do {
            val (path) = pathFactory.getRecordOrganization(
                    topic, record, suffix)

            try {
                when (cacheStore.contains(path, record)) {
                    TimestampFileCacheStore.FindResult.FILE_NOT_FOUND -> return false
                    TimestampFileCacheStore.FindResult.NOT_FOUND -> return false
                    TimestampFileCacheStore.FindResult.FOUND -> return true
                    TimestampFileCacheStore.FindResult.BAD_SCHEMA -> suffix += 1  // continue next suffix
                }
            } catch (ex: IOException) {
                logger.error("Failed to read target file {} for checking data integrity", path, ex)
                return false
            }
        } while (true)
    }

    companion object {
        private val logger = LoggerFactory.getLogger(TimestampExtractionCheck::class.java)
    }
}
