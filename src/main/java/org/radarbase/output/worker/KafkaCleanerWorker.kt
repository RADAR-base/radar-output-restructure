package org.radarbase.output.worker

import org.apache.avro.generic.GenericRecord
import org.radarbase.output.FileStoreFactory
import org.radarbase.output.accounting.Accountant
import org.radarbase.output.source.SourceStorage
import org.radarbase.output.source.TopicFile
import org.radarbase.output.util.Timer.time
import org.radarbase.output.worker.RestructureWorker.Companion.extractRecords
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.io.IOException
import java.nio.file.Path
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.atomic.AtomicBoolean

class KafkaCleanerWorker(
        private val fileStoreFactory: FileStoreFactory,
        private val accountant: Accountant,
        private val sourceStorage: SourceStorage,
        private val closed: AtomicBoolean
) : Closeable {
    private val maxFilesPerTopic: Int = fileStoreFactory.config.worker.maxFilesPerTopic ?: Int.MAX_VALUE
    private val pathFactory = fileStoreFactory.pathFactory
    private val reader = sourceStorage.createReader()

    private val deleteThreshold: Instant? = Instant.now().apply { minus(fileStoreFactory.config.cleaner.age.toLong(), ChronoUnit.DAYS) }

    private val cacheStore = ContainsFileCacheStore(fileStoreFactory)

    fun deleteOldFiles(
            topic: String,
            topicPath: Path
    ): Int {
        val offsets = accountant.offsets.copyForTopic(topic)
        return sourceStorage.walker.walkRecords(topic, topicPath)
                .filter { f -> f.lastModified.isBefore(deleteThreshold) &&
                        // ensure that there is a file with a larger offset also
                        // processed, so the largest offset is never removed.
                        offsets.contains(f.range.copy(range = f.range.range.copy(to = f.range.range.to + 1))) }
                .take(maxFilesPerTopic)
                .takeWhile { !closed.get() }
                .count { file ->
                    if (isExtracted(file)) {
                        logger.info("Removing {}", file.path)
                        time("cleaner.delete") {
                            sourceStorage.delete(file.path)
                        }
                        true
                    } else {
                        logger.warn("Source file was not completely extracted: {}", file.path)
                        accountant.remove(file.range)
                        false
                    }
                }
    }

    private fun isExtracted(file: TopicFile): Boolean {
        return reader.newInput(file).use { input ->
            // processing zero-length files may trigger a stall. See:
            // https://github.com/RADAR-base/Restructure-HDFS-topic/issues/3
            if (input.length() == 0L) {
                logger.warn("File {} has zero length, skipping.", file.path)
                return false
            }
            extractRecords(input) { records ->
                records.all { record -> containsRecord(file.topic, record) }
            }
        }
    }

    private fun containsRecord(topic: String, record: GenericRecord): Boolean {
        var suffix = 0

        do {
            val (path) = pathFactory.getRecordOrganization(
                    topic, record, suffix)

            try {
                when (cacheStore.contains(path, record)) {
                    ContainsFileCacheStore.FindResult.FILE_NOT_FOUND -> return false
                    ContainsFileCacheStore.FindResult.NOT_FOUND -> return false
                    ContainsFileCacheStore.FindResult.FOUND -> return true
                    ContainsFileCacheStore.FindResult.BAD_SCHEMA -> suffix += 1  // continue next suffix
                }
            } catch (ex: IOException) {
                logger.error("Failed to read target file for checking data integrity", ex)
                return false
            }
        } while (true)
    }

    override fun close() {
        reader.close()
        cacheStore.close()
    }

    companion object {
        private val logger = LoggerFactory.getLogger(KafkaCleanerWorker::class.java)
    }
}

