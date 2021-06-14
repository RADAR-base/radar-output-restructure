package org.radarbase.output.cleaner

import org.radarbase.output.FileStoreFactory
import org.radarbase.output.accounting.Accountant
import org.radarbase.output.util.Timer
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.io.IOException
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.LongAdder

class SourceDataCleaner(
        private val fileStoreFactory: FileStoreFactory
) : Closeable {
    private val isClosed = AtomicBoolean(false)
    private val lockManager = fileStoreFactory.remoteLockManager
    private val sourceStorage = fileStoreFactory.sourceStorage
    private val excludeTopics: Set<String> = fileStoreFactory.config.topics
            .mapNotNullTo(HashSet()) { (topic, conf) ->
                topic.takeIf { conf.excludeFromDelete }
            }
    private val maxFilesPerTopic: Int = fileStoreFactory.config.cleaner.maxFilesPerTopic ?: Int.MAX_VALUE
    private val deleteThreshold: Instant? = Instant.now()
            .minus(fileStoreFactory.config.cleaner.age.toLong(), ChronoUnit.DAYS)

    val deletedFileCount = LongAdder()

    @Throws(IOException::class, InterruptedException::class)
    fun process(directoryName: String) {
        // Get files and directories
        val absolutePath = Paths.get(directoryName)

        val paths = topicPaths(absolutePath)

        logger.info("{} topics found", paths.size)

        paths.parallelStream()
                .forEach { p ->
                    try {
                        val deleteCount = mapTopic(p)
                        if (deleteCount > 0) {
                            logger.info("Removed {} files in topic {}", deleteCount, p.fileName)
                            deletedFileCount.add(deleteCount)
                        }
                    } catch (ex: Exception) {
                        logger.warn("Failed to map topic", ex)
                    }
                }
    }

    private fun mapTopic(topicPath: Path): Long {
        if (isClosed.get()) {
            return 0L
        }

        val topic = topicPath.fileName.toString()

        return try {
            lockManager.tryRunLocked(topic) {
                Accountant(fileStoreFactory, topic).use { accountant ->
                    TimestampExtractionCheck(sourceStorage, fileStoreFactory).use { extractionCheck ->
                        deleteOldFiles(accountant, extractionCheck, topic, topicPath).toLong()
                    }
                }
            }
        } catch (ex: IOException) {
            logger.error("Failed to map files of topic {}", topic, ex)
            0L
        } ?: 0L
    }

    private fun deleteOldFiles(
            accountant: Accountant,
            extractionCheck: ExtractionCheck,
            topic: String,
            topicPath: Path
    ): Int {
        val offsets = accountant.offsets.copyForTopic(topic)
        return sourceStorage.walker.walkRecords(topic, topicPath)
                .filter { f ->
                    f.lastModified.isBefore(deleteThreshold) &&
                            // ensure that there is a file with a larger offset also
                            // processed, so the largest offset is never removed.
                            offsets.contains(f.range
                                    .mapRange { r ->
                                        r.copy(to = r.to?.let { it + 1 })
                                    })
                }
                .take(maxFilesPerTopic)
                .takeWhile { !isClosed.get() }
                .count { file ->
                    if (extractionCheck.isExtracted(file)) {
                        logger.info("Removing {}", file.path)
                        Timer.time("cleaner.delete") {
                            sourceStorage.delete(file.path)
                        }
                        true
                    } else {
                        logger.warn("Source file was not completely extracted: {}", file.path)
                        // extract the file again at a later time
                        accountant.remove(file.range.mapRange { it.ensureToOffset() })
                        false
                    }
                }
    }

    private fun topicPaths(path: Path): List<Path> = sourceStorage.walker.walkTopics(path, excludeTopics)
            .toMutableList()
            // different services start on different topics to decrease lock contention
            .also { it.shuffle() }

    override fun close() {
        isClosed.set(true)
    }

    companion object {
        private val logger = LoggerFactory.getLogger(SourceDataCleaner::class.java)
    }
}
