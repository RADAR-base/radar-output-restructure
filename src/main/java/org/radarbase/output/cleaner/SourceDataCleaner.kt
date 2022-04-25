package org.radarbase.output.cleaner

import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.sync.withPermit
import org.radarbase.output.Application.Companion.format
import org.radarbase.output.FileStoreFactory
import org.radarbase.output.accounting.Accountant
import org.radarbase.output.accounting.AccountantImpl
import org.radarbase.output.config.RestructureConfig
import org.radarbase.output.util.ResourceContext.Companion.resourceContext
import org.radarbase.output.util.SuspendedCloseable.Companion.useSuspended
import org.radarbase.output.util.Timer
import org.radarbase.output.worker.Job
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
    private val scope = CoroutineScope(Dispatchers.Default)

    @Throws(IOException::class, InterruptedException::class)
    suspend fun process(directoryName: String) {
        // Get files and directories
        val absolutePath = Paths.get(directoryName)

        val paths = topicPaths(absolutePath)

        logger.info("{} topics found", paths.size)

        paths.map { p ->
            scope.launch {
                try {
                    val deleteCount = fileStoreFactory.workerSemaphore.withPermit {
                        mapTopic(p)
                    }
                    if (deleteCount > 0) {
                        logger.info("Removed {} files in topic {}", deleteCount, p.fileName)
                        deletedFileCount.add(deleteCount)
                    }
                } catch (ex: Exception) {
                    logger.warn("Failed to map topic", ex)
                }
            }
        }.joinAll()
    }

    private suspend fun mapTopic(topicPath: Path): Long {
        if (isClosed.get()) {
            return 0L
        }

        val topic = topicPath.fileName.toString()
        return try {
            lockManager.tryWithLock(topic) {
                coroutineScope {
                    resourceContext {
                        val accountant = createResource { AccountantImpl(fileStoreFactory, topic) }
                                .apply { initialize(this@coroutineScope) }
                        val extractionCheck = createResource { TimestampExtractionCheck(sourceStorage, fileStoreFactory) }
                        deleteOldFiles(accountant, extractionCheck, topic, topicPath).toLong()
                    }
                }
            }
        } catch (ex: IOException) {
            logger.error("Failed to map files of topic {}", topic, ex)
            0L
        } ?: 0L
    }

    private suspend fun deleteOldFiles(
        accountant: Accountant,
        extractionCheck: ExtractionCheck,
        topic: String,
        topicPath: Path
    ): Int {
        val offsets = accountant.offsets.copyForTopic(topic)

        val paths = sourceStorage.listTopicFiles(topic, topicPath, maxFilesPerTopic) { f ->
            f.lastModified.isBefore(deleteThreshold) &&
                // ensure that there is a file with a larger offset also
                // processed, so the largest offset is never removed.
                offsets.contains(f.range.mapRange { r -> r.incrementTo() })
        }

        val accountantMutex = Mutex()

        return coroutineScope {
            paths
                .map { file ->
                    async {
                        if (extractionCheck.isExtracted(file)) {
                            logger.info("Removing {}", file.path)
                            Timer.time("cleaner.delete") {
                                sourceStorage.delete(file.path)
                            }
                            true
                        } else {
                            // extract the file again at a later time
                            logger.warn("Source file was not completely extracted: {}", file.path)
                            val fullRange = file.range.mapRange { it.ensureToOffset() }
                            accountantMutex.withLock {
                                accountant.remove(fullRange)
                            }
                            false
                        }
                    }
                }
                .awaitAll()
                .count { it }
        }
    }

    private suspend fun topicPaths(path: Path): List<Path> = sourceStorage.listTopics(path, excludeTopics)
            .toMutableList()
            // different services start on different topics to decrease lock contention
            .also { it.shuffle() }

    override fun close() {
        scope.cancel()
    }

    companion object {
        private val logger = LoggerFactory.getLogger(SourceDataCleaner::class.java)

        fun job(config: RestructureConfig, serviceMutex: Mutex): Job? = if (config.cleaner.enable) {
            Job("cleaner", config.cleaner.interval, ::runCleaner, serviceMutex)
        } else null

        private suspend fun runCleaner(factory: FileStoreFactory) {
            SourceDataCleaner(factory).useSuspended { cleaner ->
                for (input in factory.config.paths.inputs) {
                    logger.info("Cleaning {}", input)
                    cleaner.process(input.toString())
                }
                logger.info("Cleaned up {} files", cleaner.deletedFileCount.format())
            }
        }
    }
}
