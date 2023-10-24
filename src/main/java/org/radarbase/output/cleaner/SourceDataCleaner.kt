package org.radarbase.output.cleaner

import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withContext
import org.radarbase.kotlin.coroutines.launchJoin
import org.radarbase.output.Application.Companion.format
import org.radarbase.output.FileStoreFactory
import org.radarbase.output.accounting.Accountant
import org.radarbase.output.accounting.AccountantImpl
import org.radarbase.output.config.RestructureConfig
import org.radarbase.output.source.SourceStorageManager
import org.radarbase.output.util.ResourceContext.Companion.resourceContext
import org.radarbase.output.util.SuspendedCloseable.Companion.useSuspended
import org.radarbase.output.util.Timer
import org.radarbase.output.worker.Job
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.io.IOException
import java.nio.file.Path
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.atomic.LongAdder
import kotlin.coroutines.coroutineContext

class SourceDataCleaner(
    private val fileStoreFactory: FileStoreFactory,
    private val sourceStorageManager: SourceStorageManager,
) : Closeable {
    private val sourceStorage = sourceStorageManager.sourceStorage
    private val lockManager = fileStoreFactory.remoteLockManager
    private val excludeTopics: Set<String> = fileStoreFactory.config.topics
        .mapNotNullTo(HashSet()) { (topic, conf) ->
            topic.takeIf { conf.excludeFromDelete }
        }
    private val maxFilesPerTopic: Int =
        fileStoreFactory.config.cleaner.maxFilesPerTopic ?: Int.MAX_VALUE
    private val deleteThreshold: Instant? = Instant.now()
        .minus(fileStoreFactory.config.cleaner.age.toLong(), ChronoUnit.DAYS)

    val deletedFileCount = LongAdder()
    private val supervisor = SupervisorJob()

    @Throws(IOException::class, InterruptedException::class)
    suspend fun process() {
        // Get files and directories
        val paths = topicPaths(sourceStorage.root)

        logger.info("{} topics found", paths.size)

        withContext(coroutineContext + supervisor) {
            paths.forEach { p ->
                launch {
                    try {
                        val deleteCount = fileStoreFactory.workerSemaphore.withPermit {
                            mapTopic(p)
                        }
                        if (deleteCount > 0) {
                            logger.info("Removed {} files in topic {}", deleteCount, p.fileName)
                            deletedFileCount.add(deleteCount)
                        }
                    } catch (ex: Exception) {
                        logger.warn("Failed to map topic {}", p, ex)
                    }
                }
            }
        }
    }

    private suspend fun mapTopic(topicPath: Path): Long {
        val topic = topicPath.fileName.toString()
        return try {
            lockManager.tryWithLock(topic) {
                coroutineScope {
                    resourceContext {
                        val accountant = createResource { AccountantImpl(fileStoreFactory, topic) }
                            .apply { initialize(this@coroutineScope) }
                        val extractionCheck = createResource {
                            TimestampExtractionCheck(
                                sourceStorage,
                                fileStoreFactory,
                            )
                        }
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
        topicPath: Path,
    ): Int {
        val offsets = accountant.offsets.copyForTopic(topic)

        val paths = sourceStorageManager.listTopicFiles(topic, topicPath, maxFilesPerTopic) { f ->
            f.lastModified.isBefore(deleteThreshold) &&
                // ensure that there is a file with a larger offset also
                // processed, so the largest offset is never removed.
                offsets.contains(f.range.mapRange { r -> r.incrementTo() })
        }

        return paths
            .count { file ->
                if (extractionCheck.isExtracted(file)) {
                    logger.info("Removing {}", file.path)
                    Timer.time("cleaner.delete") {
                        sourceStorageManager.delete(file.path)
                    }
                    true
                } else {
                    // extract the file again at a later time
                    logger.warn("Source file was not completely extracted: {}", file.path)
                    val fullRange = file.range.mapRange { it.ensureToOffset() }
                    accountant.remove(fullRange)
                    false
                }
            }
    }

    private suspend fun topicPaths(path: Path): List<Path> =
        sourceStorageManager.listTopics(path, excludeTopics)
            // different services start on different topics to decrease lock contention
            .shuffled()

    override fun close() {
        supervisor.cancel()
    }

    companion object {
        private val logger = LoggerFactory.getLogger(SourceDataCleaner::class.java)

        fun job(config: RestructureConfig, serviceMutex: Mutex): Job? = if (config.cleaner.enable) {
            Job("cleaner", config.cleaner.interval, ::runCleaner, serviceMutex)
        } else {
            null
        }

        private suspend fun runCleaner(factory: FileStoreFactory) = coroutineScope {
            factory.sourceStorage.launchJoin { sourceStorage ->
                SourceDataCleaner(factory, sourceStorage).useSuspended { cleaner ->
                    sourceStorage.storageIndexManager.update()
                    logger.info("Cleaning {}", sourceStorage.sourceStorage.root)
                    cleaner.process()
                    logger.info("Cleaned up {} files", cleaner.deletedFileCount.format())
                }
            }
        }
    }
}
