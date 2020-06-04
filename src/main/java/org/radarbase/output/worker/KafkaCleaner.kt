package org.radarbase.output.worker

import org.radarbase.output.FileStoreFactory
import org.radarbase.output.accounting.Accountant
import org.radarbase.output.source.SourceStorageWalker
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.io.IOException
import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.LongAdder

class KafkaCleaner(
        private val fileStoreFactory: FileStoreFactory
) : Closeable {
    private val isClosed = AtomicBoolean(false)
    private val lockManager = fileStoreFactory.remoteLockManager
    private val sourceStorage = fileStoreFactory.sourceStorage
    private val excludeTopics: Set<String> = fileStoreFactory.config.topics
            .mapNotNullTo(HashSet()) { (topic, conf) ->
                topic.takeIf { conf.excludeFromDelete }
            }

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
                        deletedFileCount.add(deleteCount)
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
            lockManager.acquireTopicLock(topic)?.use {
                Accountant(fileStoreFactory, topic).use { accountant ->
                    KafkaCleanerWorker(fileStoreFactory, accountant, sourceStorage, isClosed).use { worker ->
                        worker.deleteOldFiles(topic, topicPath).toLong()
                                .also {
                                    if (it > 0) {
                                        logger.info("Removed {} files in topic {}", it, topic)
                                    }
                                }
                    }
                }
            }
        } catch (ex: IOException) {
            logger.error("Failed to map files of topic {}", topic, ex)
            0L
        } ?: 0L
    }


    private fun topicPaths(path: Path): List<Path> = sourceStorage.walker.walkTopics(path, excludeTopics)
            .toMutableList()
            // different services start on different topics to decrease lock contention
            .also { it.shuffle() }

    override fun close() {
        isClosed.set(true)
    }

    companion object {
        private val logger = LoggerFactory.getLogger(KafkaCleaner::class.java)
    }
}

