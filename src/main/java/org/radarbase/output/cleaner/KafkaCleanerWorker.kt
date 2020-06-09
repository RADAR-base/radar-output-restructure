package org.radarbase.output.cleaner

import org.radarbase.output.accounting.Accountant
import org.radarbase.output.config.RestructureConfig
import org.radarbase.output.source.SourceStorage
import org.radarbase.output.util.Timer.time
import org.slf4j.LoggerFactory
import java.nio.file.Path
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.atomic.AtomicBoolean

class KafkaCleanerWorker(
        config: RestructureConfig,
        private val accountant: Accountant,
        private val sourceStorage: SourceStorage,
        private val closed: AtomicBoolean,
        private val extractionCheck: ExtractionCheck
) {
    private val maxFilesPerTopic: Int = config.worker.maxFilesPerTopic ?: Int.MAX_VALUE
    private val deleteThreshold: Instant? = Instant.now()
            .minus(config.cleaner.age.toLong(), ChronoUnit.DAYS)

    fun deleteOldFiles(
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
                                                .ensureToOffset()
                                    })
                }
                .take(maxFilesPerTopic)
                .takeWhile { !closed.get() }
                .count { file ->
                    val extractionSuccessful = if (extractionCheck.isExtracted(file)) {
                        logger.info("Removing {}", file.path)
                        time("cleaner.delete") {
                            sourceStorage.delete(file.path)
                        }
                        true
                    } else {
                        logger.warn("Source file was not completely extracted: {}", file.path)
                        accountant.remove(file.range.mapRange { it.ensureToOffset() })
                        false
                    }
                    extractionSuccessful
                }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(KafkaCleanerWorker::class.java)
    }
}

