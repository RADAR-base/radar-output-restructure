package org.radarbase.output.worker

import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.conflate
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.radarbase.output.FileStoreFactory
import org.radarbase.output.util.ProgressBar.Companion.format
import org.radarbase.output.util.TimeUtil.durationSince
import org.radarbase.output.util.Timer
import org.slf4j.LoggerFactory
import java.time.Instant
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

class Job(
    val name: String,
    private val intervalSeconds: Long,
    val work: suspend (FileStoreFactory) -> Unit,
    private val serviceMutex: Mutex,
) {
    suspend fun run(factory: FileStoreFactory) {
        logger.debug("Pending start of job {}", name)
        serviceMutex.withLock {
            val timeStart = Instant.now()
            logger.info("Job {} started", name)
            try {
                work(factory)
                logger.info("Job {} completed in {}", name, timeStart.durationSince().format())
            } catch (e: InterruptedException) {
                logger.error("Job {} interrupted", name)
            } catch (ex: Throwable) {
                logger.error("Failed to run job {}", name, ex)
            } finally {
                if (Timer.isEnabled) {
                    logger.info("Job {} {}", name, Timer)
                    Timer.reset()
                }
            }
        }
    }

    suspend fun schedule(factory: FileStoreFactory) =
        repeatWithFixedInterval(intervalSeconds.seconds, intervalSeconds.seconds / 4)
            .collect { run(factory) }

    companion object {
        private val logger = LoggerFactory.getLogger(Job::class.java)

        fun repeatWithFixedInterval(
            period: Duration,
            initialDelay: Duration = Duration.ZERO,
        ): Flow<Unit> = flow {
            delay(initialDelay)
            while (currentCoroutineContext().isActive) {
                emit(Unit)
                delay(period)
            }
        }.conflate()
    }
}
