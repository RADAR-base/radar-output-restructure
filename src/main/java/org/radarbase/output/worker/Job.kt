package org.radarbase.output.worker

import org.radarbase.output.util.ProgressBar.Companion.format
import org.radarbase.output.util.TimeUtil.durationSince
import org.radarbase.output.util.Timer
import org.slf4j.LoggerFactory
import java.time.Instant
import java.util.concurrent.Future
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit

class Job(val name: String, val isEnabled: Boolean, val intervalSeconds: Long, val work: () -> Unit) {
    fun run() {
        val timeStart = Instant.now()
        try {
            work()
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

    fun schedule(executorService: ScheduledExecutorService): Future<*> {
        return executorService.scheduleAtFixedRate(::run,
                intervalSeconds / 4,
                intervalSeconds,
                TimeUnit.SECONDS)
    }

    companion object {
        private val logger = LoggerFactory.getLogger(Job::class.java)
    }
}
