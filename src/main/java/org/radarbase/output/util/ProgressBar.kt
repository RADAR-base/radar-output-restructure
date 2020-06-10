/*
 * Copyright 2017 The Hyve
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.radarbase.output.util

import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

/**
 * Progress bar.
 * Based on https://stackoverflow.com/a/43381186/574082.
 */
class ProgressBar(private val label: String, private val total: Long, private val numStripes: Int, updateInterval: Long,
                  updateIntervalUnit: TimeUnit) {
    private val startTime: Long = System.nanoTime()
    private val updateIntervalNanos: Long = updateIntervalUnit.toNanos(updateInterval)
    private var lastUpdate = 0L
    private var isDone: Boolean = false

    init {
        require(total >= 0) { "Total of progress bar must be positive" }
        require(numStripes > 0) { "Number of stripes in progress bar must be positive" }
    }

    @Synchronized
    fun update(progress: Long, force: Boolean = false) {
        val now = System.nanoTime()

        if (!force && updateIntervalNanos > 0 && now <= lastUpdate + updateIntervalNanos) {
            return
        }
        lastUpdate = now

        try {
            require(progress in 0..total) { "Update value $progress out of range [0, $total]." }
        } catch (ex: Exception) {
            logger.error("{}", ex.toString())
            return
        }

        if (progress == total) {
            // go through only once
            if (isDone) return
            else isDone = true
        }

        val builder = StringBuilder(numStripes + 30 + label.length)

        val progressPercent: Float = if (total > 0) {
            (100f * progress / total).coerceAtMost(100f)
        } else {
            100f
        }

        bar(builder, progressPercent)
        builder.append(' ')
        percentage(builder, progressPercent)
        builder.append(" - ")
        eta(builder, progress)
        builder.append(" - ")
        heapSize(builder)
        builder.append(" <")
        builder.append(label)
        builder.append('>')

        logger.info(builder.toString())
    }

    private fun heapSize(builder: StringBuilder) {
        val r = Runtime.getRuntime()
        val free = r.maxMemory() - r.totalMemory() + r.freeMemory()

        builder.append("MemFree ")
                .append(free / 1000000)
                .append(" MB")
    }

    private fun percentage(builder: StringBuilder, progressPercent: Float) {
        val percent = progressPercent.toInt()

        if (percent < 10) builder.append("  ")
        else if (percent < 100) builder.append(' ')

        builder.append(percent).append('%')
    }

    private fun bar(builder: StringBuilder, progressPercent: Float) {
        val stripesFilled = (numStripes * progressPercent / 100).toInt()
        val notFilled = '-'
        val filled = '*'
        // 2 init + numStripes + 2 end + 4 percentage
        builder.append("[")
        for (i in 0 until stripesFilled) {
            builder.append(filled)
        }
        for (i in stripesFilled until numStripes) {
            builder.append(notFilled)
        }
        builder.append(']')
    }

    private fun eta(builder: StringBuilder, progress: Long) {
        builder.append("ETA ")
        if (progress > 0) {
            val duration = (System.nanoTime() - startTime) / 1_000_000_000.0
            builder.appendTime((duration * (total - progress) / progress).toLong())
        } else {
            builder.append("-:--:--")
        }
    }

    companion object {
        fun StringBuilder.appendTime(seconds: Long): StringBuilder {
            val minutes = seconds / 60 % 60
            val sec = seconds % 60
            append(seconds / 3600).append(':')

            if (minutes < 10) append('0')
            append(minutes).append(':')

            if (sec < 10) append('0')
            return append(sec)
        }

        fun StringBuilder.appendTime(duration: Duration): StringBuilder {
            val millis = duration.toMillis()
            appendTime(millis / 1000).append('.')
            val millisLast = (millis % 1000L).toInt().toLong()
            if (millisLast < 100) {
                append('0')
            }
            if (millisLast < 10) {
                append('0')
            }
            return append(millisLast)
        }

        fun Duration.format(): String {
            return StringBuilder(16).appendTime(this).toString()
        }

        private val logger = LoggerFactory.getLogger(ProgressBar::class.java)
    }
}
