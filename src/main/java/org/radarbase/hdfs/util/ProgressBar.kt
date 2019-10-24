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

package org.radarbase.hdfs.util

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
    private val isDone: AtomicBoolean = AtomicBoolean(false)
    private val updateIntervalNanos: Long = updateIntervalUnit.toNanos(updateInterval)
    private val lastUpdate: AtomicLong = AtomicLong(0L)
    private var previousLineLength: Int = 0

    init {
        require(total >= 0) { "Total of progress bar must be positive" }
        require(numStripes > 0) { "Number of stripes in progress bar must be positive" }
    }

    @Synchronized
    fun update(progress: Long) {
        val now = System.nanoTime()
        if (updateIntervalNanos <= 0 || lastUpdate.updateAndGet { l -> if (now > l + updateIntervalNanos) now else l } != now) {
            return
        }

        require(progress in 0..total) { "Update value $progress out of range [0, $total]." }

        if (progress == total && !isDone.compareAndSet(false, true)) {
            return
        }

        val builder = StringBuilder(numStripes + 30 + label.length)

        val progressPercent: Float
        if (total > 0) {
            progressPercent = Math.min(100f * progress / total, 100f)
        } else {
            progressPercent = 100f
        }

        builder.append(label)
        builder.append(": ")
        bar(builder, progressPercent)
        builder.append(' ')
        percentage(builder, progressPercent)
        builder.append(" - ")
        eta(builder, progress)
        builder.append(" - ")
        heapSize(builder)

        // overwrite any characters from the previous print
        val currentLineLength = builder.length
        synchronized(this) {
            while (builder.length < previousLineLength) {
                builder.append(' ')
            }
            previousLineLength = currentLineLength

            if (progress >= total) {
                builder.append('\n')
            }

            print(builder.toString())
        }
    }

    private fun heapSize(builder: StringBuilder) {
        builder.append("Mem ")
                .append(Runtime.getRuntime().totalMemory() / 1000000)
                .append("/")
                .append(Runtime.getRuntime().maxMemory() / 1000000)
                .append(" MB")
    }

    private fun percentage(builder: StringBuilder, progressPercent: Float) {
        builder.append(progressPercent.toInt()).append('%')
    }

    private fun bar(builder: StringBuilder, progressPercent: Float) {
        val stripesFilled = (numStripes * progressPercent / 100).toInt()
        val notFilled = '-'
        val filled = '*'
        // 2 init + numStripes + 2 end + 4 percentage
        builder.append("\r[")
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
            formatTime(builder, (duration * (total - progress) / progress).toLong())
        } else {
            builder.append('-')
        }
    }

    companion object {
        fun formatTime(builder: StringBuilder, seconds: Long): StringBuilder {
            val minutes = seconds / 60 % 60
            val sec = seconds % 60
            builder.append(seconds / 3600).append(':')
            if (minutes < 10) {
                builder.append('0')
            }
            builder.append(minutes).append(':')
            if (sec < 10) {
                builder.append('0')
            }
            builder.append(sec)
            return builder
        }

        fun formatTime(duration: Duration): String {
            val millis = duration.toMillis()
            val builder = StringBuilder(16)
            formatTime(builder, millis / 1000)
                    .append('.')
            val millisLast = (millis % 1000L).toInt().toLong()
            if (millisLast < 100) {
                builder.append('0')
            }
            if (millisLast < 10) {
                builder.append('0')
            }
            return builder.append(millisLast).toString()
        }
    }
}
