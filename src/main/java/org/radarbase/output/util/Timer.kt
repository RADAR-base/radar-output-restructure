/*
 * Copyright 2018 The Hyve
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

import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.atomic.LongAdder
import org.radarbase.output.util.ProgressBar.Companion.appendTime
import java.util.*

/** Timer for multi-threaded timings. The timer may be disabled to increase program performance.  */
object Timer {
    private val shutdownHook = Thread({ println(Timer) }, "Timer")
    val times: ConcurrentMap<String, MutableTimerEntry> = ConcurrentHashMap()

    /**
     * All currently measured timings.
     * This returns a thread-safe sorted snapshot of the current state.
     */
    val timings: Map<String, TimerEntry>
        get() = times.mapValuesTo(TreeMap()) { it.value.toTimerEntry() }

    /**
     * Whether the timer is enabled. A disabled timer will have much less performance impact on
     * timed code.
     */
    @Volatile
    @set:Synchronized
    var isEnabled: Boolean = false
        set(value) {
            if (value != field) {
                if (value) {
                    Runtime.getRuntime().addShutdownHook(shutdownHook)
                } else {
                    Runtime.getRuntime().removeShutdownHook(shutdownHook)
                    times.clear()
                }
            }
            field = value
        }

    /**
     * Time a given action, labeled by an action type.
     */
    inline fun <T> time(type: String, action: () -> T): T {
        return if (isEnabled) {
            val startTime = System.nanoTime()
            try {
                action()
            } finally {
                val time = System.nanoTime() - startTime
                times.computeIfAbsent(type) { MutableTimerEntry() }.add(time)
            }
        } else {
            action()
        }
    }

    /** Remove all registered action types and their timings. */
    fun reset(): Unit = times.clear()

    /**
     * String of all currently measured timings, reported per action type.
     */
    override fun toString(): String {
        val builder = StringBuilder(100 * (1 + times.size))
        builder.append("Timings:")

        if (!isEnabled) {
            builder.append(" disabled")
        } else if (times.isEmpty()) {
            builder.append(" none")
        } else {
            timings.entries
                    .forEach { entry ->
                        builder.append("\n\t")
                        builder.append(entry.key)
                        builder.append(" - time: ")
                        builder.appendTime(entry.value.totalTime)
                        builder.append(" - threads: ")
                        builder.append(entry.value.numThreads)
                        builder.append(" - invocations: ")
                        builder.append(entry.value.invocations)
                    }
        }

        return builder.toString()
    }

    class MutableTimerEntry {
        private val invocations = LongAdder()
        private val totalTime = LongAdder()
        private val threads = ConcurrentHashMap<Long, Long>()

        fun add(nanoTime: Long) {
            invocations.increment()
            totalTime.add(nanoTime)
            Thread.currentThread().id.let {
                threads[it] = it
            }
        }

        fun toTimerEntry(): TimerEntry = TimerEntry(invocations.sum(), Duration.ofNanos(totalTime.sum()), threads.size)
    }

    data class TimerEntry(val invocations: Long, val totalTime: Duration, val numThreads: Int)
}
