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

package org.radarbase.hdfs.util

import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.atomic.LongAdder

/** Timer for multi-threaded timings. The timer may be disabled to increase program performance.  */
object Timer {
    private val times: ConcurrentMap<Category, LongAdder> = ConcurrentHashMap()

    /**
     * Whether the timer is enabled. A disabled timer will have much less performance impact on
     * timed code.
     */
    @Volatile
    var isEnabled: Boolean = true

    /** Add number of nanoseconds to given type of measurement.  */
    fun add(type: String, nanoTimeStart: Long) {
        if (isEnabled) {
            val time = System.nanoTime() - nanoTimeStart
            val cat = Category(type)
            times.computeIfAbsent(cat) { LongAdder() }.add(time)
        }
    }

    override fun toString(): String {
        if (!isEnabled) {
            return "Timings: disabled"
        }
        val builder = StringBuilder()
        builder.append("Timings:")

        this.times.entries
                .groupByTo(TreeMap()) { it.key.type }
                .forEach { entry ->
                    builder.append("\n\t")
                    builder.append(entry.key)
                    builder.append(" - time: ")
                    formatTime(builder, entry.value.stream()
                            .mapToLong { (_, adder) -> adder.sum() }
                            .sum())
                    builder.append(" - threads: ")
                    builder.append(entry.value.size)
                }

        return builder.toString()
    }

    private data class Category(val type: String, val thread: String = Thread.currentThread().name)

    private fun formatTime(builder: StringBuilder, nanoTime: Long) {
        val seconds = (nanoTime / 1_000_000_000L).toInt()
        val millis = (nanoTime / 1_000_000L).toInt()
        ProgressBar.formatTime(builder, seconds.toLong())
        builder.append('.')
        if (millis < 100) builder.append('0')
        if (millis < 10) builder.append('0')
        builder.append(millis)
    }
}
