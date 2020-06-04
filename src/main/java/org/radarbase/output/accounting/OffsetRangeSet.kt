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

package org.radarbase.output.accounting

import org.radarbase.output.util.FunctionalValue
import org.radarbase.output.util.LockedFunctionalValue
import org.radarbase.output.util.ReadOnlyFunctionalValue
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap

/**
 * Encompasses a range of offsets. Offsets bookkeeping takes two dimensions: offset number and last
 * modification time of an offset. To keep offset bookkeeping efficient, offset ranges are merged
 * when they overlap or are adjacent. When merging offsets, last processing time is taken as the
 * maximum of all the merged processing times.
 */
class OffsetRangeSet {
    private val factory: (OffsetIntervals) -> FunctionalValue<OffsetIntervals>
    private val ranges: ConcurrentMap<TopicPartition, FunctionalValue<OffsetIntervals>>

    /** Whether the stored offsets is empty.  */
    val isEmpty: Boolean
        get() = ranges.isEmpty()

    @JvmOverloads
    constructor(
            factory: (OffsetIntervals) -> FunctionalValue<OffsetIntervals> = { LockedFunctionalValue(it) }
    ) {
        this.ranges = ConcurrentHashMap()
        this.factory = factory
    }

    private constructor(
            ranges: ConcurrentMap<TopicPartition, FunctionalValue<OffsetIntervals>>,
            factory: (OffsetIntervals) -> FunctionalValue<OffsetIntervals>
    ) {
        this.ranges = ranges
        this.factory = factory
    }

    /** Add given offset range to seen offsets.  */
    fun add(range: TopicPartitionOffsetRange) {
        range.topicPartition.modifyIntervals { it.add(range.range) }
    }

    /** Add given single offset to seen offsets.  */
    fun add(topicPartition: TopicPartition, offset: Long, lastModified: Instant) {
        topicPartition.modifyIntervals { it.add(offset, lastModified) }
    }

    /** Add all offset stream of given set to the current set.  */
    fun addAll(set: OffsetRangeSet) {
        set.ranges.entries.forEach { (otherPartition, otherRanges) ->
            otherPartition.modifyIntervals { rangeList ->
                otherRanges.read { it.toList() }
                        .forEach { rangeList.add(it) }
            }
        }
    }

    fun addAll(topicPartition: TopicPartition, ranges: List<Range>) {
        topicPartition.modifyIntervals { intervals ->
            ranges.forEach { intervals.add(it) }
        }
    }

    /** Whether this range value completely contains the given range.  */
    operator fun contains(range: TopicPartitionOffsetRange): Boolean {
        return range.topicPartition.readIntervals { it.contains(range.range) }
    }

    /** Whether this range value completely contains the given range.  */
    fun contains(partition: TopicPartition, offset: Long, lastModified: Instant): Boolean {
        return partition.readIntervals { it.contains(offset, lastModified) }
    }

    /** Number of distinct offsets in given topic/partition.  */
    fun size(topicPartition: TopicPartition): Int {
        return topicPartition.readIntervals { it.size() }
    }

    fun remove(range: TopicPartitionOffsetRange) {
        return range.topicPartition.modifyIntervals { it.remove(range.range) }
    }

    fun withFactory(
            factory: (OffsetIntervals) -> FunctionalValue<OffsetIntervals>
    ) = OffsetRangeSet(
            ranges.entries.associateByTo(
                    ConcurrentHashMap(),
                    { it.key },
                    { (_, intervals) ->
                        intervals.read { factory(OffsetIntervals(it)) }
                    }),
            factory)

    override fun toString(): String = "OffsetRangeSet$ranges"

    fun <T> map(
            mapping: (TopicPartition, OffsetIntervals) -> T
    ): List<T> = ranges.entries
            .sortedBy { it.key }
            .map { (key, value) ->
                value.read { mapping(key, it) }
            }

    /**
     * Process all offset stream as a list. For any given topic/partition, this yields a
     * consistent result but values from different topic/partitions may not be consistent.
     */
    fun forEach(
            action: (TopicPartition, OffsetIntervals) -> Unit
    ) = ranges.entries
            .sortedBy { it.key }
            .forEach { (key, value) ->
                value.read { action(key, it) }
            }

    override fun equals(other: Any?): Boolean {
        if (other === this) {
            return true
        }
        if (javaClass != other?.javaClass) {
            return false
        }
        other as OffsetRangeSet
        return ranges == other.ranges
    }

    override fun hashCode(): Int = ranges.hashCode()

    private fun TopicPartition.modifyIntervals(consumer: (OffsetIntervals) -> Unit) = ranges
            .computeIfAbsent(this) { factory(OffsetIntervals()) }
            .modify(consumer)

    private fun <V> TopicPartition.readIntervals(function: (OffsetIntervals) -> V) = ranges
            .getOrDefault(this, EMPTY_VALUE)
            .read(function)

    fun copyForTopic(topic: String) = OffsetRangeSet(
            ranges.entries
                    .filter { it.key.topic == topic }
                    .associateByTo(ConcurrentHashMap(),
                            { it.key },
                            { (_, intervals) ->
                                intervals.read { factory(OffsetIntervals(it)) }
                            }),
            factory)


    data class Range(val from: Long, val to: Long, val lastProcessed: Instant = Instant.now()) {
        val size: Long = to - from + 1
        override fun toString() = "($from - $to, $lastProcessed)"
    }

    companion object {
        private val EMPTY_VALUE = ReadOnlyFunctionalValue(OffsetIntervals())
    }
}
