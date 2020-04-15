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

import com.almworks.integers.LongArray
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import java.util.stream.Collectors
import java.util.stream.IntStream
import java.util.stream.Stream
import org.radarbase.output.util.FunctionalValue
import org.radarbase.output.util.LockedFunctionalValue
import org.radarbase.output.util.ReadOnlyFunctionalValue
import java.util.*

/** Encompasses a range of offsets.  */
class OffsetRangeSet {
    private val factory: (OffsetRangeLists) -> FunctionalValue<OffsetRangeLists>
    private val ranges: ConcurrentMap<TopicPartition, FunctionalValue<OffsetRangeLists>>

    /** Whether the stored offsets is empty.  */
    val isEmpty: Boolean
        get() = ranges.isEmpty()

    @JvmOverloads
    constructor(factory: (OffsetRangeLists) -> FunctionalValue<OffsetRangeLists> = { LockedFunctionalValue(it) }) {
        this.ranges = ConcurrentHashMap()
        this.factory = factory
    }

    private constructor(ranges: ConcurrentMap<TopicPartition, FunctionalValue<OffsetRangeLists>>,
                        factory: (OffsetRangeLists) -> FunctionalValue<OffsetRangeLists>) {
        this.ranges = ranges
        this.factory = factory
    }

    /** Add given offset range to seen offsets.  */
    fun add(range: OffsetRange) {
        ranges.computeIfAbsent(range.topicPartition) { factory(OffsetRangeLists()) }
                .modify { rangeList -> rangeList.add(range.offsetFrom, range.offsetTo) }
    }

    /** Add given offset range to seen offsets.  */
    fun add(topicPartition: TopicPartition, offset: Long) {
        ranges.computeIfAbsent(topicPartition) { factory(OffsetRangeLists()) }
                .modify { rangeList -> rangeList.add(offset) }
    }

    /** Add all offset stream of given set to the current set.  */
    fun addAll(set: OffsetRangeSet) {
        set.stream().forEach { this.add(it) }
    }

    fun addAll(list: OffsetRangeList) {
        list.partitions.forEach { partition ->
            val topicPartition = TopicPartition(partition.topic, partition.partition)
            partition.ranges.forEach {
                add(OffsetRange(topicPartition, it.from, it.to))
            }
        }
    }

    /** Whether this range value completely contains the given range.  */
    operator fun contains(range: OffsetRange): Boolean {
        return ranges.getOrDefault(range.topicPartition, EMPTY_VALUE)
                .read { rangeList -> rangeList.contains(range.offsetFrom, range.offsetTo) }
    }

    /** Whether this range value completely contains the given range.  */
    fun contains(partition: TopicPartition, offset: Long): Boolean {
        return ranges.getOrDefault(partition, EMPTY_VALUE)
                .read { rangeList -> rangeList.contains(offset) }
    }

    /** Number of distinct offsets in given topic/partition.  */
    fun size(topicPartition: TopicPartition): Int {
        return ranges.getOrDefault(topicPartition, EMPTY_VALUE)
                .read { it.size() }
    }

    fun withFactory(factory: (OffsetRangeLists) -> FunctionalValue<OffsetRangeLists>): OffsetRangeSet {
        return OffsetRangeSet(
                ranges.entries.stream()
                        .collect(Collectors.toConcurrentMap(
                                { it.key },
                                { e -> e.value.read { range -> factory(OffsetRangeLists(range)) } })),
                factory)
    }

    override fun toString(): String = "OffsetRangeSet$ranges"

    /**
     * All offset stream as a stream. For any given topic/partition, this yields an
     * consistent result but values from different topic/partitions may not be consistent.
     */
    fun stream(): Stream<OffsetRange> {
        return ranges.entries.stream()
                .sorted(Comparator.comparing<Map.Entry<TopicPartition, FunctionalValue<OffsetRangeLists>>, TopicPartition> { it.key })
                .flatMap { e ->
                    e.value.read { OffsetRangeLists(it) }.stream()
                            .map { r -> OffsetRange(e.key, r.from, r.to) }
                }
    }

    fun toOffsetRangeList(): OffsetRangeList {
        return OffsetRangeList(ranges.entries.stream()
                .sorted(Comparator.comparing<Map.Entry<TopicPartition, FunctionalValue<OffsetRangeLists>>, TopicPartition> { it.key })
                .map { e ->
                    OffsetRangeList.TopicPartitionRange(e.key.topic, e.key.partition, e.value.read { OffsetRangeLists(it) }.stream()
                            .map { Range(it.from, it.to) }
                            .collect(Collectors.toList()))
                }
                .collect(Collectors.toList()))
    }

    override fun equals(other: Any?): Boolean {
        return other === this || (
                javaClass == other?.javaClass && ranges == (other as OffsetRangeSet).ranges)
    }

    override fun hashCode(): Int = ranges.hashCode()

    class OffsetRangeLists {
        private val offsetsFrom: LongArray
        private val offsetsTo: LongArray

        constructor() {
            offsetsFrom = LongArray(8)
            offsetsTo = LongArray(8)
        }

        constructor(other: OffsetRangeLists) {
            offsetsFrom = LongArray(other.offsetsFrom)
            offsetsTo = LongArray(other.offsetsTo)
        }

        fun contains(from: Long, to: Long): Boolean {
            //  -index-1 if not found
            val index = offsetsFrom.binarySearch(from)
            return if (index >= 0) {
                offsetsTo.get(index) >= to
            } else {
                val indexBefore = -index - 2
                indexBefore >= 0 && offsetsTo.get(indexBefore) >= to
            }
        }

        operator fun contains(offset: Long): Boolean {
            //  -index-1 if not found
            val index = offsetsFrom.binarySearch(offset)
            return if (index >= 0) {
                true
            } else {
                val indexBefore = -index - 2
                indexBefore >= 0 && offsetsTo.get(indexBefore) >= offset
            }
        }

        fun add(offset: Long) {
            var index = offsetsFrom.binarySearch(offset)
            if (index >= 0) {
                return
            }
            // index where this range would be entered
            index = -index - 1

            if (index > 0 && offset == offsetsTo.get(index - 1) + 1) {
                // concat with range before it and possibly stream afterwards
                offsetsTo.set(index - 1, offset)
                if (index < offsetsFrom.size() && offset == offsetsFrom.get(index) - 1) {
                    offsetsTo.set(index - 1, offsetsTo.get(index))
                    offsetsFrom.removeAt(index)
                    offsetsTo.removeAt(index)
                }
            } else if (index >= offsetsFrom.size() || offset < offsetsFrom.get(index) - 1) {
                // cannot concat, enter new range
                offsetsFrom.insert(index, offset)
                offsetsTo.insert(index, offset)
            } else {
                // concat with the stream after it
                offsetsFrom.set(index, offset)
            }
        }

        fun add(from: Long, to: Long) {
            var index = offsetsFrom.binarySearch(from)
            if (index < 0) {
                // index where this range would be entered
                index = -index - 1

                if (index > 0 && from <= offsetsTo.get(index - 1) + 1) {
                    // concat with range before it and possibly stream afterwards
                    index--
                } else if (index >= offsetsFrom.size() || to < offsetsFrom.get(index) - 1) {
                    // cannot concat, enter new range
                    offsetsFrom.insert(index, from)
                    offsetsTo.insert(index, to)
                    return
                } else {
                    // concat with the stream after it
                    offsetsFrom.set(index, from)
                }
            }

            if (to <= offsetsTo.get(index)) {
                return
            }

            offsetsTo.set(index, to)

            var overlapIndex = index + 1
            val startIndex = overlapIndex

            while (overlapIndex < offsetsTo.size() && to >= offsetsTo.get(overlapIndex)) {
                overlapIndex++
            }
            if (overlapIndex < offsetsFrom.size() && to >= offsetsFrom.get(overlapIndex) - 1) {
                offsetsTo.set(index, offsetsTo.get(overlapIndex))
                overlapIndex++
            }
            if (overlapIndex != startIndex) {
                offsetsFrom.removeRange(startIndex, overlapIndex)
                offsetsTo.removeRange(startIndex, overlapIndex)
            }
        }

        fun stream(): Stream<Range> {
            return IntStream.range(0, offsetsFrom.size())
                    .mapToObj { i -> Range(offsetsFrom.get(i), offsetsTo.get(i)) }
        }

        fun size(): Int = offsetsFrom.size()

        override fun toString(): String {
            return "[" + stream()
                    .map { it.toString() }
                    .collect(Collectors.joining(", ")) + "]"
        }
    }

    data class Range(val from: Long, val to: Long) {
        override fun toString() = "($from - $to)"
    }

    data class OffsetRangeList(val partitions: List<TopicPartitionRange>) {
        data class TopicPartitionRange(val topic: String, val partition: Int, val ranges: List<Range>)
    }

    companion object {
        private val EMPTY_VALUE = ReadOnlyFunctionalValue(OffsetRangeLists())
    }
}
