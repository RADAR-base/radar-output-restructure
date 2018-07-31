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

package org.radarcns.hdfs.accounting;

import com.almworks.integers.LongArray;
import org.radarcns.hdfs.util.FunctionalValue;
import org.radarcns.hdfs.util.LockedFunctionalValue;
import org.radarcns.hdfs.util.ReadOnlyFunctionalValue;

import java.util.AbstractMap;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/** Encompasses a range of offsets. */
public class OffsetRangeSet {
    private static final FunctionalValue<OffsetRangeLists> EMPTY_VALUE =
            new ReadOnlyFunctionalValue<>(new OffsetRangeLists());

    private final Function<OffsetRangeLists, FunctionalValue<OffsetRangeLists>> factory;
    private final ConcurrentMap<TopicPartition, FunctionalValue<OffsetRangeLists>> ranges;

    public OffsetRangeSet() {
        this(LockedFunctionalValue::new);
    }

    public OffsetRangeSet(
            Function<OffsetRangeLists, FunctionalValue<OffsetRangeLists>> factory) {
        this.ranges = new ConcurrentHashMap<>();
        this.factory = factory;
    }

    private OffsetRangeSet(ConcurrentMap<TopicPartition, FunctionalValue<OffsetRangeLists>> ranges,
            Function<OffsetRangeLists, FunctionalValue<OffsetRangeLists>> factory) {
        this.ranges = ranges;
        this.factory = factory;
    }

    /** Add given offset range to seen offsets. */
    public void add(OffsetRange range) {
        ranges.computeIfAbsent(range.getTopicPartition(), this::newList)
                .modify(rangeList -> rangeList.add(range.getOffsetFrom(), range.getOffsetTo()));
    }

    /** Add given offset range to seen offsets. */
    public void add(TopicPartition topicPartition, long offset) {
        ranges.computeIfAbsent(topicPartition, this::newList)
                .modify(rangeList -> rangeList.add(offset));
    }

    private FunctionalValue<OffsetRangeLists> newList(@SuppressWarnings("unused")
            TopicPartition partition) {
        return factory.apply(new OffsetRangeLists());
    }

    /** Add all offset stream of given set to the current set. */
    public void addAll(OffsetRangeSet set) {
        set.stream().forEach(this::add);
    }

    /** Whether this range value completely contains the given range. */
    public boolean contains(OffsetRange range) {
        return ranges.getOrDefault(range.getTopicPartition(), EMPTY_VALUE)
                .read(rangeList -> rangeList.contains(range.getOffsetFrom(), range.getOffsetTo()));
    }

    /** Whether this range value completely contains the given range. */
    public boolean contains(TopicPartition partition, long offset) {
        return ranges.getOrDefault(partition, EMPTY_VALUE)
                .read(rangeList -> rangeList.contains(offset));
    }

    /** Number of distict offsets in given topic/partition. */
    public int size(TopicPartition topicPartition) {
        return ranges.getOrDefault(topicPartition, EMPTY_VALUE)
                .read(OffsetRangeLists::size);
    }

    public OffsetRangeSet withFactory(
            Function<OffsetRangeLists, FunctionalValue<OffsetRangeLists>> factory) {
        return new OffsetRangeSet(
                ranges.entrySet().stream().collect(Collectors.toConcurrentMap(
                        Map.Entry::getKey, e -> e.getValue().read(
                                range -> factory.apply(new OffsetRangeLists(range))))),
                factory);
    }

    @Override
    public String toString() {
        return "OffsetRangeSet" + ranges;
    }

    /** Whether the stored offsets is empty. */
    public boolean isEmpty() {
        return ranges.isEmpty();
    }

    /** All offset stream as a stream. For any given topic/partition, this yields an
     * consistent result but values from different topic/partitions may not be consistent. */
    public Stream<OffsetRange> stream() {
        return ranges.entrySet().stream()
                .sorted(Comparator.comparing(Map.Entry::getKey))
                .flatMap(e -> e.getValue().read(OffsetRangeLists::new).stream()
                        .map(r -> new OffsetRange(e.getKey(), r.first, r.second)));
    }

    @Override
    public boolean equals(Object o) {
        return o == this
                || o != null
                && getClass().equals(o.getClass())
                && ranges.equals(((OffsetRangeSet) o).ranges);
    }

    @Override
    public int hashCode() {
        return ranges.hashCode();
    }

    public static class OffsetRangeLists {
        private final LongArray offsetsFrom;
        private final LongArray offsetsTo;

        public OffsetRangeLists() {
             offsetsFrom = new LongArray(8);
             offsetsTo = new LongArray(8);
        }

        public OffsetRangeLists(OffsetRangeLists other) {
            offsetsFrom = new LongArray(other.offsetsFrom);
            offsetsTo = new LongArray(other.offsetsTo);
        }

        public boolean contains(long from, long to) {
            //  -index-1 if not found
            int index = offsetsFrom.binarySearch(from);
            if (index >= 0) {
                return offsetsTo.get(index) >= to;
            } else {
                int indexBefore = -index - 2;
                return indexBefore >= 0 && offsetsTo.get(indexBefore) >= to;
            }
        }

        public boolean contains(long offset) {
            //  -index-1 if not found
            int index = offsetsFrom.binarySearch(offset);
            if (index >= 0) {
                return true;
            } else {
                int indexBefore = -index - 2;
                return indexBefore >= 0 && offsetsTo.get(indexBefore) >= offset;
            }
        }

        public void add(long offset) {
            int index = offsetsFrom.binarySearch(offset);
            if (index >= 0) {
                return;
            }
            // index where this range would be entered
            index = -index - 1;

            if (index > 0 && offset == offsetsTo.get(index - 1) + 1) {
                // concat with range before it and possibly stream afterwards
                offsetsTo.set(index - 1, offset);
                if (index < offsetsFrom.size() && offset == offsetsFrom.get(index) - 1) {
                    offsetsTo.set(index - 1, offsetsTo.get(index));
                    offsetsFrom.removeAt(index);
                    offsetsTo.removeAt(index);
                }
            } else if (index >= offsetsFrom.size() || offset < offsetsFrom.get(index) - 1) {
                // cannot concat, enter new range
                offsetsFrom.insert(index, offset);
                offsetsTo.insert(index, offset);
            } else {
                // concat with the stream after it
                offsetsFrom.set(index, offset);
            }
        }

        public void add(long from, long to) {
            int index = offsetsFrom.binarySearch(from);
            if (index < 0) {
                // index where this range would be entered
                index = -index - 1;

                if (index > 0 && from <= offsetsTo.get(index - 1) + 1) {
                    // concat with range before it and possibly stream afterwards
                    index--;
                } else if (index >= offsetsFrom.size() || to < offsetsFrom.get(index) - 1) {
                    // cannot concat, enter new range
                    offsetsFrom.insert(index, from);
                    offsetsTo.insert(index, to);
                    return;
                } else {
                    // concat with the stream after it
                    offsetsFrom.set(index, from);
                }
            }

            if (to <= offsetsTo.get(index)) {
                return;
            }

            offsetsTo.set(index, to);

            int overlapIndex = index + 1;
            int startIndex = overlapIndex;

            while (overlapIndex < offsetsTo.size() && to >= offsetsTo.get(overlapIndex)) {
                overlapIndex++;
            }
            if (overlapIndex < offsetsFrom.size()
                    && to >= offsetsFrom.get(overlapIndex) - 1) {
                offsetsTo.set(index, offsetsTo.get(overlapIndex));
                overlapIndex++;
            }
            if (overlapIndex != startIndex) {
                offsetsFrom.removeRange(startIndex, overlapIndex);
                offsetsTo.removeRange(startIndex, overlapIndex);
            }
        }

        public Stream<LongTuple> stream() {
            return IntStream.range(0, offsetsFrom.size())
                    .mapToObj(i -> new LongTuple(offsetsFrom.get(i), offsetsTo.get(i)));
        }

        public int size() {
            return offsetsFrom.size();
        }

        @Override
        public String toString() {
            return "[" + stream()
                    .map(Object::toString)
                    .collect(Collectors.joining(", ")) + ']';
        }
    }
    private static class LongTuple {
        private final long first;
        private final long second;

        private LongTuple(long first, long second) {
            this.first = first;
            this.second = second;
        }

        @Override
        public String toString() {
            return "(" + first + " - " + second + ')';
        }
    }
}
