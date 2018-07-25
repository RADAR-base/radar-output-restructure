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

import org.radarcns.hdfs.util.FunctionalValue;
import org.radarcns.hdfs.util.LockedFunctionalValue;
import org.radarcns.hdfs.util.ReadOnlyFunctionalValue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;
import java.util.stream.Stream;

/** Encompasses a range of offsets. */
public class OffsetRangeSet {
    private static final FunctionalValue<SortedSet<OffsetRange>> EMPTY_VALUE =
            new ReadOnlyFunctionalValue<>(Collections.emptySortedSet());

    private final Supplier<FunctionalValue<SortedSet<OffsetRange>>> factory;
    private final ConcurrentMap<String, FunctionalValue<SortedSet<OffsetRange>>> ranges;

    public OffsetRangeSet() {
        this(() -> new LockedFunctionalValue<>(new TreeSet<>()));
    }

    public OffsetRangeSet(
            Supplier<FunctionalValue<SortedSet<OffsetRange>>> factory) {
        this.ranges = new ConcurrentHashMap<>();
        this.factory = factory;
    }

    /** Add given offset range to seen offsets. */
    public void add(OffsetRange range) {
        ranges.computeIfAbsent(key(range), k -> factory.get())
                .modify(topicRanges -> {
                    SortedSet<OffsetRange> tail = topicRanges.tailSet(range);
                    SortedSet<OffsetRange> head = topicRanges.headSet(range);

                    OffsetRange next = !tail.isEmpty() ? tail.first() : null;
                    OffsetRange previous = !head.isEmpty() ? head.last() : null;

                    if (next != null) {
                        if (next.equals(range)) {
                            return;
                        }

                        if (next.getOffsetFrom() <= range.getOffsetTo() + 1) {
                            if (previous != null
                                    && previous.getOffsetTo() >= range.getOffsetFrom() - 1) {
                                next.setOffsetFrom(previous.getOffsetFrom());
                                topicRanges.remove(previous);
                            } else {
                                next.setOffsetFrom(range.getOffsetFrom());
                            }
                            return;
                        }
                    }

                    if (previous != null && previous.getOffsetTo() >= range.getOffsetFrom() - 1) {
                        previous.setOffsetTo(range.getOffsetTo());
                        return;
                    }

                    topicRanges.add(range);
                });
    }

    /** Add all offset ranges of given set to the current set. */
    public void addAll(OffsetRangeSet set) {
        set.ranges().forEach(this::add);
    }

    /** Whether this range value completely contains the given range. */
    public boolean contains(OffsetRange range) {
        return ranges.getOrDefault(key(range), EMPTY_VALUE)
                .read(topicRanges -> {
                    if (topicRanges.contains(range)) {
                        return true;
                    }

                    SortedSet<OffsetRange> tail = topicRanges.tailSet(range);
                    OffsetRange next = !tail.isEmpty() ? tail.first() : null;

                    if (next != null
                            && next.getOffsetFrom() == range.getOffsetFrom()
                            && next.getOffsetTo() >= range.getOffsetTo()) {
                        return true;
                    }

                    SortedSet<OffsetRange> head = topicRanges.headSet(range);
                    OffsetRange previous = !head.isEmpty() ? head.last() : null;
                    return previous != null && previous.getOffsetTo() >= range.getOffsetTo();
                });
    }

    /** Number of distict offsets in given topic/partition. */
    public int size(String topic, int partition) {
        return ranges.getOrDefault(key(topic, partition), EMPTY_VALUE)
                .read(Collection::size);
    }

    @Override
    public String toString() {
        return "OffsetRangeSet" + ranges;
    }

    /** Whether the stored offsets is empty. */
    public boolean isEmpty() {
        return ranges.isEmpty();
    }

    private static String key(OffsetRange range) {
        return key(range.getTopic(), range.getPartition());
    }

    private static String key(String topic, int partition) {
        return topic + '+' + partition;
    }

    /** All offset ranges as a stream. For any given topic/partition, this yields an
     * consistent result but values from different topic/partitions may not be consistent. */
    public Stream<OffsetRange> ranges() {
        return ranges.entrySet().stream()
                .sorted(Comparator.comparing(Map.Entry::getKey))
                .flatMap(v -> v.getValue().read(s -> new ArrayList<>(s).stream()));
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

}
