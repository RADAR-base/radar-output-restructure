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

package org.radarcns.hdfs;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

/** Encompasses a range of offsets. */
public class OffsetRangeSet implements Iterable<OffsetRange> {
    private final SortedMap<String, SortedSet<OffsetRange>> ranges;

    public OffsetRangeSet() {
        ranges = new TreeMap<>();
    }

    /** Add given offset range to seen offsets. */
    public void add(OffsetRange range) {
        SortedSet<OffsetRange> topicRanges = ranges.computeIfAbsent(
                key(range.getTopic(), range.getPartition()), k -> new TreeSet<>());

        SortedSet<OffsetRange> tail = topicRanges.tailSet(range);
        SortedSet<OffsetRange> head = topicRanges.headSet(range);

        OffsetRange next = !tail.isEmpty() ? tail.first() : null;
        OffsetRange previous = !head.isEmpty() ? head.last() : null;

        if (next != null) {
            if (next.equals(range)) {
                return;
            }

            if (next.getOffsetFrom() <= range.getOffsetTo() + 1) {
                if (previous != null && previous.getOffsetTo() >= range.getOffsetFrom() - 1) {
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
    }

    /** Whether this range set completely contains the given range. */
    public boolean contains(OffsetRange range) {
        SortedSet<OffsetRange> topicRanges = ranges.get(key(range.getTopic(), range.getPartition()));
        if (topicRanges == null) {
            return false;
        }

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
    }

    public int size(String topic, int partition) {
        SortedSet<OffsetRange> rangeSet = ranges.get(key(topic, partition));
        if (rangeSet != null) {
            return rangeSet.size();
        } else {
            return 0;
        }
    }

    @Override
    public String toString() {
        return "OffsetRangeSet" + ranges;
    }

    /** Whether the stored offsets is empty. */
    public boolean isEmpty() {
        return ranges.isEmpty();
    }

    private static String key(String topic, int partition) {
        return topic + '+' + partition;
    }

    @Override
    @Nonnull
    public Iterator<OffsetRange> iterator() {
        final Iterator<SortedSet<OffsetRange>> partitionIterator = ranges.values().iterator();
        return new Iterator<OffsetRange>() {
            Iterator<OffsetRange> rangeIterator;
            @Override
            public boolean hasNext() {
                while (rangeIterator == null || !rangeIterator.hasNext()) {
                    if (!partitionIterator.hasNext()) {
                        return false;
                    }
                    rangeIterator = partitionIterator.next().iterator();
                }
                return rangeIterator.hasNext();
            }

            @Override
            public OffsetRange next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return rangeIterator.next();
            }
        };
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
