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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

/** Encompasses a range of offsets. */
public class OffsetRangeSet {
    private static final Logger logger = LoggerFactory.getLogger(OffsetRangeSet.class);
    private final ConcurrentMap<String, SynchronizedSortedSet<OffsetRange>> ranges;

    public OffsetRangeSet() {
        ranges = new ConcurrentHashMap<>();
    }

    /** Add given offset range to seen offsets. */
    public void add(OffsetRange range) {
        SynchronizedSortedSet<OffsetRange> syncSet = ranges.computeIfAbsent(
                key(range.getTopic(), range.getPartition()), k -> new SynchronizedSortedSet<>());

        SortedSet<OffsetRange> topicRanges = syncSet.writeSet();

        try {
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
        } finally {
            syncSet.releaseWrite();
        }
    }

    /** Whether this range set completely contains the given range. */
    public boolean contains(OffsetRange range) {
        SynchronizedSortedSet<OffsetRange> syncSet = ranges.get(key(range.getTopic(), range.getPartition()));

        if (syncSet == null) {
            return false;
        }

        SortedSet<OffsetRange> topicRanges = syncSet.readSet();

        try {
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
        } finally {
            syncSet.releaseRead();
        }
    }

    public int size(String topic, int partition) {
        SynchronizedSortedSet<OffsetRange> syncSet = ranges.get(key(topic, partition));
        if (syncSet != null) {
            int size = syncSet.readSet().size();
            syncSet.releaseRead();
            return size;
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

    public Stream<OffsetRange> ranges() {
        return ranges.entrySet().stream()
                .sorted(Comparator.comparing(Map.Entry::getKey))
                .flatMap(v -> {
                    try {
                        return new ArrayList<>(v.getValue().readSet()).stream();
                    } finally {
                        v.getValue().releaseRead();
                    }
                });
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

    private static class SynchronizedSortedSet<T> {
        private final ReadWriteLock lock = new ReentrantReadWriteLock();
        private final SortedSet<T> set;
        private final Lock readLock;
        private final Lock writeLock;

        SynchronizedSortedSet() {
            this.set = new TreeSet<>();
            this.readLock = lock.readLock();
            this.writeLock = lock.writeLock();
        }

        SortedSet<T> readSet() {
            readLock.lock();
            return set;
        }

        SortedSet<T> writeSet() {
            writeLock.lock();
            return set;
        }

        void releaseRead() {
            readLock.unlock();

        }
        void releaseWrite() {
            writeLock.unlock();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SynchronizedSortedSet<?> that = (SynchronizedSortedSet<?>) o;

            try {
                return readSet().equals(that.readSet());
            } finally {
                that.releaseRead();
                releaseRead();
            }
        }

        @Override
        public int hashCode() {
            try {
                return readSet().hashCode();
            } finally {
                releaseRead();
            }
        }
    }
}
