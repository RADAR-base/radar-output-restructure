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

package org.radarcns;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

/** Encompasses a range of offsets. */
public class OffsetRangeSet {
    private final Map<String, SortedSet<OffsetRange>> ranges;

    public OffsetRangeSet() {
        ranges = new HashMap<>();
    }

    /** Add given offset range to seen offsets. */
    public void add(OffsetRange range) {
        SortedSet<OffsetRange> topicRanges = ranges.computeIfAbsent(
                range.getTopic() + '+' + range.getPartition(), k -> new TreeSet<>());

        SortedSet<OffsetRange> tail = topicRanges.tailSet(range);
        SortedSet<OffsetRange> head = topicRanges.headSet(range);

        if (!tail.isEmpty()) {
            if (tail.first().equals(range)) {
                return;
            }

            if (tail.first().getOffsetFrom() <= range.getOffsetTo() + 1) {
                if (!head.isEmpty() && head.last().getOffsetTo() >= range.getOffsetFrom() - 1) {
                    tail.first().setOffsetFrom(head.last().getOffsetFrom());
                    topicRanges.remove(head.last());
                } else {
                    tail.first().setOffsetFrom(range.getOffsetFrom());
                }
                return;
            }
        }

        if (!head.isEmpty() && head.last().getOffsetTo() >= range.getOffsetFrom() - 1) {
            head.last().setOffsetTo(range.getOffsetTo());
            return;
        }

        topicRanges.add(range);
    }

    /** Whether this range set completely contains the given range. */
    public boolean contains(OffsetRange range) {
        String key = range.getTopic() + '+' + range.getPartition();
        SortedSet<OffsetRange> topicRanges = ranges.get(key);
        if (topicRanges == null) {
            return false;
        }

        if (topicRanges.contains(range)) {
            return true;
        }

        SortedSet<OffsetRange> tail = topicRanges.tailSet(range);
        if (!tail.isEmpty()
                && tail.first().getOffsetFrom() == range.getOffsetFrom()
                && tail.first().getOffsetTo() >= range.getOffsetTo()) {
            return true;
        }

        SortedSet<OffsetRange> head = topicRanges.headSet(range);
        return !head.isEmpty() && head.last().getOffsetTo() >= range.getOffsetTo();
    }

    @Override
    public String toString() {
        return "OffsetRangeSet" + ranges;
    }

    /** Whether the stored offsets is empty. */
    public boolean isEmpty() {
        return ranges.isEmpty();
    }
}
