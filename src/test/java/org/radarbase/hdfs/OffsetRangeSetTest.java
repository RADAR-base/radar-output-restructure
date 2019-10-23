package org.radarbase.hdfs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.radarbase.hdfs.accounting.OffsetRange;
import org.radarbase.hdfs.accounting.OffsetRangeSet;
import org.radarbase.hdfs.accounting.TopicPartition;

public class OffsetRangeSetTest {
    @Test
    public void containsGap() {
        TopicPartition topicPartition = new TopicPartition("", 0);
        OffsetRange am = new OffsetRange(topicPartition, -1, 0);
        OffsetRange az = new OffsetRange(topicPartition, 0, 0);
        OffsetRange a = new OffsetRange(topicPartition, 0, 1);
        OffsetRange c = new OffsetRange(topicPartition, 3, 4);
        OffsetRange ac = new OffsetRange(topicPartition, 0, 3);
        OffsetRange ap = new OffsetRange(topicPartition, 0, 5);
        OffsetRangeSet set = new OffsetRangeSet();
        assertFalse(set.contains(a));
        assertEquals(0, set.size(topicPartition));
        set.add(a);
        assertEquals(1, set.size(topicPartition));
        assertTrue(set.contains(a));
        assertTrue(set.contains(az));
        assertFalse(set.contains(c));
        assertFalse(set.contains(ac));
        set.add(c);
        assertEquals(2, set.size(topicPartition), set.toString());
        assertTrue(set.contains(c));
        assertTrue(set.contains(a));
        assertFalse(set.contains(ac));
        set.add(ac);
        assertEquals(1, set.size(topicPartition));
        set.add(az);
        assertEquals(1, set.size(topicPartition));
        set.add(am);
        assertEquals(1, set.size(topicPartition));
        assertTrue(set.contains(ac));
        assertTrue(set.contains(am));
        assertFalse(set.contains(ap));
        set.add(ap);
        assertEquals(1, set.size(topicPartition));
    }

    @Test
    public void testGap() {
        OffsetRangeSet.OffsetRangeLists offsets = new OffsetRangeSet.OffsetRangeLists();
        offsets.add(0, 2);
        offsets.add(4, 5);
        assertEquals(2, offsets.size());
        assertTrue(offsets.contains(0, 1));
        assertFalse(offsets.contains(3));
        assertFalse(offsets.contains(0, 5));
        offsets.add(3);
        assertEquals(1, offsets.size());
        assertTrue(offsets.contains(3));
        assertTrue(offsets.contains(0, 5));
    }
}