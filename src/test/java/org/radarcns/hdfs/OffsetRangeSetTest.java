package org.radarcns.hdfs;

import org.junit.Test;
import org.radarcns.hdfs.accounting.OffsetRange;
import org.radarcns.hdfs.accounting.OffsetRangeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OffsetRangeSetTest {
    @Test
    public void containsGap() {
        OffsetRange am = new OffsetRange("", 0, -1, 0);
        OffsetRange az = new OffsetRange("", 0, 0, 0);
        OffsetRange a = new OffsetRange("", 0, 0, 1);
        OffsetRange c = new OffsetRange("", 0, 3, 4);
        OffsetRange ac = new OffsetRange("", 0, 0, 3);
        OffsetRange ap = new OffsetRange("", 0, 0, 5);
        OffsetRangeSet set = new OffsetRangeSet();
        assertFalse(set.contains(a));
        assertEquals(0, set.size("", 0));
        set.add(a);
        assertEquals(1, set.size("", 0));
        assertTrue(set.contains(a));
        assertTrue(set.contains(az));
        assertFalse(set.contains(c));
        assertFalse(set.contains(ac));
        set.add(c);
        assertEquals(set.toString(), 2, set.size("", 0));
        assertTrue(set.contains(c));
        assertTrue(set.contains(a));
        assertFalse(set.contains(ac));
        set.add(ac);
        assertEquals(1, set.size("", 0));
        set.add(az);
        assertEquals(1, set.size("", 0));
        set.add(am);
        assertEquals(1, set.size("", 0));
        assertTrue(set.contains(ac));
        assertTrue(set.contains(am));
        assertFalse(set.contains(ap));
        set.add(ap);
        assertEquals(1, set.size("", 0));
    }

}