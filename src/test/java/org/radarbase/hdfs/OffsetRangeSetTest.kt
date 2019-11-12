package org.radarbase.hdfs

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue

import org.junit.jupiter.api.Test
import org.radarbase.hdfs.accounting.OffsetRange
import org.radarbase.hdfs.accounting.OffsetRangeSet
import org.radarbase.hdfs.accounting.TopicPartition

class OffsetRangeSetTest {
    @Test
    fun containsGap() {
        val topicPartition = TopicPartition("", 0)
        val am = OffsetRange(topicPartition, -1, 0)
        val az = OffsetRange(topicPartition, 0, 0)
        val a = OffsetRange(topicPartition, 0, 1)
        val c = OffsetRange(topicPartition, 3, 4)
        val ac = OffsetRange(topicPartition, 0, 3)
        val ap = OffsetRange(topicPartition, 0, 5)
        val set = OffsetRangeSet()
        assertFalse(set.contains(a))
        assertEquals(0, set.size(topicPartition))
        set.add(a)
        assertEquals(1, set.size(topicPartition))
        assertTrue(set.contains(a))
        assertTrue(set.contains(az))
        assertFalse(set.contains(c))
        assertFalse(set.contains(ac))
        set.add(c)
        assertEquals(2, set.size(topicPartition), set.toString())
        assertTrue(set.contains(c))
        assertTrue(set.contains(a))
        assertFalse(set.contains(ac))
        set.add(ac)
        assertEquals(1, set.size(topicPartition))
        set.add(az)
        assertEquals(1, set.size(topicPartition))
        set.add(am)
        assertEquals(1, set.size(topicPartition))
        assertTrue(set.contains(ac))
        assertTrue(set.contains(am))
        assertFalse(set.contains(ap))
        set.add(ap)
        assertEquals(1, set.size(topicPartition))
    }

    @Test
    fun testGap() {
        OffsetRangeSet.OffsetRangeLists().run {
            add(0, 2)
            add(4, 5)
            assertEquals(2, size())
            assertTrue(contains(0, 1))
            assertFalse(contains(3))
            assertFalse(contains(0, 5))
            add(3)
            assertEquals(1, size())
            assertTrue(contains(3))
            assertTrue(contains(0, 5))
        }
    }
}