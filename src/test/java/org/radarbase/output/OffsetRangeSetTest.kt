package org.radarbase.output

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.radarbase.output.accounting.OffsetRangeSet
import org.radarbase.output.accounting.TopicPartition
import org.radarbase.output.accounting.TopicPartitionOffsetRange
import java.time.Instant

class OffsetRangeSetTest {
    private val lastModified = Instant.now()
    private val topicPartition = TopicPartition("", 0)

    @Test
    fun containsGap() {
        val am =
            TopicPartitionOffsetRange(topicPartition, OffsetRangeSet.Range(-1, 0, lastModified))
        val az = TopicPartitionOffsetRange(topicPartition, OffsetRangeSet.Range(0, 0, lastModified))
        val a = TopicPartitionOffsetRange(topicPartition, OffsetRangeSet.Range(0, 1, lastModified))
        val c = TopicPartitionOffsetRange(topicPartition, OffsetRangeSet.Range(3, 4, lastModified))
        val ac = TopicPartitionOffsetRange(topicPartition, OffsetRangeSet.Range(0, 3, lastModified))
        val ap = TopicPartitionOffsetRange(topicPartition, OffsetRangeSet.Range(0, 5, lastModified))
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
}
