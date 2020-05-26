package org.radarbase.output.accounting

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.Instant

internal class OffsetIntervalsTest {
    private val lastModified = Instant.now()
    private val futureModified = Instant.now().plusMillis(1)

    @Test
    fun testGapFuture() {
        OffsetIntervals().run {
            add(OffsetRangeSet.Range(0, 2, lastModified))
            add(OffsetRangeSet.Range(4, 5, lastModified))
            assertEquals(2, size())
            assertTrue(contains(OffsetRangeSet.Range(0, 1, lastModified)))
            assertFalse(contains(OffsetRangeSet.Range(0, 1, futureModified)))
            assertFalse(contains(3, lastModified))
            assertFalse(contains(OffsetRangeSet.Range(0, 5, lastModified)))
            assertFalse(contains(OffsetRangeSet.Range(0, 5, futureModified)))
            add(3, lastModified)
            assertEquals(1, size())
            assertTrue(contains(3, lastModified))
            assertFalse(contains(3, futureModified))
            assertTrue(contains(OffsetRangeSet.Range(0, 5, lastModified)))
            assertFalse(contains(OffsetRangeSet.Range(0, 5, futureModified)))
        }
    }

    @Test
    fun testGapFutureInsert() {
        OffsetIntervals().run {
            add(OffsetRangeSet.Range(0, 2, lastModified))
            add(OffsetRangeSet.Range(4, 5, futureModified))
            assertEquals(2, size())
            assertTrue(contains(OffsetRangeSet.Range(0, 1, lastModified)))
            assertFalse(contains(OffsetRangeSet.Range(0, 1, futureModified)))
            assertFalse(contains(3, lastModified))
            assertFalse(contains(OffsetRangeSet.Range(0, 5, lastModified)))
            assertFalse(contains(OffsetRangeSet.Range(0, 5, futureModified)))
            assertTrue(contains(OffsetRangeSet.Range(4, 5, lastModified)))
            assertTrue(contains(OffsetRangeSet.Range(4, 5, futureModified)))
            add(3, lastModified)
            assertEquals(1, size())
            assertTrue(contains(3, lastModified))
            assertTrue(contains(3, futureModified))
            assertTrue(contains(OffsetRangeSet.Range(0, 5, lastModified)))
            assertTrue(contains(OffsetRangeSet.Range(0, 5, futureModified)))
        }
    }

    @Test
    fun testGapFutureAdd() {
        OffsetIntervals().run {
            add(OffsetRangeSet.Range(0, 2, lastModified))
            assertEquals(1, size())
            assertTrue(contains(OffsetRangeSet.Range(0, 1, lastModified)))
            assertFalse(contains(OffsetRangeSet.Range(0, 1, futureModified)))
            assertFalse(contains(3, lastModified))
            add(3, futureModified)
            assertEquals(1, size())
            assertTrue(contains(3, lastModified))
            assertTrue(contains(3, futureModified))
            assertTrue(contains(2, lastModified))
            assertTrue(contains(2, futureModified))
        }
    }
}
