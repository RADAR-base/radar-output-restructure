package org.radarbase.output.util

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.radarbase.output.util.TimeUtil.toDouble
import java.time.Instant

internal class TimeUtilTest {
    @Test
    fun instantDouble() {
        val now = System.currentTimeMillis()
        val instant = Instant.ofEpochMilli(now)
        val instantDouble = instant.toDouble()
        assertEquals(now / 1000.0, instantDouble)
    }
}
