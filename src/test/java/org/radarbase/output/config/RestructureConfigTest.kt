package org.radarbase.output.config

import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class RestructureConfigTest {
    @Test
    fun emptyIncludeProjectIdsIncludesAll() {
        val config = RestructureConfig()
        assertTrue(config.projectIncluded("RECURRENT-GB"))
        assertTrue(config.projectIncluded(null))
    }

    @Test
    fun includeProjectIdsFiltersProjects() {
        val config = RestructureConfig(includeProjectIds = setOf("RECURRENT-GB", "RECURRENT-GB-Proxy"))
        assertTrue(config.projectIncluded("RECURRENT-GB"))
        assertTrue(config.projectIncluded("RECURRENT-GB-Proxy"))
        assertFalse(config.projectIncluded("OTHER-STUDY"))
        assertFalse(config.projectIncluded(null))
    }
}
