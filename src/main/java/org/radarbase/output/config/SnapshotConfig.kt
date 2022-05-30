package org.radarbase.output.config

import java.time.Duration

data class SnapshotConfig(
    val enable: Boolean = false,
    val frequency: Duration = Duration.ofDays(31),
    val numberOfSnapshots: Int = 12,
    val sourceFormat: String = "\${projectId}",
    val targetFormat: String = "\${projectId}",
)
