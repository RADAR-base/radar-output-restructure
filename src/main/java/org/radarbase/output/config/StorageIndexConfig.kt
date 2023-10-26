package org.radarbase.output.config

import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

data class StorageIndexConfig(
    /** How often to fully sync the storage index, in seconds. */
    val fullSyncInterval: Long = 3600L,
    /**
     * How often to sync empty directories with the storage index, in seconds.
     * If this is very large, empty directories will only be scanned during
     * full sync.
     */
    val emptyDirectorySyncInterval: Long = 900L,
) {
    val fullSyncDuration: Duration
        get() = fullSyncInterval.seconds

    val emptyDirectorySyncDuration: Duration
        get() = emptyDirectorySyncInterval.seconds
}
