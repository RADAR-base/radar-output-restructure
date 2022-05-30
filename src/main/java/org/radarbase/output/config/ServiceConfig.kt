package org.radarbase.output.config

data class ServiceConfig(
    /** Whether to enable the service mode of this application. */
    val enable: Boolean,
    /** Polling interval in seconds. */
    val interval: Long = 300L,
    /** Age in days after an avro file can be removed. Ignored if not strictly positive. */
    val deleteAfterDays: Int = -1,
) {
    fun validate() {
        check(interval > 0) { "Cleaner interval must be strictly positive" }
    }
}
