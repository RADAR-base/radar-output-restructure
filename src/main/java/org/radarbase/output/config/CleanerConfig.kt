package org.radarbase.output.config

data class CleanerConfig(
    /** Whether to enable the cleaner. */
    val enable: Boolean = false,
    /** How often to run the cleaner in seconds. */
    val interval: Long = 1260L,
    /** Age in days after an avro file can be removed. Must be strictly positive. */
    val age: Int = 7,
    /** Maximum number of files to clean in a given topic. */
    val maxFilesPerTopic: Int? = null,
) {
    fun validate() {
        check(age > 0) { "Cleaner file age must be strictly positive" }
        check(interval > 0) { "Cleaner interval must be strictly positive" }
        if (maxFilesPerTopic != null) check(maxFilesPerTopic > 0) { "Maximum files per topic must be strictly positive" }
    }
}
