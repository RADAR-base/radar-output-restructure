package org.radarbase.output.config

import org.radarbase.output.Application

data class WorkerConfig(
    /** Whether to enable restructuring */
    val enable: Boolean = true,
    /** Number of threads to use for processing files. */
    val numThreads: Int = 1,
    /**
     * Maximum number of files to process for a given topic. Limit this to ensure that a single
     * processing iteration including lock takes a limited amount of time.
     */
    val maxFilesPerTopic: Int? = null,
    /**
     * Number of files to simultaneously keep in cache, including open writer. A higher size will
     * decrease overhead but increase memory usage and open file descriptors.
     */
    val cacheSize: Int = Application.CACHE_SIZE_DEFAULT,
    /**
     * Number of offsets to simultaneously keep in cache. A higher size will
     * decrease overhead but increase memory usage.
     */
    val cacheOffsetsSize: Long = 500_000,
    /**
     * Minimum time since the file was last modified in seconds. Avoids
     * synchronization issues that may occur in a source file that is being
     * appended to.
     */
    val minimumFileAge: Long = 60,
) {
    init {
        check(cacheSize > 0) { "Maximum files per topic must be strictly positive" }
        if (maxFilesPerTopic != null) check(maxFilesPerTopic > 0) { "Maximum files per topic must be strictly positive" }
        check(numThreads > 0) { "Number of threads should be at least 1" }
    }
}
