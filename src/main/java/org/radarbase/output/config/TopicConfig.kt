package org.radarbase.output.config

data class TopicConfig(
    /** Topic-specific deduplication handling. */
    val deduplication: DeduplicationConfig = DeduplicationConfig(),
    /** Whether to exclude the topic from being processed. */
    val exclude: Boolean = false,
    /**
     * Whether to exclude the topic from being deleted, if this configuration has been set
     * in the service.
     */
    val excludeFromDelete: Boolean = false,
) {
    fun deduplication(deduplicationDefault: DeduplicationConfig): DeduplicationConfig =
        deduplication
            .withDefaults(deduplicationDefault)
}
