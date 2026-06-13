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
    /**
     * Specify alternative path format, following
     * [org.radarbase.output.path.FormattedPathFactory] format.
     */
    val pathProperties: PathFormatterConfig = PathFormatterConfig(),
    /**
     * Exclude given fields from output files.
     */
    val excludeFields: Set<String>? = null,
    /**
     * If non-empty, only records whose [org.radarbase.output.path.RecordPathFactory.projectIdFrom]
     * value is in this set are written. Empty means all projects are included.
     */
    val includeProjectIds: Set<String> = emptySet(),
) {
    fun deduplication(deduplicationDefault: DeduplicationConfig): DeduplicationConfig =
        deduplication
            .withDefaults(deduplicationDefault)

    /** Whether a record with the given project ID should be written for this topic. */
    fun includesProject(projectId: String?): Boolean {
        if (includeProjectIds.isEmpty()) {
            return true
        }
        return projectId != null && projectId in includeProjectIds
    }
}
