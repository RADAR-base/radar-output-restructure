package org.radarbase.output.config

import org.radarbase.output.config.RestructureConfig.Companion.copyOnChange

data class DeduplicationConfig(
    /** Whether to enable deduplication. */
    val enable: Boolean? = null,
    /**
     * Only deduplicate using given fields. Fields not specified here are ignored
     * for determining duplication.
     */
    val distinctFields: Set<String>? = null,
    /**
     * Ignore given fields for determining whether a row is identical to another.
     */
    val ignoreFields: Set<String>? = null,
) {
    fun withDefaults(deduplicationDefaults: DeduplicationConfig): DeduplicationConfig =
        deduplicationDefaults
            .copyOnChange<DeduplicationConfig, Boolean?>(null, { enable }) { copy(enable = it) }
            .copyOnChange<DeduplicationConfig, Set<String>?>(null, { distinctFields }) {
                copy(distinctFields = it)
            }
            .copyOnChange<DeduplicationConfig, Set<String>?>(null, { ignoreFields }) {
                copy(ignoreFields = it)
            }
}
