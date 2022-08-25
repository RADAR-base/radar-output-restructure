package org.radarbase.output.path

import org.radarbase.output.path.RecordPathFactory.Companion.sanitizeId

class FixedPathFormatterPlugin : PathFormatterPlugin() {
    override val allowedFormats: String = lookupTable.keys.joinToString(separator = ", ")

    override fun createLookupTable(
        parameterNames: Set<String>,
    ): Map<String, PathFormatParameters.() -> String> = lookupTable.filterKeys { it in parameterNames }

    companion object {
        val lookupTable = mapOf<String, PathFormatParameters.() -> String>(
            "projectId" to { sanitizeId(key.get("projectId"), "unknown-project") },
            "userId" to { sanitizeId(key.get("userId"), "unknown-user") },
            "sourceId" to { sanitizeId(key.get("sourceId"), "unknown-source") },
            "topic" to { topic },
            "filename" to { timeBin + attempt.toAttemptSuffix() + extension },
            "attempt" to { attempt.toAttemptSuffix() },
            "extension" to { extension },
        )

        private fun Int.toAttemptSuffix() = if (this == 0) "" else "_$this"
    }
}
