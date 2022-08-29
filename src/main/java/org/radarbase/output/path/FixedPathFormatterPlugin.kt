package org.radarbase.output.path

import org.radarbase.output.path.RecordPathFactory.Companion.sanitizeId

class FixedPathFormatterPlugin : PathFormatterPlugin() {
    override val allowedFormats: String = allowedParamNames.joinToString(separator = ", ")

    override fun lookup(parameterContents: String): PathFormatParameters.() -> String = when (parameterContents) {
        "projectId" -> ({ sanitizeId(key.get("projectId"), "unknown-project") })
        "userId" -> ({ sanitizeId(key.get("userId"), "unknown-user") })
        "sourceId" -> ({ sanitizeId(key.get("sourceId"), "unknown-source") })
        "topic" -> ({ topic })
        "filename" -> ({ timeBin + attempt.toAttemptSuffix() + extension })
        "attempt" -> ({ attempt.toAttemptSuffix() })
        "extension" -> ({ extension })
        else -> throw IllegalArgumentException("Unknown path parameter $parameterContents")
    }

    override fun extractParamContents(paramName: String): String? = paramName.takeIf { it in allowedParamNames }

    companion object {
        val allowedParamNames = setOf(
            "projectId",
            "userId",
            "sourceId",
            "topic",
            "filename",
            "attempt",
            "extension",
        )

        private fun Int.toAttemptSuffix() = if (this == 0) "" else "_$this"
    }
}
