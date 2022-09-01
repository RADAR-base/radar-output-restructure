package org.radarbase.output.path

import org.radarbase.output.path.RecordPathFactory.Companion.sanitizeId
import org.slf4j.LoggerFactory
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

class FixedPathFormatterPlugin : PathFormatterPlugin() {
    private lateinit var timeBinFormat: DateTimeFormatter
    override val name: String = "fixed"

    override val allowedFormats: String = allowedParamNames.joinToString(separator = ", ")

    override fun init(properties: Map<String, String>) {
        super.init(properties)
        timeBinFormat = createTimeBinFormatter(properties["timeBinFormat"])
    }

    private fun createTimeBinFormatter(pattern: String?): DateTimeFormatter {
        pattern ?: return HOURLY_TIME_BIN_FORMAT

        return try {
            DateTimeFormatter
                .ofPattern(pattern)
                .withZone(ZoneOffset.UTC)
        } catch (ex: IllegalArgumentException) {
            logger.error(
                "Cannot use time bin format {}, using {} instead",
                pattern,
                HOURLY_TIME_BIN_FORMAT,
                ex,
            )
            HOURLY_TIME_BIN_FORMAT
        }
    }

    override fun lookup(parameterContents: String): PathFormatParameters.() -> String = when (parameterContents) {
        "projectId" -> ({ sanitizeId(key.get("projectId"), "unknown-project") })
        "userId" -> ({ sanitizeId(key.get("userId"), "unknown-user") })
        "sourceId" -> ({ sanitizeId(key.get("sourceId"), "unknown-source") })
        "topic" -> ({ topic })
        "filename" -> (
            {
                val timeBin = sanitizeId(time?.let { timeBinFormat.format(it) }, "unknown-time")
                timeBin + attempt.toAttemptSuffix() + extension
            }
        )
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

        val HOURLY_TIME_BIN_FORMAT: DateTimeFormatter = DateTimeFormatter
            .ofPattern("yyyyMMdd_HH'00'")
            .withZone(ZoneOffset.UTC)

        private fun Int.toAttemptSuffix() = if (this == 0) "" else "_$this"

        private val logger = LoggerFactory.getLogger(FixedPathFormatterPlugin::class.java)
    }
}
