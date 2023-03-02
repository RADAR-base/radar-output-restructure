package org.radarbase.output.path

import org.radarbase.output.path.RecordPathFactory.Companion.sanitizeId
import org.slf4j.LoggerFactory
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

class FixedPathFormatterPlugin : PathFormatterPlugin.Factory {
    override fun create(
        properties: Map<String, String>
    ): PathFormatterPlugin = Plugin(properties)

    internal class Plugin(properties: Map<String, String>) : PathFormatterPlugin() {
        private val extension: String = properties["extension"] ?: ""
        private val timeBinFormat: DateTimeFormatter = createTimeBinFormatter(properties["timeBinFormat"])
        override val name: String = "fixed"

        override val allowedFormats: String = allowedParamNames.joinToString(separator = ", ")

        override fun lookup(parameterContents: String): suspend PathFormatParameters.() -> String =
            when (parameterContents) {
                "projectId" -> ({ sanitizeId(key.get("projectId"), "unknown-project") })
                "userId" -> ({ sanitizeId(key.get("userId"), "unknown-user") })
                "sourceId" -> ({ sanitizeId(key.get("sourceId"), "unknown-source") })
                "topic" -> ({ topic })
                "filename" -> {
                    {
                        val timeBin = if (time != null) {
                            timeBinFormat.format(time)
                        } else {
                            "unknown-time"
                        }
                        timeBin + attempt.toAttemptSuffix() + extension
                    }
                }
                "attempt" -> ({ attempt.toAttemptSuffix() })
                "extension" -> ({ extension })
                else -> throw IllegalArgumentException("Unknown path parameter $parameterContents")
            }

        override fun extractParamContents(paramName: String): String? =
            paramName.takeIf { it in allowedParamNames }

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

            private val HOURLY_TIME_BIN_FORMAT: DateTimeFormatter = DateTimeFormatter
                .ofPattern("yyyyMMdd_HH'00'")
                .withZone(ZoneOffset.UTC)

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

            private fun Int.toAttemptSuffix() = if (this == 0) "" else "_$this"

            private val logger = LoggerFactory.getLogger(Plugin::class.java)
        }
    }
}
