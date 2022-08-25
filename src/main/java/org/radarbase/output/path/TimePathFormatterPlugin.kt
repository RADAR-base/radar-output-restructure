package org.radarbase.output.path

import org.radarbase.output.path.RecordPathFactory.Companion.sanitizeId
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

class TimePathFormatterPlugin : PathFormatterPlugin() {
    override val allowedFormats: String = "time:YYYY-mm-dd"

    override fun createLookupTable(
        parameterNames: Set<String>
    ): Map<String, PathFormatParameters.() -> String> {
        return parameterNames
            .filter { it.startsWith("time:") }
            .associateWith { p ->
                val dateFormatter = DateTimeFormatter
                    .ofPattern(p.removePrefix("time:"))
                    .withZone(ZoneOffset.UTC)
                return@associateWith {
                    sanitizeId(
                        time?.let { dateFormatter.format(it) },
                        "unknown-time",
                    )
                }
            }
    }
}
