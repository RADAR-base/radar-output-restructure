package org.radarbase.output.path

import org.radarbase.output.path.RecordPathFactory.Companion.sanitizeId
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

class TimePathFormatterPlugin : PathFormatterPlugin() {
    override val prefix: String = "time"

    override val allowedFormats: String = "time:YYYY-mm-dd"

    override fun lookup(parameterContents: String): PathFormatParameters.() -> String {
        val dateFormatter = DateTimeFormatter
            .ofPattern(parameterContents)
            .withZone(ZoneOffset.UTC)
        return {
            sanitizeId(
                time?.let { dateFormatter.format(it) },
                "unknown-time",
            )
        }
    }
}
