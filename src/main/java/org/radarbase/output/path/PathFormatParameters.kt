package org.radarbase.output.path

import org.apache.avro.generic.GenericRecord
import java.time.Instant

data class PathFormatParameters(
    val topic: String,
    val key: GenericRecord,
    val value: GenericRecord,
    val time: Instant?,
    val attempt: Int,
    val extension: String,
    val computeTimeBin: (time: Instant?) -> String,
) {
    val timeBin: String
        get() = computeTimeBin(time)
}
