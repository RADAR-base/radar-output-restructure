package org.radarbase.output.cleaner

import org.radarbase.output.source.TopicFile
import org.radarbase.output.util.SuspendedCloseable

interface ExtractionCheck: SuspendedCloseable {
    suspend fun isExtracted(file: TopicFile): Boolean
}
