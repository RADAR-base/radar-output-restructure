package org.radarbase.output.cleaner

import org.radarbase.output.source.TopicFile
import java.io.Closeable

interface ExtractionCheck: Closeable {
    fun isExtracted(file: TopicFile): Boolean
}
