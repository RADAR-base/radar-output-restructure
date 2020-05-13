package org.radarbase.output.kafka

import org.radarbase.output.accounting.OffsetRange
import java.nio.file.Path

data class TopicFileList(val topic: String, val files: List<TopicFile>) {
    val numberOfOffsets: Long = this.files.stream()
            .mapToLong { it.size }
            .sum()

    val numberOfFiles: Int = this.files.size
}

data class TopicFile(val topic: String, val path: Path) {
    val range: OffsetRange = OffsetRange.parseFilename(path.fileName.toString())
    val size: Long = 1 + range.offsetTo - range.offsetFrom
}

data class SimpleFileStatus(val path: Path, val isDirectory: Boolean)
