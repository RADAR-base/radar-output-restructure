package org.radarbase.output.source

import org.radarbase.output.accounting.TopicPartitionOffsetRange
import java.nio.file.Path
import java.time.Instant

data class TopicFileList(val topic: String, val files: List<TopicFile>) {
    val numberOfOffsets: Long? = this.files
            .takeIf { fs -> fs.none { it.size == null } }
            ?.fold(0L) { sum, f -> sum + f.size!! }
    val numberOfFiles: Int = this.files.size
}

data class TopicFile(val topic: String, val path: Path, val lastModified: Instant, val range: TopicPartitionOffsetRange) {
    constructor(topic: String, path: Path, lastModified: Instant) : this(topic, path, lastModified, TopicPartitionOffsetRange.parseFilename(path.fileName.toString(), lastModified))
    val size: Long? = range.range.size
}

data class SimpleFileStatus(val path: Path, val isDirectory: Boolean, val lastModified: Instant?)
