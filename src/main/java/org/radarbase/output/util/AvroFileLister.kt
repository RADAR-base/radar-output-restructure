package org.radarbase.output.util

import org.radarbase.output.source.SourceStorage
import org.radarbase.output.source.TopicFile

class AvroFileLister(
    private val storage: SourceStorage,
) : TreeLister.LevelLister<TopicFile, TopicPath> {

    override suspend fun listLevel(
        context: TopicPath,
        descend: suspend (TopicPath) -> Unit,
        emit: suspend (TopicFile) -> Unit,
    ) {
        storage.list(context.path).forEach { status ->
            val filename = status.path.fileName.toString()
            when {
                status.isDirectory && filename != "+tmp" -> descend(context.copy(path = status.path))
                filename.endsWith(".avro") -> emit(storage.createTopicFile(context.topic, status))
                else -> {}
            }
        }
    }

    companion object {
        fun SourceStorage.avroFileTreeLister() = TreeLister(AvroFileLister(this))
    }
}
