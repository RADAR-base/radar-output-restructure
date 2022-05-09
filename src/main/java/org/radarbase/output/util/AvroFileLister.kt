package org.radarbase.output.util

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import org.radarbase.output.source.SourceStorage
import org.radarbase.output.source.TopicFile

class AvroFileLister(private val storage: SourceStorage) :
    TreeLister.LevelLister<TopicFile, TopicPath> {
    override fun CoroutineScope.listLevel(
        context: TopicPath,
        descend: CoroutineScope.(TopicPath) -> Unit,
        emit: suspend (TopicFile) -> Unit,
    ) = launch {
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
