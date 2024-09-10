package org.radarbase.output.util

import kotlinx.coroutines.flow.toList
import org.radarbase.output.source.SourceStorage
import org.radarbase.output.source.StorageIndex
import org.radarbase.output.source.StorageNode
import org.radarbase.output.source.TopicFile

class AvroFileLister(
    private val storage: SourceStorage,
    private val storageIndex: StorageIndex,
) : TreeLister.LevelLister<TopicFile, TopicPath> {

    override suspend fun listLevel(
        context: TopicPath,
        descend: suspend (TopicPath) -> Unit,
        emit: suspend (TopicFile) -> Unit,
    ) {
        storageIndex.list(StorageNode.StorageDirectory(context.path))
            .toList()
            .forEach { status ->
                val filename = status.path.fileName.toString()
                when {
                    status is StorageNode.StorageDirectory && filename != "+tmp" -> descend(context.copy(path = status.path))
                    status is StorageNode.StorageFile && filename.endsWith(".avro") -> emit(storage.createTopicFile(context.topic, status))
                    else -> {}
                }
            }
    }

    companion object {
        fun StorageIndex.avroFileTreeLister(sourceStorage: SourceStorage) = TreeLister(AvroFileLister(sourceStorage, this))
    }
}
