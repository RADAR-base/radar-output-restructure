package org.radarbase.output.util

import kotlinx.coroutines.flow.toList
import org.radarbase.output.source.StorageIndex
import org.radarbase.output.source.StorageNode
import java.nio.file.Path

class AvroTopicLister(
    private val storage: StorageIndex,
) : TreeLister.LevelLister<Path, Path> {
    override suspend fun listLevel(
        context: Path,
        descend: suspend (Path) -> Unit,
        emit: suspend (Path) -> Unit,
    ) {
        val fileStatuses = storage.list(StorageNode.StorageDirectory(context), maxKeys = 256)
            .toList()

        val avroFile = fileStatuses.find { file ->
            file is StorageNode.StorageFile &&
                file.path.fileName.toString().endsWith(".avro", true)
        }

        if (avroFile != null) {
            emit(avroFile.path.parent.parent)
        } else {
            fileStatuses
                .filter { file -> file is StorageNode.StorageDirectory && file.path.fileName.toString() != "+tmp" }
                .forEach { file -> descend(file.path) }
        }
    }

    companion object {
        fun StorageIndex.avroTopicTreeLister() = TreeLister(AvroTopicLister(this))
    }
}
