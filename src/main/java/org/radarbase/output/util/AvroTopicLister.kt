package org.radarbase.output.util

import org.radarbase.output.source.SourceStorage
import java.nio.file.Path

class AvroTopicLister(
    private val storage: SourceStorage,
) : TreeLister.LevelLister<Path, Path> {
    override suspend fun listLevel(
        context: Path,
        descend: suspend (Path) -> Unit,
        emit: suspend (Path) -> Unit,
    ) {
        val fileStatuses = storage.list(context, maxKeys = 256)

        val avroFile = fileStatuses.find { file ->
            !file.isDirectory &&
                file.path.fileName.toString().endsWith(".avro", true)
        }

        if (avroFile != null) {
            emit(avroFile.path.parent.parent)
        } else {
            fileStatuses
                .filter { file -> file.isDirectory && file.path.fileName.toString() != "+tmp" }
                .forEach { file -> descend(file.path) }
        }
    }

    companion object {
        fun SourceStorage.avroTopicTreeLister() = TreeLister(AvroTopicLister(this))
    }
}
