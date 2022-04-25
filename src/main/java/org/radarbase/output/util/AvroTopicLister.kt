package org.radarbase.output.util

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import org.radarbase.output.source.SourceStorage
import java.nio.file.Path

class AvroTopicLister(private val storage: SourceStorage) : TreeLister.LevelLister<Path, Path> {
    override fun CoroutineScope.listLevel(
        context: Path,
        descend: CoroutineScope.(Path) -> Unit,
        emit: suspend (Path) -> Unit,
    ) = launch {
        val fileStatuses = storage.list(context)

        val avroFile = fileStatuses.find { file ->
            !file.isDirectory
                && file.path.fileName.toString().endsWith(".avro", true)
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
