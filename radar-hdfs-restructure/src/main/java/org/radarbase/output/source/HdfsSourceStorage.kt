package org.radarbase.output.source

import org.apache.avro.file.SeekableInput
import org.apache.avro.mapred.FsInput
import org.apache.hadoop.fs.FileSystem
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Instant


class HdfsSourceStorage(
        private val fileSystem: FileSystem
): SourceStorage {
    override val walker: SourceStorageWalker = GeneralSourceStorageWalker(this)

    override fun createReader(): SourceStorage.SourceStorageReader = HDFSSourceStorageReader()

    override fun list(path: Path): Sequence<SimpleFileStatus> {
        return fileSystem.listStatus(path.toHdfsPath())
                .asSequence()
                .map { SimpleFileStatus(
                        Paths.get(it.path.toUri().path),
                        it.isDirectory,
                        Instant.ofEpochMilli(it.modificationTime))
                }
    }

    override fun delete(path: Path) {
        fileSystem.delete(path.toHdfsPath(), false)
    }

    inner class HDFSSourceStorageReader : SourceStorage.SourceStorageReader {
        override fun newInput(file: TopicFile): SeekableInput {
            return FsInput(file.path.toHdfsPath(), fileSystem)
        }

        override fun close() = Unit
    }

    companion object {
        private fun Path.toHdfsPath() = org.apache.hadoop.fs.Path(toString())
    }
}
