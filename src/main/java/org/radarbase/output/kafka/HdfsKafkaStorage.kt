package org.radarbase.output.kafka

import org.apache.avro.file.SeekableInput
import org.apache.avro.mapred.FsInput
import org.apache.hadoop.fs.FileSystem
import java.nio.file.Path
import java.nio.file.Paths


class HdfsKafkaStorage(
        private val fileSystem: FileSystem
): KafkaStorage {
    override fun reader(): KafkaStorage.KafkaStorageReader = HDFSKafkaStorageReader()

    override fun list(path: Path): Sequence<SimpleFileStatus> {
        val hdfsPath = org.apache.hadoop.fs.Path(path.toString())
        return fileSystem.listStatus(hdfsPath)
                .asSequence()
                .map { SimpleFileStatus(Paths.get(it.path.toUri().path), it.isDirectory) }
    }

    inner class HDFSKafkaStorageReader : KafkaStorage.KafkaStorageReader {
        override fun newInput(file: TopicFile): SeekableInput {
            val hdfsPath = org.apache.hadoop.fs.Path(file.path.toString())
            return FsInput(hdfsPath, fileSystem)
        }

        override fun close() = Unit
    }
}
