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
        val hdfsPath = org.apache.hadoop.fs.Path(path.toUri())
        return fileSystem.listStatus(hdfsPath)
                .asSequence()
                .map { SimpleFileStatus(Paths.get(it.path.toUri()), it.isDirectory) }
    }


    inner class HDFSKafkaStorageReader : KafkaStorage.KafkaStorageReader {
        override fun newInput(file: TopicFile): SeekableInput {
            val hdfsPath = org.apache.hadoop.fs.Path(file.path.toUri())
            return FsInput(hdfsPath, fileSystem)
        }

        override fun close() = Unit
    }
}
