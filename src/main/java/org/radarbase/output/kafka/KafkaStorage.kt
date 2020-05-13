package org.radarbase.output.kafka

import org.apache.avro.file.SeekableInput
import java.io.Closeable
import java.nio.file.Path

interface KafkaStorage {
    fun reader(): KafkaStorageReader
    fun list(path: Path): Sequence<SimpleFileStatus>
    fun delete(path: Path)

    interface KafkaStorageReader: Closeable {
        fun newInput(file: TopicFile): SeekableInput
    }
}
