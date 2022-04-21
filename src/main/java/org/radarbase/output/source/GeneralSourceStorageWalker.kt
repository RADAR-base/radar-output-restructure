package org.radarbase.output.source

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.ProducerScope
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.channels.ReceiveChannel
import java.nio.file.Path

class GeneralSourceStorageWalker(
        private val kafkaStorage: SourceStorage
): SourceStorageWalker {
    @ExperimentalCoroutinesApi
    override suspend fun walkRecords(topic: String, topicPath: Path): ReceiveChannel<TopicFile> = coroutineScope {
        produce(capacity = 10_000) {
            produceRecords(topic, topicPath)
        }
    }

    private suspend fun ProducerScope<TopicFile>.produceRecords(topic: String, topicPath: Path) {
        kafkaStorage.list(topicPath).forEach { status ->
            val filename = status.path.fileName.toString()
            when {
                status.isDirectory && filename != "+tmp" -> produceRecords(topic, status.path)
                filename.endsWith(".avro") -> send(kafkaStorage.createTopicFile(topic, status))
                else -> {}
            }
        }
    }

    private suspend fun findTopicPaths(path: Path): List<Path> = coroutineScope {
        val fileStatuses = kafkaStorage.list(path)
        val avroFile = fileStatuses.find { !it.isDirectory && it.path.fileName.toString().endsWith(".avro", true) }

        if (avroFile != null) {
            listOf(avroFile.path.parent.parent)
        } else {
            fileStatuses
                .filter { it.isDirectory && it.path.fileName.toString() != "+tmp" }
                .map { file -> async { findTopicPaths(file.path) } }
                .awaitAll()
                .flatten()
        }
    }

    override suspend fun walkTopics(root: Path, exclude: Set<String>): List<Path> {
        return LinkedHashSet(findTopicPaths(root))
            .filter { it.fileName.toString() !in exclude }
    }
}
