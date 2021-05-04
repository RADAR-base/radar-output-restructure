package org.radarbase.output.source

import java.nio.file.Path

class GeneralSourceStorageWalker(
        private val kafkaStorage: SourceStorage
): SourceStorageWalker {
    override fun walkRecords(topic: String, topicPath: Path): Sequence<TopicFile> = kafkaStorage.list(topicPath)
            .flatMap { status ->
                val filename = status.path.fileName.toString()
                when {
                    status.isDirectory && filename != "+tmp" -> walkRecords(topic, status.path)
                    filename.endsWith(".avro") -> sequenceOf(kafkaStorage.createTopicFile(topic, status))
                    else -> emptySequence()
                }
            }

    private fun findTopicPaths(path: Path): Sequence<Path> {
        val fileStatuses = kafkaStorage.list(path).toList()
        val avroFile = fileStatuses.find { !it.isDirectory && it.path.fileName.toString().endsWith(".avro", true) }

        return if (avroFile != null) {
            sequenceOf(avroFile.path.parent.parent)
        } else {
            fileStatuses.asSequence()
                    .filter { it.isDirectory && it.path.fileName.toString() != "+tmp" }
                    .flatMap { findTopicPaths(it.path) }
        }
    }

    override fun walkTopics(root: Path, exclude: Set<String>): Sequence<Path> = findTopicPaths(root)
            .distinct()
            .filter { it.fileName.toString() !in exclude }
}
