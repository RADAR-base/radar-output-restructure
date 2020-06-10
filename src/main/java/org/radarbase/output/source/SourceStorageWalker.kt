package org.radarbase.output.source

import java.nio.file.Path

interface SourceStorageWalker {
    /**
     * Recursively returns all records in a sequence of a given topic with path.
     * The path must only contain records of a single topic, this is not verified.
     */
    fun walkRecords(topic: String, topicPath: Path): Sequence<TopicFile>

    /**
     * Recursively find all topic root paths of records in the given path.
     * Exclude paths belonging to the set of given excluded topics.
     */
    fun walkTopics(root: Path, exclude: Set<String> = emptySet()): Sequence<Path>
}
