package org.radarbase.output.source

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.flow.Flow
import java.nio.file.Path

interface SourceStorageWalker {
    /**
     * Recursively returns all records in a sequence of a given topic with path.
     * The path must only contain records of a single topic, this is not verified.
     */
    @FlowPreview
    @ExperimentalCoroutinesApi
    suspend fun walkRecords(topic: String, topicPath: Path): ReceiveChannel<TopicFile>

    /**
     * Recursively find all topic root paths of records in the given path.
     * Exclude paths belonging to the set of given excluded topics.
     */
    suspend fun walkTopics(root: Path, exclude: Set<String> = emptySet()): List<Path>
}
