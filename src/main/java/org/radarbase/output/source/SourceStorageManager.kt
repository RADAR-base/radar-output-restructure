package org.radarbase.output.source

import org.radarbase.output.config.StorageIndexConfig
import org.radarbase.output.util.AvroFileLister.Companion.avroFileTreeLister
import org.radarbase.output.util.AvroTopicLister.Companion.avroTopicTreeLister
import org.radarbase.output.util.TopicPath
import java.nio.file.Path
import java.time.Instant

class SourceStorageManager(
    val sourceStorage: SourceStorage,
    private val storageIndex: StorageIndex,
    storageIndexConfig: StorageIndexConfig,
) {
    val storageIndexManager: StorageIndexManager = StorageIndexManager(
        storageIndex,
        sourceStorage,
        storageIndexConfig,
    )

    suspend fun delete(path: Path) {
        sourceStorage.delete(path)
        storageIndex.remove(StorageNode.StorageFile(path, Instant.MIN))
    }

    /**
     * Recursively returns all record files in a sequence of a given topic with path.
     * The path must only contain records of a single topic, this is not verified.
     */
    suspend fun listTopicFiles(
        topic: String,
        topicPath: Path,
        limit: Int,
        predicate: (TopicFile) -> Boolean,
    ): List<TopicFile> = storageIndex.avroFileTreeLister(sourceStorage)
        .list(TopicPath(topic, topicPath), limit, predicate)

    /**
     * Recursively find all topic root paths of records in the given path.
     * Exclude paths belonging to the set of given excluded topics.
     */
    suspend fun listTopics(
        exclude: Set<String>,
    ): List<Path> = storageIndex.avroTopicTreeLister()
        .listTo(LinkedHashSet(), StorageIndex.ROOT.path)
        .filter { it.fileName.toString() !in exclude }
}
