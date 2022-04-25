package org.radarbase.output.source

import org.apache.avro.file.SeekableInput
import org.radarbase.output.accounting.TopicPartitionOffsetRange
import org.radarbase.output.util.*
import org.radarbase.output.util.AvroFileLister.Companion.avroFileTreeLister
import org.radarbase.output.util.AvroTopicLister.Companion.avroTopicTreeLister
import java.nio.file.Path
import java.time.Instant

/** Source storage type. */
interface SourceStorage {
    /** Create a reader for the storage medium. It should be closed by the caller. */
    fun createReader(): SourceStorageReader
    /** List all files in the given directory. */
    suspend fun list(path: Path): List<SimpleFileStatus>

    /** Delete given file. Will not delete any directories. */
    suspend fun delete(path: Path)
    suspend fun createTopicFile(topic: String, status: SimpleFileStatus): TopicFile {
        val lastModified = status.lastModified ?: Instant.now()
        val range = TopicPartitionOffsetRange.parseFilename(status.path.fileName.toString(), lastModified)
        return TopicFile(topic, status.path, lastModified, range)
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
    ): List<TopicFile> = avroFileTreeLister()
        .list(TopicPath(topic, topicPath), limit, predicate)

    /**
     * Recursively find all topic root paths of records in the given path.
     * Exclude paths belonging to the set of given excluded topics.
     */
    suspend fun listTopics(
        root: Path,
        exclude: Set<String>,
    ): List<Path> = avroTopicTreeLister()
        .listTo(LinkedHashSet(), root)
        .filter { it.fileName.toString() !in exclude }

    /**
     * File reader for the storage medium.
     * All inputs opened by this reader should be closed before closing the reader itself.
     */
    interface SourceStorageReader: SuspendedCloseable {
        /**
         * Open given file path as a SeekableInput.
         * For remote files, implementing this may download the file to a local cache before
         * opening the input stream. It should be closed by the caller.
         */
        suspend fun newInput(file: TopicFile): SeekableInput
    }
}
