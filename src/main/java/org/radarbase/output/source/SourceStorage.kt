package org.radarbase.output.source

import org.apache.avro.file.SeekableInput
import org.radarbase.output.util.SuspendedCloseable
import java.nio.file.Path
import java.time.Instant

/** Source storage type. */
interface SourceStorage {
    val root: Path

    /** Create a reader for the storage medium. It should be closed by the caller. */
    fun createReader(): SourceStorageReader

    /** List all files in the given directory. */
    suspend fun list(
        path: Path,
        startAfter: Path? = null,
        maxKeys: Int? = null,
    ): List<StorageNode>

    /** Delete given file. Will not delete any directories. */
    suspend fun delete(path: Path)
    suspend fun createTopicFile(topic: String, status: StorageNode): TopicFile = TopicFile(
        topic = topic,
        path = status.path,
        lastModified = if (status is StorageNode.StorageFile) status.lastModified else Instant.now(),
    )

    /**
     * File reader for the storage medium.
     * All inputs opened by this reader should be closed before closing the reader itself.
     */
    interface SourceStorageReader : SuspendedCloseable {
        /**
         * Open given file path as a SeekableInput.
         * For remote files, implementing this may download the file to a local cache before
         * opening the input stream. It should be closed by the caller.
         */
        suspend fun newInput(file: TopicFile): SeekableInput
    }
}
