package org.radarbase.output.source

import org.apache.avro.file.SeekableInput
import java.io.Closeable
import java.nio.file.Path

/** Source storage type. */
interface SourceStorage {
    /** Create a reader for the storage medium. It should be closed by the caller. */
    fun reader(): SourceStorageReader
    /** List all files in the given directory. */
    fun list(path: Path): Sequence<SimpleFileStatus>
    /** List all topic paths in the given directory. */
    fun findTopicPaths(path: Path): Sequence<Path> {
        val fileStatuses = list(path)
        val avroFile = fileStatuses.find {  !it.isDirectory && it.path.fileName.toString().endsWith(".avro", true) }
        return if (avroFile != null) {
            sequenceOf(avroFile.path.parent.parent)
        } else {
            fileStatuses.asSequence()
                    .filter { it.isDirectory && it.path.fileName.toString() != "+tmp" }
                    .flatMap { findTopicPaths(it.path) }
        }
    }
    /** Delete given file. Will not delete any directories. */
    fun delete(path: Path)

    /**
     * File reader for the storage medium.
     * All inputs opened by this reader should be closed before closing the reader itself.
     */
    interface SourceStorageReader: Closeable {
        /**
         * Open given file path as a SeekableInput.
         * For remote files, implementing this may download the file to a local cache before
         * opening the input stream. It should be closed by the caller.
         */
        fun newInput(file: TopicFile): SeekableInput
    }
}
