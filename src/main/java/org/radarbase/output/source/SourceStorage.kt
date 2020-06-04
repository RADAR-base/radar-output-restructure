package org.radarbase.output.source

import org.apache.avro.file.SeekableInput
import java.io.Closeable
import java.nio.file.Path

/** Source storage type. */
interface SourceStorage {
    /** Create a reader for the storage medium. It should be closed by the caller. */
    fun createReader(): SourceStorageReader
    /** List all files in the given directory. */
    fun list(path: Path): Sequence<SimpleFileStatus>
    /** Delete given file. Will not delete any directories. */
    fun delete(path: Path)
    /** Find records and topics. */
    val walker: SourceStorageWalker

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
