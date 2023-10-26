package org.radarbase.output.source

import java.nio.file.Paths

/**
 * Index of files in a source storage.
 * This index does not modify itself so it needs to be synced by a [StorageIndexManager].
 */
interface StorageIndex {
    /**
     * List given directory.
     * If [maxKeys] is given, no more than that many entries will be returned.
     */
    suspend fun list(dir: StorageNode.StorageDirectory, maxKeys: Int? = null): List<StorageNode>

    /**
     * Remove a file from the index.
     * This will typically be called if the file was removed by the current process.
     */
    suspend fun remove(file: StorageNode.StorageFile)

    companion object {
        /**
         * Root directory. All files that are in the index can be found by traversing the index
         * starting at this root.
         */
        val ROOT = StorageNode.StorageDirectory(Paths.get("."))
    }
}
