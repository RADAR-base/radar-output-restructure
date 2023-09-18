package org.radarbase.output.source

import java.nio.file.Path
import java.time.Instant

/**
 * A node in a file tree of the source or target storage.
 */
sealed interface StorageNode {
    /** Path that the node represents.. */
    val path: Path

    /**
     * Parent of the current node, or `null` if the current node is the storage root or topmost
     * level of a relative path.
     */
    fun parent(): StorageDirectory? {
        val parentPath = path.parent
        return if (parentPath != null) {
            StorageDirectory(parentPath)
        } else {
            null
        }
    }

    /** Storage node that represents a directory. */
    data class StorageDirectory(override val path: Path) : StorageNode

    /**
     * Storage node that represents a file.
     */
    data class StorageFile(
        override val path: Path,
        /** Time that the file was last modified. */
        val lastModified: Instant,
    ) : StorageNode
}
