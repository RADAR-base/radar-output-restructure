package org.radarbase.output.source

import java.nio.file.Path
import java.time.Instant

sealed interface StorageNode {
    val path: Path

    fun parent(): StorageDirectory? {
        val parentPath = path.parent
        return if (parentPath != null) {
            StorageDirectory(parentPath)
        } else {
            null
        }
    }

    data class StorageDirectory(override val path: Path) : StorageNode

    data class StorageFile(
        override val path: Path,
        val lastModified: Instant,
    ) : StorageNode
}
