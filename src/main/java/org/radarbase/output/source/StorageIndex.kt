package org.radarbase.output.source

import java.nio.file.Paths

interface StorageIndex {
    suspend fun list(dir: StorageNode.StorageDirectory, maxKeys: Int? = null): List<StorageNode>

    suspend fun remove(file: StorageNode.StorageFile)

    companion object {
        val ROOT = StorageNode.StorageDirectory(Paths.get("/"))
    }
}
