package org.radarbase.output.source

class DelegatingStorageIndex(
    private val sourceStorage: SourceStorage,
) : StorageIndex {
    override suspend fun list(dir: StorageNode.StorageDirectory, maxKeys: Int?): List<StorageNode> =
        sourceStorage.list(dir.path, maxKeys = maxKeys)

    override suspend fun remove(file: StorageNode.StorageFile) = Unit
}
