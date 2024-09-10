package org.radarbase.output.source

/**
 * Delegate all calls directly to the underlying storage. This effectively means that no caching
 * takes place.
 */
class DelegatingStorageIndex(
    private val sourceStorage: SourceStorage,
) : StorageIndex {
    override suspend fun list(dir: StorageNode.StorageDirectory, maxKeys: Int?): List<StorageNode> =
        sourceStorage.list(dir.path, maxKeys = maxKeys)

    override suspend fun remove(file: StorageNode.StorageFile) = Unit
}
