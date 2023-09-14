package org.radarbase.output.source

interface MutableStorageIndex : StorageIndex {
    suspend fun addAll(parent: StorageNode.StorageDirectory, nodes: List<StorageNode>): Collection<StorageNode>

    suspend fun sync(parent: StorageNode.StorageDirectory, nodes: List<StorageNode>): Collection<StorageNode>
}
