package org.radarbase.output.source

/** Storage index that may be modified by the storage index manager. */
interface MutableStorageIndex : StorageIndex {
    /**
     * Add a list of storage nodes to the given directory.
     * All values in [nodes] should have [parent] as parent node. No nodes will be removed from the
     * current directory listing, but updated values (e.g. last modified values) will be overridden.
     *
     * @return the current file listing after adding new nodes.
     */
    suspend fun addAll(parent: StorageNode.StorageDirectory, nodes: List<StorageNode>): Collection<StorageNode>

    /**
     * Fully sync a storage node list with the index.
     * All values in [nodes] should have [parent] as parent node. All nodes in the index
     * corresponding to [parent] will be removed from that directory and replaced by the given list.
     */
    suspend fun sync(parent: StorageNode.StorageDirectory, nodes: List<StorageNode>)
}
