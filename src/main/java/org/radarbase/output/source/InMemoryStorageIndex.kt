package org.radarbase.output.source

import org.radarbase.output.source.StorageIndex.Companion.ROOT
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap

/**
 * Storage index that keeps the given file tree in memory.
 * For very large file systems, this may
 * cause a memory issue.
 */
class InMemoryStorageIndex : MutableStorageIndex {
    private val fileIndex: ConcurrentMap<StorageNode.StorageDirectory, Map<Path, StorageNode>> = ConcurrentHashMap()
    private val rootSet = ConcurrentHashMap<Path, StorageNode>()

    init {
        fileIndex[ROOT] = rootSet
    }

    override suspend fun list(dir: StorageNode.StorageDirectory, maxKeys: Int?): List<StorageNode> {
        val listing = if (dir === ROOT) {
            rootSet
        } else {
            fileIndex[dir] ?: return listOf()
        }

        return if (maxKeys != null) {
            listing.values.take(maxKeys)
        } else {
            listing.values.toList()
        }
    }

    /** Adds a node and all its parents to the file hierarchy. */
    private fun add(dir: StorageNode.StorageDirectory) {
        var currentDir = dir
        var parentDir = currentDir.parent()
        while (parentDir != null) {
            fileIndex.compute(parentDir) { _, map ->
                if (map == null) {
                    mapOf(currentDir.path to currentDir)
                } else {
                    buildMap(map.size + 1) {
                        putAll(map)
                        put(currentDir.path, currentDir)
                    }
                }
            }
            currentDir = parentDir
            parentDir = currentDir.parent()
        }
        rootSet[currentDir.path] = currentDir
    }

    override suspend fun addAll(parent: StorageNode.StorageDirectory, nodes: List<StorageNode>): Collection<StorageNode> {
        add(parent)

        if (nodes.isEmpty()) {
            return fileIndex[parent]?.values
                ?: listOf()
        }

        nodes.asSequence()
            .filterIsInstance<StorageNode.StorageDirectory>()
            .forEach { node ->
                fileIndex.computeIfAbsent(node) {
                    mapOf()
                }
            }
        val newMap = fileIndex.compute(parent) { _, map ->
            if (map == null) {
                buildMap(nodes.size) {
                    nodes.forEach { put(it.path, it) }
                }
            } else {
                buildMap(nodes.size + map.size) {
                    putAll(map)
                    nodes.forEach { put(it.path, it) }
                }
            }
        } ?: mapOf()

        return newMap.values
    }

    override suspend fun sync(parent: StorageNode.StorageDirectory, nodes: List<StorageNode>) {
        add(parent)
        val newMap = buildMap(nodes.size) {
            nodes.forEach { put(it.path, it) }
        }

        fileIndex[parent] = newMap

        nodes.asSequence()
            .filterIsInstance<StorageNode.StorageDirectory>()
            .filter { it.path !in newMap }
            .forEach { removeRecursive(it) }
    }

    override suspend fun remove(file: StorageNode.StorageFile) {
        val parent = file.parent()

        if (parent != null) {
            fileIndex.computeIfPresent(parent) { _, map ->
                (map - file.path).takeIf { it.isNotEmpty() }
            }
        } else {
            rootSet.remove(file.path)
        }
    }

    private fun removeRecursive(node: StorageNode.StorageDirectory) {
        val directoriesToRemove = ArrayDeque<StorageNode.StorageDirectory>()
        fileIndex.remove(node)?.values?.filterIsInstanceTo(directoriesToRemove)
        while (directoriesToRemove.isNotEmpty()) {
            val first = directoriesToRemove.removeFirst()
            fileIndex.remove(first)?.values?.filterIsInstanceTo(directoriesToRemove)
        }
    }
}
