package org.radarbase.output.source

import org.radarbase.output.source.StorageIndex.Companion.ROOT
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap

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

    private fun add(node: StorageNode) {
        var currentNode = node
        var parent = currentNode.parent()
        if (currentNode is StorageNode.StorageDirectory) {
            fileIndex.computeIfAbsent(currentNode) {
                mapOf()
            }
        }
        while (parent != null) {
            fileIndex.compute(parent) { _, map ->
                if (map == null) {
                    mapOf(currentNode.path to currentNode)
                } else {
                    val newMap = map.toMutableMap()
                    newMap[currentNode.path] = currentNode
                    newMap
                }
            }
            currentNode = parent
            parent = currentNode.parent()
        }
        rootSet[currentNode.path] = currentNode
    }

    override suspend fun addAll(parent: StorageNode.StorageDirectory, nodes: List<StorageNode>): Collection<StorageNode> {
        add(parent)
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

    override suspend fun sync(parent: StorageNode.StorageDirectory, nodes: List<StorageNode>): Collection<StorageNode> {
        add(parent)
        val newMap = fileIndex.compute(parent) { _, map ->
            if (map == null) {
                buildMap(nodes.size) {
                    nodes.forEach { put(it.path, it) }
                }
            } else {
                buildMap(nodes.size) {
                    nodes.forEach { put(it.path, it) }
                }
            }
        } ?: mapOf()

        nodes.asSequence()
            .filterIsInstance<StorageNode.StorageDirectory>()
            .filter { it.path !in newMap }
            .forEach { removeRecursive(it) }

        return newMap.values
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
