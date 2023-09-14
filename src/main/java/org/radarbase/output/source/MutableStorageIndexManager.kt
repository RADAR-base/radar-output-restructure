package org.radarbase.output.source

import org.radarbase.kotlin.coroutines.forkJoin
import org.slf4j.LoggerFactory
import java.nio.file.Path
import java.time.Duration
import java.time.Instant

class MutableStorageIndexManager(
    storageIndex: MutableStorageIndex,
    private val sourceStorage: SourceStorage,
    private val rescanDirectoryDuration: Duration,
    private val rescanEmptyDuration: Duration,
    root: Path,
) : StorageIndexManager {
    private val root = StorageNode.StorageDirectory(root)
    private val mutableStorageIndex = storageIndex
    override val storageIndex: StorageIndex = storageIndex

    var nextSync = Instant.MIN
        private set

    var nextEmptySync = Instant.MIN
        private set

    override suspend fun update() {
        if (nextSync < Instant.now()) {
            sync()
        } else {
            val rescanEmpty = nextEmptySync < Instant.now()
            if (rescanEmpty) {
                logger.info("Updating source {} index (including empty directories)...", root)
                nextEmptySync = Instant.now() + rescanEmptyDuration
            } else {
                logger.info("Updating source {} index (excluding empty directories)...", root)
            }
            val listOperations = updateLevel(root, rescanEmpty)
            logger.debug("Updated source {} with {} list operations...", root, listOperations)
        }
    }

    private suspend fun updateLevel(node: StorageNode.StorageDirectory, rescanEmpty: Boolean): Long {
        val list = storageIndex.list(node)
        if (list.isEmpty()) {
            return if (rescanEmpty) {
                syncLevel(node)
            } else {
                0L
            }
        }
        val lastFile = list.asSequence()
            .filterIsInstance<StorageNode.StorageFile>()
            .maxByOrNull { it.path }

        var listOperations = if (lastFile != null) {
            mutableStorageIndex.addAll(node, sourceStorage.list(node.path, startAfter = lastFile.path))
            1L
        } else {
            0L
        }

        listOperations += storageIndex.list(node)
            .filterIsInstance<StorageNode.StorageDirectory>()
            .forkJoin { updateLevel(it, rescanEmpty) }
            .sum()

        return listOperations
    }

    override suspend fun sync() {
        logger.info("Syncing source {} index...", root)
        val listOperations = syncLevel(root)
        logger.debug("Synced source {} index with {} list operations...", root, listOperations)
        nextSync = Instant.now() + rescanDirectoryDuration
        nextEmptySync = Instant.now() + rescanEmptyDuration
    }

    private suspend fun syncLevel(node: StorageNode.StorageDirectory): Long {
        mutableStorageIndex.sync(node, sourceStorage.list(node.path))
        var listOperations = 1L

        listOperations += storageIndex.list(node)
            .filterIsInstance<StorageNode.StorageDirectory>()
            .forkJoin { syncLevel(it) }
            .sum()

        return listOperations
    }

    companion object {
        private val logger = LoggerFactory.getLogger(MutableStorageIndexManager::class.java)
    }
}
