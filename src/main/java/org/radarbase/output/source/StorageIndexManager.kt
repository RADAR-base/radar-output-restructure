package org.radarbase.output.source

import org.radarbase.kotlin.coroutines.forkJoin
import org.slf4j.LoggerFactory
import java.nio.file.Path
import java.time.Duration
import java.time.Instant

/** Manager to manage a storage index. */
class StorageIndexManager(
    /** Storage index to manage. */
    val storageIndex: StorageIndex,
    /** Source storage to index. */
    private val sourceStorage: SourceStorage,
    /** Root directory in source storage to start scanning. */
    root: Path,
    /** How often to rescan the full directory structure. */
    private val rescanDirectoryDuration: Duration,
    /** How often to rescan empty directories. */
    private val rescanEmptyDuration: Duration,
) {
    private val root = StorageNode.StorageDirectory(root)

    private var nextSync = Instant.MIN

    private var nextEmptySync = Instant.MIN

    /** Update the storage index, taking into account caching times. */
    suspend fun update() {
        if (storageIndex !is MutableStorageIndex) return
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
            val listOperations = storageIndex.updateLevel(root, rescanEmpty)
            logger.debug("Updated source {} with {} list operations...", root, listOperations)
        }
    }

    private suspend fun MutableStorageIndex.updateLevel(node: StorageNode.StorageDirectory, rescanEmpty: Boolean): Long {
        val list = list(node)
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

        val currentOperations = if (lastFile != null) {
            addAll(node, sourceStorage.list(node.path, startAfter = lastFile.path))
            1L
        } else {
            0L
        }

        val listOperations = list(node)
            .filterIsInstance<StorageNode.StorageDirectory>()
            .filterNot { it.path.fileName.toString() == "+tmp" }
            .forkJoin { updateLevel(it, rescanEmpty) }
            .sum()

        return currentOperations + listOperations
    }

    /** Fully synchronize the storage index with the source storage. */
    suspend fun sync() {
        if (storageIndex !is MutableStorageIndex) return
        logger.info("Syncing source {} index...", root)
        val listOperations = storageIndex.syncLevel(root)
        logger.debug("Synced source {} index with {} list operations...", root, listOperations)
        nextSync = Instant.now() + rescanDirectoryDuration
        nextEmptySync = Instant.now() + rescanEmptyDuration
    }

    private suspend fun MutableStorageIndex.syncLevel(node: StorageNode.StorageDirectory): Long {
        sync(node, sourceStorage.list(node.path))

        val listOperations = list(node)
            .filterIsInstance<StorageNode.StorageDirectory>()
            .filterNot { it.path.fileName.toString() == "+tmp" }
            .forkJoin { syncLevel(it) }
            .sum()

        return 1L + listOperations
    }

    companion object {
        private val logger = LoggerFactory.getLogger(StorageIndexManager::class.java)
    }
}
