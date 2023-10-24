package org.radarbase.output.source

import org.radarbase.kotlin.coroutines.forkJoin
import org.radarbase.output.config.StorageIndexConfig
import org.slf4j.LoggerFactory
import java.nio.file.Path
import kotlin.time.TimeSource.Monotonic.markNow

/** Manager to manage a storage index. */
class StorageIndexManager(
    /** Storage index to manage. */
    val storageIndex: StorageIndex,
    /** Source storage to index. */
    private val sourceStorage: SourceStorage,
    /** Root directory in source storage to start scanning. */
    root: Path,
    config: StorageIndexConfig,
) {
    private val root = StorageNode.StorageDirectory(root)
    private val rescanEmptyDuration = config.emptyDirectorySyncDuration
    private val rescanDirectoryDuration = config.fullSyncDuration

    private var nextSync = markNow()

    private var nextEmptySync = markNow()

    /** Update the storage index, taking into account caching times. */
    suspend fun update() {
        if (storageIndex !is MutableStorageIndex) return
        when {
            nextSync.hasPassedNow() -> {
                sync()
            }
            nextEmptySync.hasPassedNow() -> {
                logger.info("Updating source {} index (including empty directories)...", root)
                nextEmptySync = markNow() + rescanEmptyDuration
                val listOperations = storageIndex.updateLevel(root, true)
                logger.debug("Updated source {} with {} list operations...", root, listOperations)
            }
            else -> {
                logger.info("Updating source {} index (excluding empty directories)...", root)
                val listOperations = storageIndex.updateLevel(root, false)
                logger.debug("Updated source {} with {} list operations...", root, listOperations)
            }
        }
    }

    private suspend fun MutableStorageIndex.updateLevel(node: StorageNode.StorageDirectory, rescanEmpty: Boolean): Long {
        val list = list(node)
        if (list.isEmpty()) {
            return if (rescanEmpty) syncLevel(node) else 0L
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
        val now = markNow()
        nextSync = now + rescanDirectoryDuration
        nextEmptySync = now + rescanEmptyDuration
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
