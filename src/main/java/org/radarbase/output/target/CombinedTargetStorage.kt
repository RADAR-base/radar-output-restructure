package org.radarbase.output.target

import kotlinx.coroutines.coroutineScope
import org.radarbase.kotlin.coroutines.launchJoin
import org.radarbase.output.target.TargetStorage.PathStatus
import java.io.InputStream
import java.nio.file.Path

class CombinedTargetStorage(
    private val delegates: Map<String, TargetStorage>,
    defaultName: String? = null,
) : TargetStorage {
    private val defaultDelegate = if (defaultName != null) {
        requireNotNull(delegates[defaultName]) { "Default target storage $defaultName not found in ${delegates.keys}" }
    } else {
        require(delegates.size == 1) { "Must provide a default taret storage if more than one target storage is defined: ${delegates.keys}" }
        delegates.values.first()
    }

    override fun allowsPrefix(prefix: String): Boolean = prefix in delegates

    override suspend fun initialize() = coroutineScope {
        delegates.values.launchJoin { it.initialize() }
    }

    override suspend fun status(path: Path): PathStatus? = withDelegate(path) { status(it) }

    override suspend fun newInputStream(path: Path): InputStream = withDelegate(path) { newInputStream(it) }

    override suspend fun move(oldPath: Path, newPath: Path) {
        val (oldDelegate, oldDelegatePath) = delegate(oldPath)
        val (newDelegate, newDelegatePath) = delegate(newPath)

        require(oldDelegate == newDelegate) { "Cannot move files between storage systems ($oldPath to $newPath)" }
        return oldDelegate.move(oldDelegatePath, newDelegatePath)
    }

    override suspend fun store(localPath: Path, newPath: Path) = withDelegate(newPath) {
        store(localPath, it)
    }

    override suspend fun delete(path: Path) = withDelegate(path) { delete(it) }

    override suspend fun createDirectories(directory: Path?) {
        if (directory != null) {
            val delegateName = directory.firstOrNull() ?: return
            val delegate = delegates[delegateName.toString()] ?: return

            if (directory.count() == 1) {
                delegate.createDirectories(null)
            } else {
                delegate.createDirectories(delegateName.relativize(directory))
            }
        }
    }

    private fun delegate(path: Path): Pair<TargetStorage, Path> {
        val targetName = requireNotNull(path.firstOrNull()) { "Target storage not found in path '$this'" }
        val delegate = delegates[targetName.toString()] ?: defaultDelegate
        val delegatePath = try {
            targetName.relativize(path)
        } catch (ex: IllegalArgumentException) {
            throw IllegalArgumentException("Failed to split path $path into a relative path", ex)
        }
        return Pair(delegate, delegatePath)
    }

    private inline fun <T> withDelegate(path: Path, block: TargetStorage.(Path) -> T): T {
        val (delegate, delegatePath) = delegate(path)
        return delegate.block(delegatePath)
    }
}
