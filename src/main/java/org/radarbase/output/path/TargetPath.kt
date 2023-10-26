package org.radarbase.output.path

import java.nio.file.Path
import java.nio.file.Paths

data class TargetPath(
    val target: String,
    val path: Path,
) : Comparable<TargetPath> {

    override fun compareTo(other: TargetPath): Int = comparator.compare(this, other)

    override fun toString(): String = "$target:$path"

    fun navigate(block: (Path) -> Path): TargetPath = copy(path = block(path))

    fun toLocalPath(root: Path): Path = root.resolve(path)

    companion object {
        private val comparator = compareBy(TargetPath::target, TargetPath::path)
    }
}

fun Path.toTargetPath(target: String) = TargetPath(target, this)

fun String.toTargetPath(target: String) = Paths.get(this).toTargetPath(target)
