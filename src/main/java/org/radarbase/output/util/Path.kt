package org.radarbase.output.util

import io.minio.BucketArgs
import io.minio.ObjectArgs
import java.nio.file.Path
import java.nio.file.Paths

private val rootPath = Paths.get("/")

fun Path.toKey() = if (startsWith(rootPath)) {
    rootPath.relativize(this).toString()
} else toString()

inline fun <S : BucketArgs, T : BucketArgs.Builder<out T, out S>> T.bucketBuild(
    bucket: String,
    configure: T.() -> T = { this },
): S {
    bucket(bucket)
    configure()
    return build()
}

inline fun <S : ObjectArgs, T : ObjectArgs.Builder<out T, out S>> T.objectBuild(
    bucket: String,
    path: Path,
    configure: T.() -> T = { this },
): S = bucketBuild(bucket) {
    `object`(path.toKey())
    configure()
}
