package org.radarbase.output.util

import io.minio.BucketArgs
import io.minio.ObjectArgs
import java.nio.file.Path

fun Path.withoutFirstSegment(): String {
    // remove bucket prefix
    return first().relativize(this).toString()
}

fun Path.splitFirstSegment(): Pair<String, String> {
    val bucketPath = first()
    return Pair(
        bucketPath.toString(),
        bucketPath.relativize(this).toString(),
    )
}

fun Path.firstSegment(): String = first().toString()

inline fun <S : BucketArgs, T : BucketArgs.Builder<out T, out S>> T.bucketBuild(
    bucket: String,
    configure: T.() -> T = { this },
): S {
    bucket(bucket)
    configure()
    return build()
}

inline fun <S : ObjectArgs, T : ObjectArgs.Builder<out T, out S>> T.objectBuild(
    path: Path,
    configure: T.() -> T = { this },
): S {
    val (bucket, key) = path.splitFirstSegment()
    return bucketBuild(bucket) {
        `object`(key)
        configure()
    }
}

inline fun <S : ObjectArgs, T : ObjectArgs.Builder<out T, out S>> T.objectBuild(
    bucket: String,
    key: Path,
    configure: T.() -> T = { this },
): S {
    return bucketBuild(bucket) {
        `object`(key.toString())
        configure()
    }
}
