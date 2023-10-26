package org.radarbase.output.util

import io.minio.BucketArgs
import io.minio.ObjectArgs
import java.nio.file.Path

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
    key: Path,
    configure: T.() -> T = { this },
): S {
    return bucketBuild(bucket) {
        `object`(key.toString())
        configure()
    }
}
