/*
 * Copyright 2018 The Hyve
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.radarbase.output.target

import io.minio.BucketExistsArgs
import io.minio.CopyObjectArgs
import io.minio.CopySource
import io.minio.GetObjectArgs
import io.minio.MakeBucketArgs
import io.minio.MinioClient
import io.minio.RemoveObjectArgs
import io.minio.StatObjectArgs
import io.minio.UploadObjectArgs
import io.minio.errors.ErrorResponseException
import org.radarbase.output.config.S3Config
import org.radarbase.output.source.S3SourceStorage.Companion.faultTolerant
import org.radarbase.output.util.bucketBuild
import org.radarbase.output.util.objectBuild
import org.slf4j.LoggerFactory
import java.io.FileNotFoundException
import java.io.IOException
import java.io.InputStream
import java.nio.file.Path
import kotlin.io.path.deleteExisting

class S3TargetStorage(
    override val baseDir: Path,
    config: S3Config,
) : TargetStorage {
    private val s3Client: MinioClient = try {
        config.createS3Client()
    } catch (ex: IllegalArgumentException) {
        logger.warn("Invalid S3 configuration", ex)
        throw ex
    }

    private val bucket = requireNotNull(config.bucket) { "Missing bucket configuration" }

    init {
        logger.info(
            "Object storage configured with endpoint {}",
            config.endpoint,
        )
    }

    override suspend fun initialize() {
        ensureBucket()
    }

    override suspend fun status(path: Path): TargetStorage.PathStatus? {
        val statRequest = StatObjectArgs.builder().objectBuild(bucket, path.withRoot())
        return try {
            faultTolerant {
                s3Client.statObject(statRequest)
                    .let { TargetStorage.PathStatus(it.size()) }
            }
        } catch (ex: FileNotFoundException) {
            null
        }
    }

    private suspend fun ensureBucket() {
        try {
            val bucketExistsRequest = BucketExistsArgs.builder().bucketBuild(bucket)
            val isExist: Boolean = faultTolerant { s3Client.bucketExists(bucketExistsRequest) }
            if (isExist) {
                logger.info("Bucket $bucket already exists.")
            } else {
                val makeBucketRequest = MakeBucketArgs.builder().bucketBuild(bucket)
                faultTolerant {
                    try {
                        s3Client.makeBucket(makeBucketRequest)
                    } catch (ex: ErrorResponseException) {
                        if (ex.errorResponse().code() == "BucketAlreadyOwnedByYou") {
                            logger.warn("Bucket {} was already created while the request was busy", bucket)
                        } else {
                            throw ex
                        }
                    }
                }
                logger.info("Bucket $bucket was created.")
            }
        } catch (ex: Exception) {
            logger.error(
                "Failed to create bucket {}: {}",
                bucket,
                ex.message,
            )
            throw ex
        }
    }

    private fun Path.withRoot(): Path = this@S3TargetStorage.baseDir.resolve(this)

    @Throws(IOException::class)
    override suspend fun newInputStream(path: Path): InputStream {
        val getRequest = GetObjectArgs.builder().objectBuild(bucket, path.withRoot())
        return faultTolerant { s3Client.getObject(getRequest) }
    }

    @Throws(IOException::class)
    override suspend fun move(oldPath: Path, newPath: Path) {
        val copyRequest = CopyObjectArgs.builder().objectBuild(bucket, newPath.withRoot()) {
            source(CopySource.Builder().objectBuild(bucket, oldPath.withRoot()))
        }
        faultTolerant { s3Client.copyObject(copyRequest) }
        delete(oldPath)
    }

    @Throws(IOException::class)
    override suspend fun store(localPath: Path, newPath: Path) {
        val uploadRequest = UploadObjectArgs.builder().objectBuild(bucket, newPath.withRoot()) {
            filename(localPath.toAbsolutePath().toString())
        }

        faultTolerant { s3Client.uploadObject(uploadRequest) }
        localPath.deleteExisting()
    }

    @Throws(IOException::class)
    override suspend fun delete(path: Path) {
        val removeRequest = RemoveObjectArgs.builder().objectBuild(bucket, path.withRoot())
        faultTolerant { s3Client.removeObject(removeRequest) }
    }

    override suspend fun createDirectories(directory: Path) = Unit

    override fun toString(): String = "S3TargetStorage(baseDir=$baseDir, bucket='$bucket')"

    companion object {
        private val logger = LoggerFactory.getLogger(S3TargetStorage::class.java)
    }
}
