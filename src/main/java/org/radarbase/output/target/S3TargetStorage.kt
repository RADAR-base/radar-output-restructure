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

import io.minio.*
import org.radarbase.kotlin.coroutines.CacheConfig
import org.radarbase.kotlin.coroutines.CachedValue
import org.radarbase.output.config.S3Config
import org.radarbase.output.source.S3SourceStorage.Companion.faultTolerant
import org.radarbase.output.util.bucketBuild
import org.radarbase.output.util.objectBuild
import org.radarbase.output.util.firstSegment
import org.slf4j.LoggerFactory
import java.io.FileNotFoundException
import java.io.IOException
import java.io.InputStream
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap
import kotlin.io.path.deleteExisting
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.minutes

class S3TargetStorage(
    config: S3Config,
) : TargetStorage {
    private val s3Client: MinioClient = try {
        config.createS3Client()
    } catch (ex: IllegalArgumentException) {
        logger.warn("Invalid S3 configuration", ex)
        throw ex
    }

    private val buckets = ConcurrentHashMap<String, CachedValue<Unit>>()
    private val cacheConfig = CacheConfig(
        refreshDuration = 1.days,
        retryDuration = 1.hours,
        exceptionCacheDuration = 1.minutes,
    )

    init {
        logger.info(
            "Object storage configured with endpoint {}",
            config.endpoint,
        )
    }

    override suspend fun initialize() {}

    override suspend fun status(path: Path): TargetStorage.PathStatus? {
        val statRequest = StatObjectArgs.builder().objectBuild(path)
            .also { it.ensureBucket() }
        return try {
            faultTolerant {
                s3Client.statObject(statRequest)
                    .let { TargetStorage.PathStatus(it.size()) }
            }
        } catch (ex: FileNotFoundException) {
            null
        }
    }

    private suspend fun BucketArgs.ensureBucket() = ensureBucket(bucket())

    private suspend fun ensureBucket(bucket: String) {
        try {
            buckets.computeIfAbsent(bucket) {
                CachedValue(cacheConfig) {
                    val bucketExistsRequest = BucketExistsArgs.builder().bucketBuild(bucket)
                    val isExist: Boolean = faultTolerant { s3Client.bucketExists(bucketExistsRequest) }
                    if (isExist) {
                        logger.info("Bucket $bucket already exists.")
                    } else {
                        val makeBucketRequest = MakeBucketArgs.builder().bucketBuild(bucket)
                        faultTolerant { s3Client.makeBucket(makeBucketRequest) }
                        logger.info("Bucket $bucket was created.")
                    }
                }
            }.get()
        } catch (ex: Exception) {
            logger.error(
                "Failed to create bucket {}: {}",
                bucket,
                ex.message
            )
            throw ex
        }
    }

    @Throws(IOException::class)
    override suspend fun newInputStream(path: Path): InputStream {
        val getRequest = GetObjectArgs.builder().objectBuild(path)
            .also { it.ensureBucket() }
        return faultTolerant { s3Client.getObject(getRequest) }
    }

    @Throws(IOException::class)
    override suspend fun move(oldPath: Path, newPath: Path) {
        val copyRequest = CopyObjectArgs.builder().objectBuild(newPath) {
            source(CopySource.Builder().objectBuild(oldPath))
        }
        faultTolerant { s3Client.copyObject(copyRequest) }
        delete(oldPath)
    }

    @Throws(IOException::class)
    override suspend fun store(localPath: Path, newPath: Path) {
        val uploadRequest = UploadObjectArgs.builder().objectBuild(newPath) {
            filename(localPath.toAbsolutePath().toString())
        }
            .also { it.ensureBucket() }

        faultTolerant { s3Client.uploadObject(uploadRequest) }
        localPath.deleteExisting()
    }

    @Throws(IOException::class)
    override suspend fun delete(path: Path) {
        val removeRequest = RemoveObjectArgs.builder().objectBuild(path)
            .also { it.ensureBucket() }
        faultTolerant { s3Client.removeObject(removeRequest) }
    }

    override suspend fun createDirectories(directory: Path) {
        ensureBucket(directory.firstSegment())
    }

    companion object {
        private val logger = LoggerFactory.getLogger(S3TargetStorage::class.java)
    }
}
