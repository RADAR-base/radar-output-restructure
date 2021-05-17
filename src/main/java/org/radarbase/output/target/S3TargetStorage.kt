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
import io.minio.errors.ErrorResponseException
import org.radarbase.output.config.S3Config
import org.radarbase.output.source.S3SourceStorage.Companion.faultTolerant
import org.radarbase.output.util.bucketBuild
import org.radarbase.output.util.objectBuild
import org.slf4j.LoggerFactory
import java.io.FileNotFoundException
import java.io.IOException
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Path

class S3TargetStorage(config: S3Config) : TargetStorage {
    private val bucket: String = config.bucket
    private val s3Client: MinioClient = try {
        config.createS3Client()
    } catch (ex: IllegalArgumentException) {
        logger.warn("Invalid S3 configuration", ex)
        throw ex
    }

    init {
        logger.info("Object storage configured with endpoint {} in bucket {}",
                config.endpoint, config.bucket)

        // Check if the bucket already exists.
        val bucketExistsRequest = BucketExistsArgs.Builder().bucketBuild(bucket)
        val isExist: Boolean = faultTolerant { s3Client.bucketExists(bucketExistsRequest) }
        if (isExist) {
            logger.info("Bucket $bucket already exists.")
        } else {
            val makeBucketRequest = MakeBucketArgs.Builder().bucketBuild(bucket)
            faultTolerant { s3Client.makeBucket(makeBucketRequest) }
            logger.info("Bucket $bucket was created.")
        }
    }

    override fun status(path: Path): TargetStorage.PathStatus? {
        val statRequest = StatObjectArgs.Builder().objectBuild(bucket, path)
        return try {
            faultTolerant {
                s3Client.statObject(statRequest)
                    .let { TargetStorage.PathStatus(it.size()) }
            }
        } catch (ex: FileNotFoundException) {
            null
        }
    }

    @Throws(IOException::class)
    override fun newInputStream(path: Path): InputStream {
        val getRequest = GetObjectArgs.Builder().objectBuild(bucket, path)
        return faultTolerant { s3Client.getObject(getRequest) }
    }

    @Throws(IOException::class)
    override fun move(oldPath: Path, newPath: Path) {
        val copyRequest = CopyObjectArgs.Builder().objectBuild(bucket, newPath) {
            source(CopySource.Builder().objectBuild(bucket, oldPath))
        }
        faultTolerant { s3Client.copyObject(copyRequest) }
        delete(oldPath)
    }

    @Throws(IOException::class)
    override fun store(localPath: Path, newPath: Path) {
        val uploadRequest = UploadObjectArgs.Builder().objectBuild(bucket, newPath) {
            filename(localPath.toAbsolutePath().toString())
        }
        faultTolerant { s3Client.uploadObject(uploadRequest) }
        Files.delete(localPath)
    }

    @Throws(IOException::class)
    override fun delete(path: Path) {
        val removeRequest = RemoveObjectArgs.Builder().objectBuild(bucket, path)
        faultTolerant { s3Client.removeObject(removeRequest) }
    }

    override fun createDirectories(directory: Path) {
        // noop
    }

    companion object {
        private val logger = LoggerFactory.getLogger(S3TargetStorage::class.java)
    }
}
