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

package org.radarbase.hdfs.storage

import io.minio.ErrorCode
import io.minio.MinioClient
import io.minio.PutObjectOptions
import io.minio.errors.ErrorResponseException
import org.slf4j.LoggerFactory
import java.io.IOException
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

class S3StorageDriver : StorageDriver {
    private lateinit var bucket: String
    private lateinit var s3Client: MinioClient

    override fun init(properties: Map<String, String>) {
        s3Client = try {
            MinioClient(properties["s3EndpointUrl"], properties["s3AccessToken"], properties["s3SecretKey"])
        } catch (ex: IllegalArgumentException) {
            logger.warn("Invalid S3 configuration", ex)
            throw ex
        }

        bucket = requireNotNull(properties["s3Bucket"]) { "No S3 bucket provided" }

        logger.info("Object storage configured with endpoint {} in bucket {}",
                properties["s3EndpointUrl"],
                properties["s3Bucket"])

        // Check if the bucket already exists.
        val isExist: Boolean = s3Client.bucketExists(bucket)
        if (isExist) {
            println("Bucket $bucket already exists.")
        } else {
            s3Client.makeBucket(bucket)
        }
    }

    override fun status(path: Path): StorageDriver.PathStatus? {
        return try {
            s3Client.statObject(bucket, path.toKey())
                    .let { StorageDriver.PathStatus(it.length()) }
        } catch (ex: ErrorResponseException) {
            if (ex.errorResponse().errorCode() == ErrorCode.NO_SUCH_KEY || ex.errorResponse().errorCode() == ErrorCode.NO_SUCH_OBJECT) {
                null
            } else {
                throw ex
            }
        }
    }

    @Throws(IOException::class)
    override fun newInputStream(path: Path): InputStream = s3Client.getObject(bucket, path.toKey())

    @Throws(IOException::class)
    override fun move(oldPath: Path, newPath: Path) {
        s3Client.copyObject(bucket, newPath.toKey(), null, null, bucket, oldPath.toKey(), null, null)
        delete(oldPath)
    }

    @Throws(IOException::class)
    override fun store(localPath: Path, newPath: Path) {
        s3Client.putObject(bucket, newPath.toKey(), localPath.toAbsolutePath().toString(),
                PutObjectOptions(Files.size(localPath), -1))
        Files.delete(localPath)
    }

    @Throws(IOException::class)
    override fun delete(path: Path) {
        s3Client.removeObject(bucket, path.toKey())
    }

    override fun createDirectories(directory: Path) {
        // noop
    }

    companion object {
        private val logger = LoggerFactory.getLogger(S3StorageDriver::class.java)

        private val rootPath = Paths.get("/")

        private fun Path.toKey(): String {
            return if (this.startsWith(rootPath)) {
                rootPath.relativize(this).toString()
            } else {
                toString()
            }
        }
    }
}
