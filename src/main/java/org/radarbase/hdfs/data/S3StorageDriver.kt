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

package org.radarbase.hdfs.data

import org.slf4j.LoggerFactory
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.NoSuchKeyException
import java.io.IOException
import java.io.InputStream
import java.net.URI
import java.nio.file.Files
import java.nio.file.Path

class S3StorageDriver : StorageDriver {
    private lateinit var bucket: String
    private lateinit var awsClient: S3Client

    override fun init(properties: Map<String, String>) {
        awsClient = S3Client.builder().also { s3Builder ->
            properties["s3EndpointUrl"]?.let {
                val endpoint = try {
                    URI.create(it)
                } catch (ex: IllegalArgumentException) {
                    logger.warn("Invalid S3 URL", ex)
                    throw ex
                }

                s3Builder.endpointOverride(endpoint)
            }
        }.build()

        bucket = requireNotNull(properties["s3Bucket"]) { "No AWS bucket provided" }
    }

    override fun status(path: Path): StorageDriver.PathStatus? {
        return try {
            awsClient.headObject { it.bucket(bucket).key(path.toString()) }
                    .let { StorageDriver.PathStatus(it.contentLength()) }
        } catch (ex: NoSuchKeyException) {
            null
        }
    }

    @Throws(IOException::class)
    override fun newInputStream(path: Path): InputStream = awsClient.getObject {
        it.bucket(bucket).key(path.toString())
    }

    @Throws(IOException::class)
    override fun move(oldPath: Path, newPath: Path) {
        awsClient.copyObject {
            it.bucket(bucket).key(newPath.toString())
                    .copySource("$bucket/$oldPath")
        }
        delete(oldPath)
    }

    @Throws(IOException::class)
    override fun store(localPath: Path, newPath: Path) {
        awsClient.putObject({ it.bucket(bucket).key(newPath.toString()) }, localPath)
        Files.delete(localPath)
    }

    @Throws(IOException::class)
    override fun delete(path: Path) {
        awsClient.deleteObject { it.bucket(bucket).key(path.toString()) }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(S3StorageDriver::class.java)
    }
}
