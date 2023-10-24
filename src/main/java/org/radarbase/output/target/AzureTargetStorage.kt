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

import com.azure.storage.blob.BlobClient
import com.azure.storage.blob.BlobContainerClient
import com.azure.storage.blob.BlobServiceClient
import com.azure.storage.blob.models.ListBlobContainersOptions
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.radarbase.kotlin.coroutines.CacheConfig
import org.radarbase.kotlin.coroutines.CachedValue
import org.radarbase.output.config.AzureConfig
import org.slf4j.LoggerFactory
import java.io.IOException
import java.io.InputStream
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import kotlin.io.path.deleteExisting
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.minutes

class AzureTargetStorage(
    private val root: Path,
    private val config: AzureConfig,
) : TargetStorage {
    private lateinit var serviceClient: BlobServiceClient
    private val containerClient: ConcurrentMap<String, CachedValue<BlobContainerClient>> = ConcurrentHashMap()
    private val cacheConfig = CacheConfig(
        refreshDuration = 1.days,
        retryDuration = 1.hours,
        exceptionCacheDuration = 1.minutes,
    )
    private val container = requireNotNull(config.container) { "Missing Azure Blob Storage container setting" }

    init {
        logger.info(
            "Azure Blob storage configured with endpoint {}",
            config.endpoint,
        )
    }

    override suspend fun initialize() {
        serviceClient = try {
            config.createAzureClient()
        } catch (ex: IllegalArgumentException) {
            logger.warn("Invalid S3 configuration", ex)
            throw ex
        }
    }

    private suspend fun client(container: String) =
        containerClient.computeIfAbsent(container) {
            CachedValue(
                cacheConfig,
            ) {
                withContext(Dispatchers.IO) {
                    // Check if the bucket already exists.
                    val listContainer = ListBlobContainersOptions().apply { prefix = container }

                    val isExist: Boolean = serviceClient.listBlobContainers(listContainer, null)
                        .any { it.name == container }

                    if (isExist) {
                        logger.info("Container {} already exists.", container)
                    } else {
                        serviceClient.createBlobContainer(container)
                        logger.info("Container {} was created.", container)
                    }

                    serviceClient.getBlobContainerClient(container)
                }
            }
        }.get()

    override suspend fun status(path: Path): TargetStorage.PathStatus? =
        withContext(Dispatchers.IO) {
            try {
                TargetStorage.PathStatus(
                    blob(path)
                        .getPropertiesWithResponse(null, null, null)
                        .value
                        .blobSize,
                )
            } catch (ex: Exception) {
                null
            }
        }

    @Throws(IOException::class)
    override suspend fun newInputStream(path: Path): InputStream = withContext(Dispatchers.IO) {
        blob(path).openInputStream()
    }

    @Throws(IOException::class)
    override suspend fun move(oldPath: Path, newPath: Path) = withContext(Dispatchers.IO) {
        blob(newPath).copyFromUrl("${config.endpoint}/$oldPath")
        doDelete(oldPath)
    }

    @Throws(IOException::class)
    override suspend fun store(localPath: Path, newPath: Path) = withContext(Dispatchers.IO) {
        blob(newPath).uploadFromFile(localPath.toString(), true)
        localPath.deleteExisting()
    }

    @Throws(IOException::class)
    override suspend fun delete(path: Path) = withContext(Dispatchers.IO) {
        doDelete(path)
    }

    private suspend fun doDelete(path: Path) {
        blob(path).delete()
    }

    override suspend fun createDirectories(directory: Path?) = Unit

    private suspend fun blob(path: Path): BlobClient {
        return client(container).getBlobClient(root.resolve(path).toString())
    }

    companion object {
        private val logger = LoggerFactory.getLogger(AzureTargetStorage::class.java)
    }
}
