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
import com.azure.storage.blob.models.ListBlobContainersOptions
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.radarbase.output.config.AzureConfig
import org.radarbase.output.util.toKey
import org.slf4j.LoggerFactory
import java.io.IOException
import java.io.InputStream
import java.nio.file.Path
import kotlin.io.path.deleteExisting

class AzureTargetStorage(private val config: AzureConfig) : TargetStorage {
    private val container: String = config.container
    private lateinit var containerClient: BlobContainerClient

    init {
        logger.info("Azure Blob storage configured with endpoint {} in container {}",
            config.endpoint, config.container)
    }

    override suspend fun initialize() {
        val serviceClient = try {
            config.createAzureClient()
        } catch (ex: IllegalArgumentException) {
            logger.warn("Invalid S3 configuration", ex)
            throw ex
        }

        // Check if the bucket already exists.
        val listContainer = ListBlobContainersOptions().apply { prefix = container }

        val isExist: Boolean = withContext(Dispatchers.IO) {
            serviceClient.listBlobContainers(listContainer, null)
        }.any { it.name == container }

        if (isExist) {
            logger.info("Container $container already exists.")
        } else {
            withContext(Dispatchers.IO) {
                serviceClient.createBlobContainer(container)
            }
            logger.info("Container $container was created.")
        }

        containerClient = serviceClient.getBlobContainerClient(container)
    }

    override suspend fun status(path: Path): TargetStorage.PathStatus? =
        withContext(Dispatchers.IO) {
            try {
                TargetStorage.PathStatus(blob(path)
                    .getPropertiesWithResponse(null, null, null)
                    .value
                    .blobSize)
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
        blob(newPath).copyFromUrl("${config.endpoint}/${config.container}/${oldPath.toKey()}")
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

    private fun doDelete(path: Path) {
        blob(path).delete()
    }

    override fun createDirectories(directory: Path) {
        // noop
    }

    private fun blob(path: Path): BlobClient = containerClient.getBlobClient(path.toKey())

    companion object {
        private val logger = LoggerFactory.getLogger(AzureTargetStorage::class.java)
    }
}
