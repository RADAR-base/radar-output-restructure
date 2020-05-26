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
import org.radarbase.output.config.AzureConfig
import org.radarbase.output.util.toKey
import org.slf4j.LoggerFactory
import java.io.IOException
import java.io.InputStream
import java.lang.Exception
import java.nio.file.Files
import java.nio.file.Path

class AzureTargetStorage(private val config: AzureConfig) : TargetStorage {
    private val container: String = config.container
    private val containerClient: BlobContainerClient

    init {
        val serviceClient = try {
            config.createAzureClient()
        } catch (ex: IllegalArgumentException) {
            logger.warn("Invalid S3 configuration", ex)
            throw ex
        }

        logger.info("Azure Blob storage configured with endpoint {} in container {}",
                config.endpoint, config.container)

        // Check if the bucket already exists.
        val listContainer = ListBlobContainersOptions().apply { prefix = container }
        val isExist: Boolean = serviceClient.listBlobContainers(listContainer, null)
                .any { it.name == container }
        if (isExist) {
            logger.info("Container $container already exists.")
        } else {
            serviceClient.createBlobContainer(container)
            logger.info("Container $container was created.")
        }

        containerClient = serviceClient.getBlobContainerClient(container)
    }

    override fun status(path: Path): TargetStorage.PathStatus? {
        return try {
            TargetStorage.PathStatus(blob(path)
                    .getPropertiesWithResponse(null, null, null)
                    .value
                    .blobSize)
        } catch (ex: Exception) {
            null
        }
    }

    @Throws(IOException::class)
    override fun newInputStream(path: Path): InputStream = blob(path).openInputStream()

    @Throws(IOException::class)
    override fun move(oldPath: Path, newPath: Path) {
        blob(newPath).copyFromUrl("${config.endpoint}/${config.container}/${oldPath.toKey()}")
        delete(oldPath)
    }

    @Throws(IOException::class)
    override fun store(localPath: Path, newPath: Path) {
        blob(newPath).uploadFromFile(localPath.toString(), true)
        Files.delete(localPath)
    }

    @Throws(IOException::class)
    override fun delete(path: Path) {
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
