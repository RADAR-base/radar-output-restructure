package org.radarbase.output.source

import com.azure.storage.blob.BlobServiceClient
import com.azure.storage.blob.models.BlobItem
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.apache.avro.file.SeekableFileInput
import org.apache.avro.file.SeekableInput
import org.radarbase.output.config.AzureConfig
import org.radarbase.output.util.TemporaryDirectory
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Instant
import kotlin.io.path.createTempFile
import kotlin.io.path.deleteIfExists

class AzureSourceStorage(
    override val baseDir: Path,
    client: BlobServiceClient,
    config: AzureConfig,
    private val tempPath: Path,
) : SourceStorage {
    private val blobContainerClient = client.getBlobContainerClient(config.container)
    private val readOffsetFromMetadata = config.endOffsetFromMetadata

    private fun blobClient(path: Path) = blobContainerClient.getBlobClient(path.toSourcePath().toString())

    override suspend fun list(path: Path, startAfter: Path?, maxKeys: Int?): List<StorageNode> =
        withContext(Dispatchers.IO) {
            var iterable: Iterable<BlobItem> = blobContainerClient.listBlobsByHierarchy("${path.toSourcePath()}/")
            if (startAfter != null) {
                iterable = iterable.filter { Paths.get(it.name) > startAfter }
            }
            if (maxKeys != null) {
                iterable = iterable.take(maxKeys)
            }
            val baseDirPrefix = "$baseDir/"
            iterable.map { item ->
                val itemPath = Paths.get(item.name.removePrefix(baseDirPrefix))
                if (item.isPrefix == true) {
                    StorageNode.StorageFile(
                        itemPath,
                        item.properties?.lastModified?.toInstant() ?: Instant.now(),
                    )
                } else {
                    StorageNode.StorageDirectory(itemPath)
                }
            }
        }

    override suspend fun createTopicFile(topic: String, status: StorageNode): TopicFile {
        var topicFile = super.createTopicFile(topic, status)

        if (readOffsetFromMetadata && topicFile.range.range.to == null) {
            try {
                val endOffset = withContext(Dispatchers.IO) {
                    blobClient(topicFile.path).properties
                }.metadata["endOffset"]?.toLongOrNull()

                if (endOffset != null) {
                    topicFile = topicFile.copy(
                        range = topicFile.range.mapRange {
                            it.copy(to = endOffset)
                        },
                    )
                }
            } catch (ex: Exception) {
                // skip reading end offset
            }
        }

        return topicFile
    }

    override suspend fun delete(path: Path) = withContext(Dispatchers.IO) {
        blobClient(path).delete()
    }

    override fun createReader(): SourceStorage.SourceStorageReader = AzureSourceStorageReader()

    override fun toString(): String =
        "AzureSourceStorage(container=${blobContainerClient.blobContainerName}, baseDir=$baseDir)"

    private inner class AzureSourceStorageReader : SourceStorage.SourceStorageReader {
        private val tempDir = TemporaryDirectory(tempPath, "worker-")

        override suspend fun newInput(file: TopicFile): SeekableInput =
            withContext(Dispatchers.IO) {
                val fileName =
                    createTempFile(tempDir.path, "${file.topic}-${file.path.fileName}", ".avro")

                blobClient(file.path)
                    .downloadToFile(fileName.toString(), true)

                object : SeekableFileInput(fileName.toFile()) {
                    override fun close() {
                        super.close()
                        fileName.deleteIfExists()
                    }
                }
            }

        override suspend fun closeAndJoin() = withContext(Dispatchers.IO) {
            tempDir.close()
        }
    }
}
