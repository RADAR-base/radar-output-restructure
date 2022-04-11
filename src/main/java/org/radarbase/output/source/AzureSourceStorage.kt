package org.radarbase.output.source

import com.azure.storage.blob.BlobServiceClient
import org.apache.avro.file.SeekableFileInput
import org.apache.avro.file.SeekableInput
import org.radarbase.output.config.AzureConfig
import org.radarbase.output.util.TemporaryDirectory
import org.radarbase.output.util.toKey
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.io.path.createTempFile
import kotlin.io.path.deleteIfExists

class AzureSourceStorage(
        client: BlobServiceClient,
        config: AzureConfig,
        private val tempPath: Path
): SourceStorage {
    private val blobContainerClient = client.getBlobContainerClient(config.container)
    private val readOffsetFromMetadata = config.endOffsetFromMetadata

    private fun blobClient(path: Path) = blobContainerClient.getBlobClient(path.toKey())

    override fun list(path: Path): Sequence<SimpleFileStatus> = blobContainerClient.listBlobsByHierarchy("$path/")
            .asSequence()
            .map { SimpleFileStatus(Paths.get(it.name), it.isPrefix ?: false, it.properties?.lastModified?.toInstant()) }

    override fun createTopicFile(topic: String, status: SimpleFileStatus): TopicFile {
        var topicFile = super.createTopicFile(topic, status)

        if (readOffsetFromMetadata && topicFile.range.range.to == null) {
            try {
                val endOffset = blobClient(topicFile.path).properties.metadata["endOffset"]?.toLongOrNull()

                if (endOffset != null) {
                    topicFile = topicFile.copy(
                            range = topicFile.range.mapRange {
                                it.copy(to = endOffset)
                            })
                }
            } catch (ex: Exception) {
                // skip reading end offset
            }
        }

        return topicFile
    }

    override fun delete(path: Path) {
        blobClient(path).delete()
    }

    override val walker: SourceStorageWalker = GeneralSourceStorageWalker(this)

    override fun createReader(): SourceStorage.SourceStorageReader = AzureSourceStorageReader()

    private inner class AzureSourceStorageReader: SourceStorage.SourceStorageReader {
        private val tempDir = TemporaryDirectory(tempPath, "worker-")

        override fun newInput(file: TopicFile): SeekableInput {
            val fileName = createTempFile(tempDir.path, "${file.topic}-${file.path.fileName}", ".avro")

            blobClient(file.path)
                    .downloadToFile(fileName.toString(), true)

            return object : SeekableFileInput(fileName.toFile()) {
                override fun close() {
                    super.close()
                    fileName.deleteIfExists()
                }
            }
        }

        override fun close() {
            tempDir.close()
        }
    }
}
