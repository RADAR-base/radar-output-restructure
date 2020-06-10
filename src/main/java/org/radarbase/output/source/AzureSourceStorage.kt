package org.radarbase.output.source

import com.azure.storage.blob.BlobServiceClient
import org.apache.avro.file.SeekableFileInput
import org.apache.avro.file.SeekableInput
import org.radarbase.output.util.TemporaryDirectory
import org.radarbase.output.util.toKey
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths


class AzureSourceStorage(
        client: BlobServiceClient,
        container: String,
        private val tempPath: Path
): SourceStorage {
    private val blobContainerClient = client.getBlobContainerClient(container)

    override fun list(path: Path): Sequence<SimpleFileStatus> = blobContainerClient.listBlobsByHierarchy("$path/")
            .asSequence()
            .map { SimpleFileStatus(Paths.get(it.name), it.isPrefix, it.properties.lastModified.toInstant()) }

    override fun delete(path: Path) {
        blobContainerClient.getBlobClient(path.toKey())
                .delete()
    }

    override val walker: SourceStorageWalker = GeneralSourceStorageWalker(this)

    override fun createReader(): SourceStorage.SourceStorageReader = AzureSourceStorageReader()

    private inner class AzureSourceStorageReader: SourceStorage.SourceStorageReader {
        private val tempDir = TemporaryDirectory(tempPath, "worker-")

        override fun newInput(file: TopicFile): SeekableInput {
            val fileName = Files.createTempFile(tempDir.path, "${file.topic}-${file.path.fileName}", ".avro")

            blobContainerClient
                    .getBlobClient(file.path.toKey())
                    .downloadToFile(fileName.toString())

            return object : SeekableFileInput(fileName.toFile()) {
                override fun close() {
                    super.close()
                    Files.deleteIfExists(fileName)
                }
            }
        }

        override fun close() {
            tempDir.close()
        }
    }
}
