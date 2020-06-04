package org.radarbase.output.source

import io.minio.MinioClient
import org.apache.avro.file.SeekableFileInput
import org.apache.avro.file.SeekableInput
import org.radarbase.output.util.TemporaryDirectory
import org.radarbase.output.util.toKey
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

class S3SourceStorage(
        private val s3Client: MinioClient,
        private val bucket: String,
        private val tempPath: Path
): SourceStorage {
    override val walker: SourceStorageWalker = GeneralSourceStorageWalker(this)

    override fun list(path: Path): Sequence<SimpleFileStatus> = s3Client.listObjects(bucket, path.toString())
            .asSequence()
            .map {
                val item = it.get()
                SimpleFileStatus(Paths.get(item.objectName()), item.isDir, item.lastModified().toInstant())
            }

    override fun delete(path: Path) {
        s3Client.removeObject(bucket, path.toKey())
    }

    override fun createReader(): SourceStorage.SourceStorageReader = S3SourceStorageReader()

    private inner class S3SourceStorageReader: SourceStorage.SourceStorageReader {
        private val tempDir = TemporaryDirectory(tempPath, "worker-")

        override fun newInput(file: TopicFile): SeekableInput {
            val fileName = Files.createTempFile(tempDir.path, "${file.topic}-${file.path.fileName}", ".avro")
            s3Client.getObject(bucket, file.path.toKey(), fileName.toString())
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
