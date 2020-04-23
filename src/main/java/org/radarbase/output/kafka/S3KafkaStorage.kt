package org.radarbase.output.kafka

import io.minio.MinioClient
import org.apache.avro.file.SeekableFileInput
import org.apache.avro.file.SeekableInput
import org.radarbase.output.util.TemporaryDirectory
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths


class S3KafkaStorage(
        private val s3Client: MinioClient,
        private val bucket: String,
        private val tempPath: Path
): KafkaStorage {
    override fun list(path: Path): Sequence<SimpleFileStatus> = s3Client.listObjects(bucket, path.toString())
            .asSequence()
            .map {
                val item = it.get()
                SimpleFileStatus(Paths.get(item.objectName()), item.isDir, item.lastModified().toInstant())
            }

    override fun reader(): KafkaStorage.KafkaStorageReader = S3KafkaStorageReader()

    private inner class S3KafkaStorageReader: KafkaStorage.KafkaStorageReader {
        private val tempDir = TemporaryDirectory(tempPath, "worker-")

        override fun newInput(file: TopicFile): SeekableInput {
            val fileName = Files.createTempFile(tempDir.path, "${file.topic}-${file.path.fileName}", ".avro")
            s3Client.getObject(bucket, file.path.toString(), fileName.toString())
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
