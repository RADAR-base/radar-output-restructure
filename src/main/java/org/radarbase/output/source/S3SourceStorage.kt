package org.radarbase.output.source

import io.minio.*
import org.apache.avro.file.SeekableFileInput
import org.apache.avro.file.SeekableInput
import org.radarbase.output.util.TemporaryDirectory
import org.radarbase.output.util.bucketBuild
import org.radarbase.output.util.objectBuild
import org.slf4j.LoggerFactory
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

class S3SourceStorage(
        private val s3Client: MinioClient,
        private val bucket: String,
        private val tempPath: Path
): SourceStorage {
    override val walker: SourceStorageWalker = GeneralSourceStorageWalker(this)

    override fun list(path: Path): Sequence<SimpleFileStatus> = s3Client.listObjects(
            ListObjectsArgs.Builder().bucketBuild(bucket) {
                prefix("$path/")
                recursive(false)
                useUrlEncodingType(false)
            })
            .asSequence()
            .map {
                val item = it.get()
                SimpleFileStatus(Paths.get(item.objectName()), item.isDir, if (item.isDir) null else item.lastModified().toInstant())
            }

    override fun createTopicFile(topic: String, status: SimpleFileStatus): TopicFile {
        var topicFile = super.createTopicFile(topic, status)

        if (topicFile.range.range.to == null) {
            val tags = s3Client.getObjectTags(GetObjectTagsArgs.Builder().objectBuild(bucket, status.path))
            val endOffset = tags.get()["endOffset"]?.toLongOrNull()
            if (endOffset != null) {
                topicFile = topicFile.copy(
                        range = topicFile.range.mapRange {
                            it.copy(to = endOffset)
                        })
            }
        }

        return topicFile
    }

    override fun delete(path: Path) {
        s3Client.removeObject(RemoveObjectArgs.Builder().objectBuild(bucket, path))
    }

    override fun createReader(): SourceStorage.SourceStorageReader = S3SourceStorageReader()

    private inner class S3SourceStorageReader: SourceStorage.SourceStorageReader {
        private val tempDir = TemporaryDirectory(tempPath, "worker-")

        override fun newInput(file: TopicFile): SeekableInput {
            val tempFile = Files.createTempFile(tempDir.path, "${file.topic}-${file.path.fileName}", ".avro")

            try {
                Files.newOutputStream(tempFile).use { out ->
                    s3Client.getObject(GetObjectArgs.Builder().objectBuild(bucket, file.path)).copyTo(out)
                }
            } catch (ex: IOException) {
                try {
                    Files.delete(tempFile)
                } catch (ex: IOException) {
                    logger.warn("Failed to delete temporary file {}", tempFile)
                }
                throw ex
            }
            return object : SeekableFileInput(tempFile.toFile()) {
                override fun close() {
                    super.close()
                    Files.deleteIfExists(tempFile)
                }
            }
        }

        override fun close() {
            tempDir.close()
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(S3SourceStorage::class.java)
    }
}
