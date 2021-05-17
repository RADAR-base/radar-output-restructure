package org.radarbase.output.source

import io.minio.*
import org.apache.avro.file.SeekableFileInput
import org.apache.avro.file.SeekableInput
import org.eclipse.jetty.util.MultiException
import org.radarbase.output.config.S3Config
import org.radarbase.output.util.TemporaryDirectory
import org.radarbase.output.util.bucketBuild
import org.radarbase.output.util.objectBuild
import org.slf4j.LoggerFactory
import java.io.IOException
import java.nio.file.*

class S3SourceStorage(
        private val s3Client: MinioClient,
        config: S3Config,
        private val tempPath: Path
): SourceStorage {
    override val walker: SourceStorageWalker = GeneralSourceStorageWalker(this)
    private val bucket = config.bucket
    private val readEndOffset = config.endOffsetFromTags

    override fun list(path: Path): Sequence<SimpleFileStatus> {
        val listRequest = ListObjectsArgs.Builder().bucketBuild(bucket) {
            prefix("$path/")
            recursive(false)
            useUrlEncodingType(false)
        }
        return faultTolerant { s3Client.listObjects(listRequest) }
            .asSequence()
            .map {
                val item = it.get()
                SimpleFileStatus(Paths.get(item.objectName()), item.isDir, if (item.isDir) null else item.lastModified().toInstant())
            }
    }

    override fun createTopicFile(topic: String, status: SimpleFileStatus): TopicFile {
        var topicFile = super.createTopicFile(topic, status)

        if (readEndOffset && topicFile.range.range.to == null) {
            try {
                val tags = s3Client.getObjectTags(GetObjectTagsArgs.Builder().objectBuild(bucket, status.path))
                val endOffset = tags.get()["endOffset"]?.toLongOrNull()
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
        val removeRequest = RemoveObjectArgs.Builder().objectBuild(bucket, path)
        faultTolerant { s3Client.removeObject(removeRequest) }
    }

    override fun createReader(): SourceStorage.SourceStorageReader = S3SourceStorageReader()

    private inner class S3SourceStorageReader: SourceStorage.SourceStorageReader {
        private val tempDir = TemporaryDirectory(tempPath, "worker-")

        override fun newInput(file: TopicFile): SeekableInput {
            val tempFile = Files.createTempFile(tempDir.path, "${file.topic}-${file.path.fileName}", ".avro")

            try {
                faultTolerant {
                    Files.newOutputStream(tempFile, StandardOpenOption.TRUNCATE_EXISTING).use { out ->
                        s3Client.getObject(GetObjectArgs.Builder().objectBuild(bucket, file.path))
                            .copyTo(out)
                    }
                }
            } catch (ex: Exception) {
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

        fun <T> faultTolerant(attempt: (Int) -> T): T = tryRepeat(3, attempt)

        fun <T> tryRepeat(numberOfAttempts: Int, attempt: (Int) -> T): T {
            val exceptions = MultiException()
            repeat(numberOfAttempts) { i ->
                try {
                    return attempt(i)
                } catch (ex: Exception) {
                    logger.warn("Temporarily failed to do S3 operation: {}", ex.toString())
                    if (i < numberOfAttempts - 1) {
                        logger.warn("Temporarily failed to do S3 operation: {}, retrying after 1 second.", ex.toString())
                        Thread.sleep(1000L)
                    } else {
                        logger.error("Failed to do S3 operation: {}", ex.toString())
                    }
                    exceptions.add(ex)
                }
            }
            throw exceptions
        }
    }
}
