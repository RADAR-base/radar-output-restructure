package org.radarbase.output.source

import io.minio.*
import io.minio.errors.ErrorResponseException
import io.minio.messages.Tags
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.retryWhen
import kotlinx.coroutines.withContext
import org.apache.avro.file.SeekableFileInput
import org.apache.avro.file.SeekableInput
import org.radarbase.output.config.S3Config
import org.radarbase.output.util.TemporaryDirectory
import org.radarbase.output.util.bucketBuild
import org.radarbase.output.util.objectBuild
import org.slf4j.LoggerFactory
import java.io.FileNotFoundException
import java.io.IOException
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardOpenOption
import kotlin.io.path.createTempFile
import kotlin.io.path.deleteExisting
import kotlin.io.path.deleteIfExists
import kotlin.io.path.outputStream
import kotlin.time.Duration.Companion.seconds

class S3SourceStorage(
    private val s3Client: MinioClient,
    config: S3Config,
    private val tempPath: Path,
) : SourceStorage {
    private val bucket = config.bucket
    private val readEndOffset = config.endOffsetFromTags

    override suspend fun list(
        path: Path,
        maxKeys: Int?,
    ): List<SimpleFileStatus> {
        val listRequest = ListObjectsArgs.Builder().bucketBuild(bucket) {
            if (maxKeys != null) {
                maxKeys(maxKeys.coerceAtMost(1000))
            }
            prefix("$path/")
            recursive(false)
            useUrlEncodingType(false)
        }
        var iterable = faultTolerant { s3Client.listObjects(listRequest) }
        if (maxKeys != null) {
            iterable = iterable.take(maxKeys)
        }
        return iterable
            .map {
                val item = it.get()
                SimpleFileStatus(
                    Paths.get(item.objectName()),
                    item.isDir,
                    if (item.isDir) null else item.lastModified().toInstant()
                )
            }
    }

    override suspend fun createTopicFile(topic: String, status: SimpleFileStatus): TopicFile {
        var topicFile = super.createTopicFile(topic, status)

        if (readEndOffset && topicFile.range.range.to == null) {
            try {
                val tags = getObjectTags(status.path)
                val endOffset = tags.get()["endOffset"]?.toLongOrNull()
                if (endOffset != null) {
                    topicFile = topicFile.copy(
                        range = topicFile.range.mapRange {
                            it.copy(to = endOffset)
                        }
                    )
                }
            } catch (ex: Exception) {
                // skip reading end offset
            }
        }

        return topicFile
    }

    private suspend fun getObjectTags(path: Path): Tags {
        val tagRequest = GetObjectTagsArgs.Builder().objectBuild(bucket, path)
        return faultTolerant { s3Client.getObjectTags(tagRequest) }
    }

    override suspend fun delete(path: Path) {
        val removeRequest = RemoveObjectArgs.Builder().objectBuild(bucket, path)
        faultTolerant { s3Client.removeObject(removeRequest) }
    }

    override fun createReader(): SourceStorage.SourceStorageReader = S3SourceStorageReader()

    private inner class S3SourceStorageReader : SourceStorage.SourceStorageReader {
        private val tempDir = TemporaryDirectory(tempPath, "worker-")

        override suspend fun newInput(file: TopicFile): SeekableInput =
            withContext(Dispatchers.IO) {
                val tempFile = createTempFile(
                    directory = tempDir.path,
                    prefix = "${file.topic}-${file.path.fileName}",
                    suffix = ".avro",
                )
                try {
                    faultTolerant {
                        tempFile.outputStream(StandardOpenOption.TRUNCATE_EXISTING).use { out ->
                            s3Client.getObject(
                                GetObjectArgs.Builder()
                                    .objectBuild(bucket, file.path)
                            ).use { input ->
                                input.copyTo(out)
                            }
                        }
                    }
                } catch (ex: Exception) {
                    try {
                        tempFile.deleteExisting()
                    } catch (ex: IOException) {
                        logger.warn("Failed to delete temporary file {}", tempFile)
                    }
                    throw ex
                }
                object : SeekableFileInput(tempFile.toFile()) {
                    override fun close() {
                        super.close()
                        tempFile.deleteIfExists()
                    }
                }
            }

        override suspend fun closeAndJoin() = withContext(Dispatchers.IO) {
            tempDir.close()
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(S3SourceStorage::class.java)

        suspend fun <T> faultTolerant(action: () -> T): T {
            return flow { emit(action()) }
                .retryWhen { cause, attempt ->
                    if (
                        cause is ErrorResponseException &&
                        cause.errorResponse().code() in arrayOf("NoSuchKey", "ResourceNotFound")
                    ) {
                        throw FileNotFoundException()
                    }
                    if (attempt < 2) {
                        val timeout = attempt + 1
                        logger.warn(
                            "Temporarily failed to do S3 operation: {}, retrying after {} second(s).",
                            cause.toString(),
                            timeout,
                        )
                        delay(timeout.seconds)
                        true
                    } else {
                        logger.error("Failed to do S3 operation: {}", cause.toString())
                        false
                    }
                }
                .flowOn(Dispatchers.IO)
                .first()
        }
    }
}
