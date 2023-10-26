package org.radarbase.output.source

import io.minio.GetObjectTagsArgs
import io.minio.ListObjectsArgs
import io.minio.MinioClient
import io.minio.RemoveObjectArgs
import io.minio.errors.ErrorResponseException
import io.minio.messages.Tags
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.retryWhen
import org.radarbase.output.config.S3Config
import org.radarbase.output.util.bucketBuild
import org.radarbase.output.util.objectBuild
import org.slf4j.LoggerFactory
import java.io.FileNotFoundException
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.io.path.pathString
import kotlin.time.Duration.Companion.seconds

class S3SourceStorage(
    override val baseDir: Path,
    private val s3Client: MinioClient,
    config: S3Config,
    private val tempPath: Path,
) : SourceStorage {
    private val bucket = requireNotNull(config.bucket) { "Source storage requires a bucket name" }
    private val readEndOffset = config.endOffsetFromTags

    override suspend fun list(
        path: Path,
        startAfter: Path?,
        maxKeys: Int?,
    ): List<StorageNode> {
        val listRequest = ListObjectsArgs.Builder().bucketBuild(bucket) {
            if (maxKeys != null) {
                maxKeys(maxKeys.coerceAtMost(1000))
            }
            prefix("${path.toSourcePath()}/")
            recursive(false)
            if (startAfter != null) {
                startAfter(startAfter.pathString)
            }
            useUrlEncodingType(false)
        }
        var iterable = faultTolerant { s3Client.listObjects(listRequest) }
        if (maxKeys != null) {
            iterable = iterable.take(maxKeys)
        }
        val baseDirPrefix = "$baseDir/"
        return iterable
            .map {
                val item = it.get()
                val itemPath = Paths.get(item.objectName().removePrefix(baseDirPrefix))
                if (item.isDir) {
                    StorageNode.StorageDirectory(itemPath)
                } else {
                    StorageNode.StorageFile(itemPath, item.lastModified().toInstant())
                }
            }
    }

    override suspend fun createTopicFile(topic: String, status: StorageNode): TopicFile {
        var topicFile = super.createTopicFile(topic, status)

        if (readEndOffset && topicFile.range.range.to == null) {
            try {
                val tags = getObjectTags(status.path)
                val endOffset = tags.get()["endOffset"]?.toLongOrNull()
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

    private suspend fun getObjectTags(path: Path): Tags {
        val tagRequest = GetObjectTagsArgs.Builder().objectBuild(bucket, path.toSourcePath())
        return faultTolerant { s3Client.getObjectTags(tagRequest) }
    }

    override suspend fun delete(path: Path) {
        val removeRequest = RemoveObjectArgs.Builder().objectBuild(bucket, path.toSourcePath())
        faultTolerant { s3Client.removeObject(removeRequest) }
    }

    override fun createReader(): SourceStorage.SourceStorageReader = S3SourceStorageReader(tempPath, s3Client, bucket)

    override fun toString(): String {
        return "S3SourceStorage(bucket=$bucket, baseDir=$baseDir)"
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
