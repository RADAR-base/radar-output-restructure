package org.radarbase.output.source

import io.minio.GetObjectArgs
import io.minio.MinioClient
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.apache.avro.file.SeekableFileInput
import org.apache.avro.file.SeekableInput
import org.radarbase.output.source.S3SourceStorage.Companion.faultTolerant
import org.radarbase.output.util.TemporaryDirectory
import org.radarbase.output.util.objectBuild
import org.slf4j.LoggerFactory
import java.io.IOException
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import kotlin.io.path.deleteExisting
import kotlin.io.path.deleteIfExists
import kotlin.io.path.outputStream

internal class S3SourceStorageReader(
    tempPath: Path,
    private val s3Client: MinioClient,
    private val bucket: String,
) : SourceStorage.SourceStorageReader {
    private val tempDir = TemporaryDirectory(tempPath, "worker-")

    override suspend fun newInput(file: TopicFile): SeekableInput = withContext(Dispatchers.IO) {
        val tempFile = kotlin.io.path.createTempFile(
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

    companion object {
        private val logger = LoggerFactory.getLogger(S3SourceStorageReader::class.java)
    }
}
