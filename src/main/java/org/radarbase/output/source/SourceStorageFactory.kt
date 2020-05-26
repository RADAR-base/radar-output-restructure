package org.radarbase.output.source

import com.azure.storage.blob.BlobServiceClient
import io.minio.MinioClient
import org.apache.hadoop.fs.FileSystem
import org.radarbase.output.config.ResourceConfig
import org.radarbase.output.config.ResourceType
import java.nio.file.Path

class SourceStorageFactory(private val resourceConfig: ResourceConfig, private val tempPath: Path) {
    private val s3SourceClient: MinioClient? = if (resourceConfig.sourceType == ResourceType.S3) {
        requireNotNull(resourceConfig.s3).createS3Client()
    } else null

    private val azureSourceClient: BlobServiceClient? = if (resourceConfig.sourceType == ResourceType.AZURE) {
        requireNotNull(resourceConfig.azure).createAzureClient()
    } else null

    fun createSourceStorage() = when(resourceConfig.sourceType) {
        ResourceType.S3 -> {
            val s3Config = requireNotNull(resourceConfig.s3)
            val minioClient = requireNotNull(s3SourceClient)
            S3SourceStorage(minioClient, s3Config.bucket, tempPath)
        }
        ResourceType.HDFS -> {
            val hdfsConfig = requireNotNull(resourceConfig.hdfs)
            val fileSystem = FileSystem.get(hdfsConfig.configuration)
            HdfsSourceStorage(fileSystem)
        }
        ResourceType.AZURE -> {
            val azureClient = requireNotNull(azureSourceClient)
            val config = requireNotNull(resourceConfig.azure)
            AzureSourceStorage(azureClient, config.container, tempPath)
        }
        else -> throw IllegalStateException("Cannot create kafka storage for type ${resourceConfig.sourceType}")
    }
}
