package org.radarbase.output.source

import com.azure.storage.blob.BlobServiceClient
import io.minio.MinioClient
import org.radarbase.output.config.ResourceConfig
import org.radarbase.output.config.ResourceType
import java.nio.file.Path

class SourceStorageFactory(
    private val resourceConfig: ResourceConfig,
    private val tempPath: Path,
) {
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
            S3SourceStorage(minioClient, s3Config, tempPath)
        }
        ResourceType.HDFS -> {
            val storage = Class.forName("org.radarbase.output.source.HdfsSourceStorageFactory")
            val constructor = storage.getDeclaredConstructor(ResourceConfig::class.java, Path::class.java)
            val factory = constructor.newInstance(resourceConfig, tempPath)
            val createSourceStorage = storage.getDeclaredMethod("createSourceStorage")
            createSourceStorage.invoke(factory) as SourceStorage
        }
        ResourceType.AZURE -> {
            val azureClient = requireNotNull(azureSourceClient)
            val azureConfig = requireNotNull(resourceConfig.azure)
            AzureSourceStorage(azureClient, azureConfig, tempPath)
        }
        else -> throw IllegalStateException("Cannot create kafka storage for type ${resourceConfig.sourceType}")
    }
}
