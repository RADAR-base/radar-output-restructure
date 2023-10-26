package org.radarbase.output.source

import com.azure.storage.blob.BlobServiceClient
import org.radarbase.output.config.ResourceConfig
import org.radarbase.output.config.ResourceType
import java.nio.file.Path

class SourceStorageFactory(
    private val tempPath: Path,
) {
    fun createSourceStorage(consolidatedSources: List<ResourceConfig>) = consolidatedSources.map { createSourceStorage(it) }

    fun createSourceStorage(sourceConfig: ResourceConfig) = when (sourceConfig.sourceType) {
        ResourceType.S3 -> {
            val s3Config =
                requireNotNull(sourceConfig.s3) { "Missing S3 configuration for source storage" }
            val minioClient = s3Config.createS3Client()
            S3SourceStorage(sourceConfig.path, minioClient, s3Config, tempPath)
        }
        ResourceType.AZURE -> {
            val azureConfig = requireNotNull(sourceConfig.azure) { "Missing Azure configuration for source storage" }
            val azureSourceClient: BlobServiceClient = azureConfig.createAzureClient()

            AzureSourceStorage(sourceConfig.path, azureSourceClient, azureConfig, tempPath)
        }
        else -> throw IllegalStateException("Cannot create kafka storage for type ${sourceConfig.sourceType}")
    }
}
