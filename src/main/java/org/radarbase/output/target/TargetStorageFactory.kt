package org.radarbase.output.target

import org.radarbase.output.config.ResourceConfig
import org.radarbase.output.config.ResourceType

class TargetStorageFactory(private val config: ResourceConfig) {
    fun createTargetStorage(): TargetStorage = when (config.sourceType) {
        ResourceType.S3 -> S3TargetStorage(config.s3!!)
        ResourceType.LOCAL -> LocalTargetStorage(config.local!!)
        ResourceType.AZURE -> AzureTargetStorage(config.azure!!)
        else -> throw IllegalStateException("Cannot create storage driver for ${config.sourceType}")
    }
}
