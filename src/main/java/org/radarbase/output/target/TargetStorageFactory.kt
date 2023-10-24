package org.radarbase.output.target

import org.radarbase.output.config.ResourceConfig
import org.radarbase.output.config.ResourceType

class TargetStorageFactory {
    fun createTargetStorage(defaultName: String, configs: Map<String, ResourceConfig>): TargetStorage =
        CombinedTargetStorage(configs.mapValues { (_, config) -> createTargetStorage(config) }, defaultName)

    private fun createTargetStorage(config: ResourceConfig) = when (config.sourceType) {
        ResourceType.S3 -> S3TargetStorage(config.path, config.s3!!)
        ResourceType.LOCAL -> LocalTargetStorage(config.path, config.local!!)
        ResourceType.AZURE -> AzureTargetStorage(config.path, config.azure!!)
    }
}
