package org.radarbase.output.config

import com.fasterxml.jackson.annotation.JsonIgnore
import org.radarbase.output.config.ResourceType.Companion.toResourceType
import org.radarbase.output.config.RestructureConfig.Companion.copyOnChange
import java.nio.file.Path
import java.nio.file.Paths

data class ResourceConfig(
    /** Resource type. One of s3, azure or local. */
    val type: String,
    val path: Path = Paths.get("/"),
    val s3: S3Config? = null,
    val local: LocalConfig? = null,
    val azure: AzureConfig? = null,
    val index: StorageIndexConfig = StorageIndexConfig(),
) {
    @get:JsonIgnore
    val sourceType: ResourceType by lazy {
        requireNotNull(type.toResourceType()) { "Unknown resource type $type, choose s3, azure or local" }
    }

    @get:JsonIgnore
    val name: String?
        get() = when (sourceType) {
            ResourceType.S3 -> checkNotNull(s3) { "No S3 configuration provided." }.bucket
            ResourceType.AZURE -> checkNotNull(azure) { "No Azure configuration provided." }.container
            else -> null
        }

    fun validate() {
        when (sourceType) {
            ResourceType.S3 -> checkNotNull(s3) { "No S3 configuration provided." }
            ResourceType.LOCAL -> checkNotNull(local) { "No local configuration provided." }
            ResourceType.AZURE -> checkNotNull(azure) { "No Azure configuration provided." }
        }
    }

    fun withEnv(prefix: String): ResourceConfig = when (sourceType) {
        ResourceType.S3 -> copyOnChange(s3, { it?.withEnv(prefix) }) { copy(s3 = it) }
        ResourceType.LOCAL -> this
        ResourceType.AZURE -> copyOnChange(azure, { it?.withEnv(prefix) }) { copy(azure = it) }
    }
}
