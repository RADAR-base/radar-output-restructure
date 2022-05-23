package org.radarbase.output.config

import com.azure.core.credential.BasicAuthenticationCredential
import com.azure.core.util.HttpClientOptions
import com.azure.storage.blob.BlobServiceClient
import com.azure.storage.blob.BlobServiceClientBuilder
import com.azure.storage.common.StorageSharedKeyCredential
import org.radarbase.output.config.RestructureConfig.Companion.copyEnv
import org.slf4j.LoggerFactory
import java.time.Duration

data class AzureConfig(
    /** URL to reach object store at. */
    val endpoint: String,
    /** Name of the Azure Blob Storage container. */
    val container: String,
    /** If no endOffset is in the filename, read it from object metadata. */
    val endOffsetFromMetadata: Boolean = false,
    /** Azure username. */
    val username: String?,
    /** Azure password. */
    val password: String?,
    /** Shared Azure Blob Storage account name. */
    val accountName: String?,
    /** Shared Azure Blob Storage account key. */
    val accountKey: String?,
    /** Azure SAS token for a configured service. */
    val sasToken: String?,
    /** HTTP connect timeout. */
    val connectTimeout: Long? = null,
    /** HTTP response timeout. */
    val responseTimeout: Long? = null,
    /** HTTP write timeout. */
    val writeTimeout: Long? = null,
    /** HTTP read timeout. */
    val readTimeout: Long? = null,
    ) {
    fun createAzureClient(): BlobServiceClient = BlobServiceClientBuilder().apply {
        endpoint(endpoint)
        when {
            !username.isNullOrEmpty() && !password.isNullOrEmpty() -> credential(
                BasicAuthenticationCredential(username, password))
            !accountName.isNullOrEmpty() && !accountKey.isNullOrEmpty() -> credential(
                StorageSharedKeyCredential(accountName, accountKey))
            !sasToken.isNullOrEmpty() -> sasToken(sasToken)
            else -> logger.warn("No Azure credentials supplied. Assuming a public blob storage.")
        }
        clientOptions(HttpClientOptions().apply {
            connectTimeout = this@AzureConfig.connectTimeout.toDurationOrNull()
            responseTimeout = this@AzureConfig.responseTimeout.toDurationOrNull()
            writeTimeout = this@AzureConfig.writeTimeout.toDurationOrNull()
            readTimeout = this@AzureConfig.readTimeout.toDurationOrNull()
        })
    }.buildClient()

    fun withEnv(prefix: String): AzureConfig = this
        .copyEnv("${prefix}AZURE_USERNAME") { copy(username = it) }
        .copyEnv("${prefix}AZURE_PASSWORD") { copy(password = it) }
        .copyEnv("${prefix}AZURE_ACCOUNT_NAME") { copy(accountName = it) }
        .copyEnv("${prefix}AZURE_ACCOUNT_KEY") { copy(accountKey = it) }
        .copyEnv("${prefix}AZURE_SAS_TOKEN") { copy(sasToken = it) }

    companion object {
        private val logger = LoggerFactory.getLogger(AzureConfig::class.java)
        private fun Long?.toDurationOrNull(): Duration? = this
            ?.takeIf { it > 0 }
            ?.let { Duration.ofSeconds(it) }
    }
}
