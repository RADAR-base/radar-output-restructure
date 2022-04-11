package org.radarbase.output.config

import io.minio.MinioClient
import io.minio.credentials.IamAwsProvider
import org.radarbase.output.config.RestructureConfig.Companion.copyEnv

data class S3Config(
    /** URL to reach object store at. */
    val endpoint: String,
    /** Access token for writing data with. */
    val accessToken: String?,
    /** Secret key belonging to access token. */
    val secretKey: String?,
    /** Bucket name. */
    val bucket: String,
    /** If no endOffset is in the filename, read it from object tags. */
    val endOffsetFromTags: Boolean = false,
) {
    fun createS3Client(): MinioClient = MinioClient.Builder().apply {
        endpoint(endpoint)
        if (accessToken.isNullOrBlank() || secretKey.isNullOrBlank()) {
            credentialsProvider(IamAwsProvider(null, null))
        } else {
            credentials(accessToken, secretKey)
        }
    }.build()

    fun withEnv(prefix: String): S3Config = this
        .copyEnv("${prefix}S3_ACCESS_TOKEN") { copy(accessToken = it) }
        .copyEnv("${prefix}S3_SECRET_KEY") { copy(secretKey = it) }
        .copyEnv("${prefix}S3_BUCKET") { copy(bucket = it) }
        .copyEnv("${prefix}S3_ENDPOINT") { copy(endpoint = it) }
}
