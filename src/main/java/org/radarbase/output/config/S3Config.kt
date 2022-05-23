package org.radarbase.output.config

import io.minio.MinioClient
import io.minio.credentials.IamAwsProvider
import io.minio.http.HttpUtils
import org.radarbase.output.config.RestructureConfig.Companion.copyEnv
import java.util.concurrent.TimeUnit

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
    /** HTTP connect timeout. */
    val connectTimeout: Long? = null,
    /** HTTP write timeout. */
    val writeTimeout: Long? = null,
    /** HTTP read timeout. */
    val readTimeout: Long? = null,
) {
    fun createS3Client(): MinioClient = MinioClient.Builder().apply {
        endpoint(endpoint)
        if (accessToken.isNullOrBlank() || secretKey.isNullOrBlank()) {
            credentialsProvider(IamAwsProvider(null, null))
        } else {
            credentials(accessToken, secretKey)
        }
        httpClient(HttpUtils.newDefaultHttpClient(
            connectTimeout.toMillisOrDefault(),
            writeTimeout.toMillisOrDefault(),
            readTimeout.toMillisOrDefault(),
        ))
    }.build()

    fun withEnv(prefix: String): S3Config = this
        .copyEnv("${prefix}S3_ACCESS_TOKEN") { copy(accessToken = it) }
        .copyEnv("${prefix}S3_SECRET_KEY") { copy(secretKey = it) }
        .copyEnv("${prefix}S3_BUCKET") { copy(bucket = it) }
        .copyEnv("${prefix}S3_ENDPOINT") { copy(endpoint = it) }

    companion object {
        private val DEFAULT_CONNECTION_TIMEOUT: Long = TimeUnit.MINUTES.toMillis(1)
        private fun Long?.toMillisOrDefault(): Long = this
            ?.takeIf { it > 0 }
            ?.let { TimeUnit.SECONDS.toMillis(it) }
            ?: DEFAULT_CONNECTION_TIMEOUT
    }
}
