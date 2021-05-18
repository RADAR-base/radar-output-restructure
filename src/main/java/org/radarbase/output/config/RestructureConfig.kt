package org.radarbase.output.config

import com.azure.core.credential.BasicAuthenticationCredential
import com.azure.storage.blob.BlobServiceClient
import com.azure.storage.blob.BlobServiceClientBuilder
import com.azure.storage.common.StorageSharedKeyCredential
import com.fasterxml.jackson.annotation.JsonIgnore
import io.minio.MinioClient
import io.minio.credentials.IamAwsProvider
import org.radarbase.output.Application.Companion.CACHE_SIZE_DEFAULT
import org.radarbase.output.Plugin
import org.radarbase.output.compression.Compression
import org.radarbase.output.compression.CompressionFactory
import org.radarbase.output.config.RestructureConfig.Companion.copyEnv
import org.radarbase.output.config.RestructureConfig.Companion.copyOnChange
import org.radarbase.output.format.FormatFactory
import org.radarbase.output.format.RecordConverterFactory
import org.radarbase.output.path.FormattedPathFactory
import org.radarbase.output.path.RecordPathFactory
import org.slf4j.LoggerFactory
import java.net.URI
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Duration

data class RestructureConfig(
    /** Whether and how to run as a service. */
    val service: ServiceConfig = ServiceConfig(enable = false),
    /** Cleaner of old files. */
    val cleaner: CleanerConfig = CleanerConfig(),
    /** Work limits. */
    val worker: WorkerConfig = WorkerConfig(),
    /** Topic exceptional handling. */
    val topics: Map<String, TopicConfig> = emptyMap(),
    /** Source data resource configuration. */
    val source: ResourceConfig = ResourceConfig("s3"),
    /** Target data resource configuration. */
    val target: ResourceConfig = ResourceConfig("local", local = LocalConfig()),
    /** Redis configuration for synchronization and storing offsets. */
    val redis: RedisConfig = RedisConfig(),
    /** Paths to use for processing. */
    val paths: PathConfig = PathConfig(),
    /** File compression to use for output files. */
    val compression: CompressionConfig = CompressionConfig(),
    /** File format to use for output files. */
    val format: FormatConfig = FormatConfig(),
    /** Snapshot */
    val snapshot: SnapshotConfig = SnapshotConfig(),
) {

    fun validate() {
        source.validate()
        target.validate()
        cleaner.validate()
        service.validate()
        check(worker.enable || cleaner.enable) { "Either restructuring or cleaning needs to be enabled."}
    }

    /** Override configuration using command line arguments. */
    fun addArgs(args: CommandLineArgs) {
        args.asService?.let { copy(service = service.copy(enable = it)) }
        args.pollInterval?.let { copy(service = service.copy(interval = it)) }
        args.cacheSize?.let { copy(worker = worker.copy(cacheSize = it)) }
        args.numThreads?.let { copy(worker = worker.copy(numThreads = it)) }
        args.maxFilesPerTopic?.let { copy(worker = worker.copy(maxFilesPerTopic = it)) }
        args.tmpDir?.let { copy(paths = paths.copy(temp = Paths.get(it))) }
        args.inputPaths?.let { inputs -> copy(paths = paths.copy(inputs = inputs.map { Paths.get(it) })) }
        args.outputDirectory?.let { copy(paths = paths.copy(output = Paths.get(it))) }
        args.hdfsName?.let { copy(source = source.copy(hdfs = source.hdfs?.copy(nameNodes = listOf(it)) ?: HdfsConfig(nameNodes = listOf(it)))) }
        args.format?.let { copy(format = format.copy(type = it)) }
        args.deduplicate?.let { copy(format = format.copy(deduplication = format.deduplication.copy(enable = it))) }
        args.compression?.let { copy(compression = compression.copy(type = it)) }
        args.clean?.let { copy(cleaner = cleaner.copy(enable = it)) }
        args.noRestructure?.let { copy(worker = worker.copy(enable = !it)) }
    }

    fun withEnv(): RestructureConfig = this
        .copyOnChange(source, { it.withEnv("SOURCE_") }) { copy(source = it) }
        .copyOnChange(target, { it.withEnv("TARGET_") }) { copy(target = it) }
        .copyOnChange(redis, { it.withEnv() }) { copy(redis = it) }

    companion object {
        fun load(path: String?): RestructureConfig = YAMLConfigLoader.load(path, RESTRUCTURE_CONFIG_FILE_NAME) {
            logger.info("No config file found. Using default configuration.")
            RestructureConfig()
        }

        private val logger = LoggerFactory.getLogger(RestructureConfig::class.java)
        internal const val RESTRUCTURE_CONFIG_FILE_NAME = "restructure.yml"

        inline fun <T> T.copyEnv(key: String, doCopy: T.(String) -> T): T = copyOnChange<T, String?>(
            null,
            modification = { System.getenv(key) },
            doCopy = { doCopy(requireNotNull(it)) }
        )

        inline fun <T, V> T.copyOnChange(original: V, modification: (V) -> V, doCopy: T.(V) -> T): T {
            val newValue = modification(original)
            return if (newValue != original) {
                doCopy(newValue)
            } else this
        }
    }
}

/** Redis configuration. */
data class RedisConfig(
    /**
     * Full Redis URI. The protocol should be redis for plain text and rediss for TLS. It
     * should contain at least a hostname and port, but it may also include username and
     * password.
     */
    val uri: URI = URI.create("redis://localhost:6379"),
    /**
     * Prefix to use for creating a lock of a topic.
     */
    val lockPrefix: String = "radar-output/lock",
) {
    fun withEnv(): RedisConfig = this
        .copyEnv("REDIS_URI") { copy(uri = URI.create(it)) }
}

data class ServiceConfig(
    /** Whether to enable the service mode of this application. */
    val enable: Boolean,
    /** Polling interval in seconds. */
    val interval: Long = 300L,
    /** Age in days after an avro file can be removed. Ignored if not strictly positive. */
    val deleteAfterDays: Int = -1,
) {
    fun validate() {
        check(interval > 0) { "Cleaner interval must be strictly positive" }
    }
}

data class CleanerConfig(
    /** Whether to enable the cleaner. */
    val enable: Boolean = false,
    /** How often to run the cleaner in seconds. */
    val interval: Long = 1260L,
    /** Age in days after an avro file can be removed. Must be strictly positive. */
    val age: Int = 7,
) {
    fun validate() {
        check(age > 0) { "Cleaner file age must be strictly positive" }
        check(interval > 0) { "Cleaner interval must be strictly positive" }
    }
}

data class WorkerConfig(
    /** Whether to enable restructuring */
    val enable: Boolean = true,
    /** Number of threads to use for processing files. */
    val numThreads: Int = 1,
    /**
     * Maximum number of files to process for a given topic. Limit this to ensure that a single
     * processing iteration including lock takes a limited amount of time.
     */
    val maxFilesPerTopic: Int? = null,
    /**
     * Number of files to simultaneously keep in cache, including open writer. A higher size will
     * decrease overhead but increase memory usage and open file descriptors.
     */
    val cacheSize: Int = CACHE_SIZE_DEFAULT,
    /**
     * Number of offsets to simultaneously keep in cache. A higher size will
     * decrease overhead but increase memory usage.
     */
    val cacheOffsetsSize: Long = 500_000,
    /**
     * Minimum time since the file was last modified in seconds. Avoids
     * synchronization issues that may occur in a source file that is being
     * appended to.
     */
    val minimumFileAge: Long = 60,
) {
    init {
        check(cacheSize >= 1) { "Maximum files per topic must be strictly positive" }
        maxFilesPerTopic?.let { check(it >= 1) { "Maximum files per topic must be strictly positive" } }
        check(numThreads >= 1) { "Number of threads should be at least 1" }
    }
}

interface PluginConfig {
    /** Factory class to use. */
    val factory: String
    /** Additional plugin-specific properties. */
    val properties: Map<String, String>
}

data class PathConfig(
    override val factory: String = FormattedPathFactory::class.qualifiedName!!,
    override val properties: Map<String, String> = emptyMap(),
    /** Input paths referencing the source resource. */
    val inputs: List<Path> = emptyList(),
    /** Temporary directory for processing output files before uploading. */
    val temp: Path = Files.createTempDirectory("radar-output-restructure"),
    /** Output path on the target resource. */
    val output: Path = Paths.get("output"),
    /** Output path on the target resource. */
    val snapshots: Path = Paths.get("snapshots"),
) : PluginConfig {
    fun createFactory(): RecordPathFactory = factory.toPluginInstance(properties)
}

data class CompressionConfig(
    override val factory: String = CompressionFactory::class.qualifiedName!!,
    override val properties: Map<String, String> = emptyMap(),
    /** Compression type. Currently one of gzip, zip or none. */
    val type: String = "none",
) : PluginConfig {
    fun createFactory(): CompressionFactory = factory.toPluginInstance(properties)
    fun createCompression(): Compression = createFactory()[type]
}

data class FormatConfig(
        override val factory: String = FormatFactory::class.qualifiedName!!,
        override val properties: Map<String, String> = emptyMap(),
        /** Output format. One of csv or json. */
        val type: String = "csv",
        /** Whether and how to remove duplicate entries. */
        val deduplication: DeduplicationConfig = DeduplicationConfig(enable = false, distinctFields = emptySet(), ignoreFields = emptySet())
) : PluginConfig {
    fun createFactory(): FormatFactory = factory.toPluginInstance(properties)
    fun createConverter(): RecordConverterFactory = createFactory()[type]
}

private inline fun <reified T: Plugin> String.toPluginInstance(properties: Map<String, String>): T {
    return try {
        (Class.forName(this).getConstructor().newInstance() as T)
                .also { it.init(properties) }
    } catch (ex: ReflectiveOperationException) {
        throw IllegalStateException("Cannot map class $this to ${T::class.java.name}")
    }
}

data class TopicConfig(
    /** Topic-specific deduplication handling. */
    val deduplication: DeduplicationConfig = DeduplicationConfig(),
    /** Whether to exclude the topic from being processed. */
    val exclude: Boolean = false,
    /**
     * Whether to exclude the topic from being deleted, if this configuration has been set
     * in the service.
     */
    val excludeFromDelete: Boolean = false,
) {
    fun deduplication(deduplicationDefault: DeduplicationConfig): DeduplicationConfig = deduplication
        .withDefaults(deduplicationDefault)
}

data class DeduplicationConfig(
    /** Whether to enable deduplication. */
    val enable: Boolean? = null,
    /**
     * Only deduplicate using given fields. Fields not specified here are ignored
     * for determining duplication.
     */
    val distinctFields: Set<String>? = null,
    /**
     * Ignore given fields for determining whether a row is identical to another.
     */
    val ignoreFields: Set<String>? = null,
) {
    fun withDefaults(deduplicationDefaults: DeduplicationConfig): DeduplicationConfig = deduplicationDefaults
        .copyOnChange<DeduplicationConfig, Boolean?>(null, { enable }) { copy(enable = it) }
        .copyOnChange<DeduplicationConfig, Set<String>?>(null, { distinctFields }) { copy(distinctFields = it) }
        .copyOnChange<DeduplicationConfig, Set<String>?>(null, { ignoreFields }) { copy(ignoreFields = it) }
}

data class HdfsConfig(
    /** HDFS name nodes to use. */
    val nameNodes: List<String> = emptyList(),
    /** Additional HDFS configuration parameters. */
    val properties: Map<String, String> = emptyMap(),
) {
    fun validate() {
        check(nameNodes.isNotEmpty()) { "Cannot use HDFS without any name nodes." }
    }
}

data class ResourceConfig(
    /** Resource type. One of s3, hdfs or local. */
    val type: String,
    val s3: S3Config? = null,
    val hdfs: HdfsConfig? = null,
    val local: LocalConfig? = null,
    val azure: AzureConfig? = null,
) {
    @get:JsonIgnore
    val sourceType: ResourceType by lazy {
        requireNotNull(type.toResourceType()) { "Unknown resource type $type, choose s3, hdfs or local" }
    }

    fun validate() {
        when (sourceType) {
            ResourceType.S3 -> checkNotNull(s3) { "No S3 configuration provided." }
            ResourceType.HDFS -> checkNotNull(hdfs) { "No HDFS configuration provided." }.also { it.validate() }
            ResourceType.LOCAL -> checkNotNull(local) { "No local configuration provided." }
            ResourceType.AZURE -> checkNotNull(azure) { "No Azure configuration provided." }
        }
    }

    fun withEnv(prefix: String): ResourceConfig = when (sourceType) {
        ResourceType.S3 -> copyOnChange(s3, { it?.withEnv(prefix) }) { copy(s3 = it) }
        ResourceType.HDFS -> this
        ResourceType.LOCAL -> this
        ResourceType.AZURE -> copyOnChange(azure, { it?.withEnv(prefix) }) { copy(azure = it) }
    }
}

enum class ResourceType {
    S3, HDFS, LOCAL, AZURE
}

fun String.toResourceType() = when(toLowerCase()) {
    "s3" -> ResourceType.S3
    "hdfs" -> ResourceType.HDFS
    "local" -> ResourceType.LOCAL
    "azure" -> ResourceType.AZURE
    else -> null
}

data class LocalConfig(
    /** User ID (uid) to write data as. Only valid on Unix-based filesystems. */
    val userId: Int = -1,
    /** Group ID (gid) to write data as. Only valid on Unix-based filesystems. */
    val groupId: Int = -1,
)

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
) {
    fun createAzureClient(): BlobServiceClient = BlobServiceClientBuilder().apply {
        endpoint(endpoint)
        when {
            !username.isNullOrEmpty() && !password.isNullOrEmpty() -> credential(BasicAuthenticationCredential(username, password))
            !accountName.isNullOrEmpty() && !accountKey.isNullOrEmpty() -> credential(StorageSharedKeyCredential(accountName, accountKey))
            !sasToken.isNullOrEmpty() -> sasToken(sasToken)
            else -> logger.warn("No Azure credentials supplied. Assuming a public blob storage.")
        }
    }.buildClient()

    fun withEnv(prefix: String): AzureConfig = this
        .copyEnv("${prefix}AZURE_USERNAME") { copy(username = it) }
        .copyEnv("${prefix}AZURE_PASSWORD") { copy(password = it) }
        .copyEnv("${prefix}AZURE_ACCOUNT_NAME") { copy(accountName = it) }
        .copyEnv("${prefix}AZURE_ACCOUNT_KEY") { copy(accountKey = it) }
        .copyEnv("${prefix}AZURE_SAS_TOKEN") { copy(sasToken = it) }

    companion object {
        private val logger = LoggerFactory.getLogger(AzureConfig::class.java)
    }
}

data class SnapshotConfig(
    val enable: Boolean = false,
    val frequency: Duration = Duration.ofDays(31),
    val numberOfSnapshots: Int = 12,
    val sourceFormat: String = "\${projectId}",
    val targetFormat: String = "\${projectId}",
)
