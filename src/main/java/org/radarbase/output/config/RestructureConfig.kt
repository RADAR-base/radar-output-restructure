package org.radarbase.output.config

import com.fasterxml.jackson.annotation.JsonIgnore
import io.minio.MinioClient
import org.apache.hadoop.conf.Configuration
import org.radarbase.output.Application.Companion.CACHE_SIZE_DEFAULT
import org.radarbase.output.Plugin
import org.radarbase.output.path.ObservationKeyPathFactory
import org.radarbase.output.compression.Compression
import org.radarbase.output.compression.CompressionFactory
import org.radarbase.output.format.FormatFactory
import org.radarbase.output.format.RecordConverterFactory
import org.radarbase.output.path.RecordPathFactory
import org.radarbase.output.storage.LocalStorageDriver
import org.radarbase.output.storage.S3StorageDriver
import org.radarbase.output.storage.StorageDriver
import org.slf4j.LoggerFactory
import java.lang.IllegalStateException
import java.net.URI
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

data class RestructureConfig(
        val service: ServiceConfig = ServiceConfig(false),
        val worker: WorkerConfig = WorkerConfig(),
        val topics: Map<String, TopicConfig> = emptyMap(),
        val source: ResourceConfig = ResourceConfig("hdfs", hdfs = HdfsConfig()),
        val target: ResourceConfig = ResourceConfig("local", local = LocalConfig()),
        val redis: RedisConfig = RedisConfig(),
        val paths: PathConfig = PathConfig(),
        val compression: CompressionConfig = CompressionConfig(),
        val format: FormatConfig = FormatConfig()) {

    fun validate() {
        source.validate()
        target.validate()
    }

    companion object {
        fun load(path: String?): RestructureConfig = YAMLConfigLoader.load(path, RESTRUCTURE_CONFIG_FILE_NAME) {
            logger.info("No config file found. Using default configuration.")
            RestructureConfig()
        }

        private val logger = LoggerFactory.getLogger(RestructureConfig::class.java)
        internal const val RESTRUCTURE_CONFIG_FILE_NAME = "restructure.yml"
    }
}

data class RedisConfig(
        val uri: URI = URI.create("redis://localhost:6379"),
        val lockPrefix: String = "radar-output/lock/")

data class ServiceConfig(val enable: Boolean, val interval: Long = 3600)

data class WorkerConfig(
        val numThreads: Int = 1,
        val maxFilesPerTopic: Int? = null,
        val cacheSize: Int = CACHE_SIZE_DEFAULT) {
    init {
        check(cacheSize >= 1) { "Maximum files per topic must be strictly positive" }
        maxFilesPerTopic?.let { check(it >= 1) { "Maximum files per topic must be strictly positive" } }
        check(numThreads >= 1) { "Number of threads should be at least 1" }
    }
}

interface PluginConfig {
    val factory: String
    val properties: Map<String, String>
}

data class PathConfig(
        override val factory: String = ObservationKeyPathFactory::class.qualifiedName!!,
        override val properties: Map<String, String> = emptyMap(),
        val inputs: List<Path> = emptyList(),
        val temp: Path = Files.createTempDirectory("radar-hdfs-restructure"),
        val output: Path = Paths.get("output")
) : PluginConfig {
    fun createFactory(): RecordPathFactory = factory.toPluginInstance(properties)
}

data class CompressionConfig(
        override val factory: String = CompressionFactory::class.qualifiedName!!,
        override val properties: Map<String, String> = emptyMap(),
        val type: String = "none"
) : PluginConfig {
    fun createFactory(): CompressionFactory = factory.toPluginInstance(properties)
    fun createCompression(): Compression = createFactory()[type]
}

data class FormatConfig(
        override val factory: String = FormatFactory::class.qualifiedName!!,
        override val properties: Map<String, String> = emptyMap(),
        val type: String = "csv",
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
        val deduplication: DeduplicationConfig? = null,
        val exclude: Boolean = false) {
    fun deduplication(deduplicationDefault: DeduplicationConfig): DeduplicationConfig {
        return deduplication
                ?.run { if (enable == null) copy(enable = deduplicationDefault.enable) else this }
                ?.run { if (distinctFields == null) copy(distinctFields = deduplicationDefault.distinctFields) else this }
                ?.run { if (ignoreFields == null) copy(distinctFields = deduplicationDefault.ignoreFields) else this }
                ?: deduplicationDefault
    }
}

data class DeduplicationConfig(
        val enable: Boolean? = null,
        val distinctFields: Set<String>? = null,
        val ignoreFields: Set<String>? = null)

data class HdfsConfig(
        val name: String? = null,
        val nameNodes: List<HdfsNameNode> = emptyList(),
        val properties: Map<String, String> = emptyMap()) {

    val configuration: Configuration = Configuration()

    init {
        if (name != null) {
            configuration["fs.defaultFS"] = "hdfs://$name"
            configuration["fs.hdfs.impl.disable.cache"] = "true"
            if (nameNodes.size >= 2) {
                configuration["dfs.nameservices"] = name
                configuration["dfs.ha.namenodes.$name"] = nameNodes.joinToString(",") { it.name }

                nameNodes.forEach {
                    configuration["dfs.namenode.rpc-address.$name.${it.name}"] = "${it.hostname}:8020"
                }

                configuration["dfs.client.failover.proxy.provider.$name"] = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
            }
        }
    }
}

data class ResourceConfig(
        val type: String,
        val s3: S3Config? = null,
        val hdfs: HdfsConfig? = null,
        val local: LocalConfig? = null) {

    @JsonIgnore
    lateinit var sourceType: ResourceType

    fun toStorageDriver(): StorageDriver = when(sourceType) {
        ResourceType.S3 -> S3StorageDriver(s3!!)
        ResourceType.LOCAL -> LocalStorageDriver(local!!)
        else -> throw IllegalArgumentException("Cannot create storage driver for $sourceType")
    }

    fun validate() {
        sourceType = type.toResourceType()

        when(sourceType) {
            ResourceType.S3 -> checkNotNull(s3)
            ResourceType.HDFS -> checkNotNull(hdfs?.name)
            ResourceType.LOCAL -> checkNotNull(local)
        }
    }
}

enum class ResourceType {
    S3, HDFS, LOCAL
}

fun String.toResourceType() = when(toLowerCase()) {
    "s3" -> ResourceType.S3
    "hdfs" -> ResourceType.HDFS
    "local" -> ResourceType.LOCAL
    else -> throw IllegalArgumentException("Unknown resource type $this, choose s3, hdfs or local")
}

data class LocalConfig(
        val userId: Int = -1,
        val groupId: Int = -1)

data class S3Config(
        val endpoint: String,
        val accessToken: String,
        val secretKey: String,
        val bucket: String) {
    fun createS3Client() = MinioClient(endpoint, accessToken, secretKey)
}

data class HdfsNameNode(val name: String, val hostname: String)
