package org.radarbase.hdfs.config

import org.apache.hadoop.conf.Configuration
import org.radarbase.hdfs.Application.Companion.CACHE_SIZE_DEFAULT
import org.radarbase.hdfs.ObservationKeyPathFactory
import org.radarbase.hdfs.RecordPathFactory
import org.radarbase.hdfs.data.*
import org.slf4j.LoggerFactory
import java.lang.IllegalStateException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

data class RestructureConfig(
        val service: ServiceConfig = ServiceConfig(false),
        val worker: WorkerConfig = WorkerConfig(),
        val topics: Map<String, TopicConfig> = emptyMap(),
        val hdfs: HdfsConfig = HdfsConfig(),
        val paths: PathConfig = PathConfig(),
        val compression: CompressionConfig = CompressionConfig(),
        val format: FormatConfig = FormatConfig(),
        val storage: StorageConfig = StorageConfig()) {

    fun validate() {
        checkNotNull(hdfs.name) { "HDFS name is missing"}
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
    fun createFactory(): RecordPathFactory = factory.toClassInstance()
}

data class CompressionConfig(
        override val factory: String = CompressionFactory::class.qualifiedName!!,
        override val properties: Map<String, String> = emptyMap(),
        val type: String = "none"
) : PluginConfig {
    fun createFactory(): CompressionFactory = factory.toClassInstance()
    fun createCompression(): Compression = createFactory()[type]
}

data class FormatConfig(
        override val factory: String = FormatFactory::class.qualifiedName!!,
        override val properties: Map<String, String> = emptyMap(),
        val type: String = "csv",
        val deduplicate: Boolean = false
) : PluginConfig {
    fun createFactory(): FormatFactory = factory.toClassInstance()
    fun createConverter(): RecordConverterFactory = createFactory()[type]
}

data class StorageConfig(
        override val factory: String = LocalStorageDriver::class.qualifiedName!!,
        override val properties: Map<String, String> = emptyMap()
): PluginConfig {
    fun createFactory(): StorageDriver = factory.toClassInstance()
}

private inline fun <reified T: Any> String.toClassInstance(): T {
    return try {
        Class.forName(this).getConstructor().newInstance() as T
    } catch (ex: ReflectiveOperationException) {
        throw IllegalStateException("Cannot map class $this to ${T::class.java.name}")
    }
}

data class TopicConfig(
        val deduplicate: Boolean? = null,
        val deduplicateFields: List<String> = listOf(),
        val exclude: Boolean = false) {
    fun isDeduplicated(defaultValue: Boolean) = deduplicate ?: defaultValue
}

data class HdfsConfig(
        val name: String? = null,
        val nameNodes: List<HdfsNameNode> = emptyList(),
        val lockPath: String = "/logs/org.radarbase.hdfs/lock",
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

data class HdfsNameNode(val name: String, val hostname: String)