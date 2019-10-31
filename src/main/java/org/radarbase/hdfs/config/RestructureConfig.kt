package org.radarbase.hdfs.config

import org.apache.hadoop.conf.Configuration
import org.radarbase.hdfs.Application.Companion.CACHE_SIZE_DEFAULT
import org.radarbase.hdfs.ObservationKeyPathFactory
import org.radarbase.hdfs.Plugin
import org.radarbase.hdfs.RecordPathFactory
import org.radarbase.hdfs.data.CompressionFactory
import org.radarbase.hdfs.data.FormatFactory
import org.radarbase.hdfs.data.LocalStorageDriver
import org.radarbase.hdfs.data.StorageDriver
import org.slf4j.LoggerFactory
import java.lang.IllegalStateException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

data class RestructureConfig(
        val interval: Long = 3600,
        val compression: String = "none",
        val format: String = "csv",
        val cacheSize: Int = CACHE_SIZE_DEFAULT,
        val inputPaths: List<Path>,
        val tempPath: Path,
        val outputPath: Path,
        val numThreads: Int = 1,
        val maxFilesPerTopic: Int? = null,
        val deduplicate: Boolean = false,
        val topics: Map<String, TopicConfig> = emptyMap(),
        val hdfs: HdfsConfig,
        val pathFactory: PluginConfig<RecordPathFactory> = PluginConfig(ObservationKeyPathFactory(), emptyMap()),
        val compressionFactory: PluginConfig<CompressionFactory> = PluginConfig(CompressionFactory(), emptyMap()),
        val formatFactory: PluginConfig<FormatFactory> = PluginConfig(FormatFactory(), emptyMap()),
        val storageDriver: PluginConfig<StorageDriver> = PluginConfig(LocalStorageDriver(), emptyMap())) {

    init {
        require(inputPaths.isNotEmpty()) { "Missing input paths"}
        check(numThreads >= 1) { "Number of threads should be at least 1" }
        check(cacheSize >= 1) { "Maximum files per topic must be strictly positive" }
        maxFilesPerTopic?.let { check(it >= 1) { "Maximum files per topic must be strictly positive" } }
    }

    fun topic(topic: String): TopicConfig = topics[topic] ?: TopicConfig(deduplicate)
}

data class MutableRestructureConfig(
        var interval: Long = 3600,
        var compression: String = "gzip",
        var format: String = "csv",
        var cacheSize: Int = CACHE_SIZE_DEFAULT,
        var inputDirs: List<String> = emptyList(),
        var tempDir: String? = null,
        var outputDir: String? = null,
        var numThreads: Int = 1,
        var maxFilesPerTopic: Int? = null,
        var deduplicate: Boolean = true,
        var topics: Map<String, MutableTopicConfig> = emptyMap(),
        var hdfs: MutableHdfsConfig = MutableHdfsConfig(),
        val pathFactory: MutablePluginConfig = MutablePluginConfig(ObservationKeyPathFactory::class.qualifiedName!!),
        val compressionFactory: MutablePluginConfig = MutablePluginConfig(CompressionFactory::class.qualifiedName!!),
        val formatFactory: MutablePluginConfig = MutablePluginConfig(FormatFactory::class.qualifiedName!!),
        val storageDriver: MutablePluginConfig = MutablePluginConfig(LocalStorageDriver::class.qualifiedName!!)) {

    fun toRestructureConfig() = RestructureConfig(
            interval,
            compression,
            format,
            cacheSize.coerceAtLeast(1),
            inputDirs.map { Paths.get(it) },
            tempDir?.let { Paths.get(it) }
                    ?: Files.createTempDirectory("radar-hdfs-restructure"),
            Paths.get(checkNotNull(outputDir) { "Missing output path" }),
            numThreads.coerceAtLeast(1),
            maxFilesPerTopic?.coerceAtLeast(1),
            deduplicate,
            topics.mapValues { (_, conf) -> conf.toTopicConfig(deduplicate) },
            hdfs.toHdfsConfig(),
            pathFactory.toPluginConfig(),
            compressionFactory.toPluginConfig(),
            formatFactory.toPluginConfig(),
            storageDriver.toPluginConfig())

    companion object {
        fun load(path: String?): MutableRestructureConfig = YAMLConfigLoader.load(path, RESTRUCTURE_CONFIG_FILE_NAME) {
            logger.info("No config file found. Using default configuration.")
            MutableRestructureConfig()
        }

        private val logger = LoggerFactory.getLogger(MutableRestructureConfig::class.java)
        const val RESTRUCTURE_CONFIG_FILE_NAME = "restructure.yml"
    }
}

data class MutablePluginConfig(
        var `class`: String,
        val properties: MutableMap<String, String> = mutableMapOf()) {

    inline fun <reified T: Plugin> toPluginConfig(): PluginConfig<T> {
        return try {
            val factoryClass = Class.forName(`class`)
            PluginConfig(
                    factoryClass.getConstructor().newInstance() as T,
                    properties.toMap())
        } catch (ex: ReflectiveOperationException) {
            throw IllegalStateException("Cannot map class $`class` to ${T::class.java.name}")
        }
    }
}

data class PluginConfig<T: Plugin>(
        val factory: T,
        val properties: Map<String, String> = emptyMap()
) {
    init {
        factory.init(properties)
    }
}

data class MutableTopicConfig(
        var deduplicate: Boolean? = null,
        val deduplicateFields: MutableList<String> = mutableListOf(),
        var exclude: Boolean = false) {
    fun toTopicConfig(defaultDeduplicate: Boolean) = TopicConfig(
            deduplicate ?: defaultDeduplicate,
            deduplicateFields.toList(),
            exclude)
}

data class TopicConfig(
        val deduplicate: Boolean,
        val deduplicateFields: List<String> = listOf(),
        val exclude: Boolean = false)

data class MutableHdfsConfig(
        var name: String? = null,
        val nameNodes: MutableList<HdfsNameNode> = mutableListOf(),
        var lockDirectory: String = "/logs/org.radarbase.hdfs/lock",
        val properties: MutableMap<String, String> = mutableMapOf()) {

    fun toHdfsConfig() = HdfsConfig(
            checkNotNull(name) { "HDFS cluster ID or name nodes not provided"},
            nameNodes.toList(),
            lockDirectory,
            properties.toMap())
}

data class HdfsConfig(
        val name: String,
        val nameNodes: List<HdfsNameNode> = emptyList(),
        val lockDirectory: String = "/logs/org.radarbase.hdfs/lock",
        val properties: Map<String, String> = emptyMap()) {

    val configuration: Configuration = Configuration()

    init {
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

data class HdfsNameNode(val name: String, val hostname: String)