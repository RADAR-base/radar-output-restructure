package org.radarbase.output.config

import com.fasterxml.jackson.annotation.JsonIgnore
import org.slf4j.LoggerFactory
import java.nio.file.Paths

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
    val source: ResourceConfig? = null,
    /** Source data resource configuration. */
    val sources: List<ResourceConfig> = emptyList(),
    /** Target data resource configuration. */
    val target: ResourceConfig? = null,
    /** Target data resource configuration. */
    val targets: Map<String, ResourceConfig> = emptyMap(),
    /** Redis configuration for synchronization and storing offsets. */
    val redis: RedisConfig = RedisConfig(),
    /** Paths to use for processing. */
    val paths: PathConfig = PathConfig(),
    /** File compression to use for output files. */
    val compression: CompressionConfig = CompressionConfig(),
    /** File format to use for output files. */
    val format: FormatConfig = FormatConfig(),
) {
    @get:JsonIgnore
    val consolidatedTargets: Map<String, ResourceConfig> by lazy {
        buildMap(targets.size + 1) {
            putAll(targets)

            if (target != null) {
                val name = target.name
                if (name != null && name !in this) {
                    put(name, target)
                } else {
                    val bucketConfig = paths.target
                    require(bucketConfig.default !in this) { "Deprecated target storage does not have a proper name." }
                    put(bucketConfig.default, target)
                }
            }
        }
    }

    @get:JsonIgnore
    val consolidatedSources: List<ResourceConfig> by lazy {
        if (source != null) {
            sources + source
        } else {
            sources
        }
    }

    fun validate() {
        consolidatedSources.forEach(ResourceConfig::validate)
        consolidatedTargets.values.forEach(ResourceConfig::validate)
        cleaner.validate()
        service.validate()
        check(worker.enable || cleaner.enable) { "Either restructuring or cleaning needs to be enabled." }
    }

    /** Override configuration using command line arguments. */
    fun addArgs(args: CommandLineArgs) {
        args.asService?.let { copy(service = service.copy(enable = it)) }
        args.pollInterval?.let { copy(service = service.copy(interval = it)) }
        args.cacheSize?.let { copy(worker = worker.copy(cacheSize = it)) }
        args.numThreads?.let { copy(worker = worker.copy(numThreads = it)) }
        args.maxFilesPerTopic?.let { copy(worker = worker.copy(maxFilesPerTopic = it)) }
        args.tmpDir?.let { copy(paths = paths.copy(temp = Paths.get(it))) }
        args.format?.let { copy(format = format.copy(type = it)) }
        args.deduplicate?.let {
            copy(format = format.copy(deduplication = format.deduplication.copy(enable = it)))
        }
        args.compression?.let { copy(compression = compression.copy(type = it)) }
        args.clean?.let { copy(cleaner = cleaner.copy(enable = it)) }
        args.noRestructure?.let { copy(worker = worker.copy(enable = !it)) }
    }

    fun withEnv(): RestructureConfig = this
        .copyOnChange(source, { it?.withNamedEnv("SOURCE_") }) { copy(source = it) }
        .copyOnChange(sources, { it.map { source -> source.withNamedEnv("SOURCE_") } }) { copy(sources = it) }
        .copyOnChange(
            targets,
            {
                it.mapValues { (name, target) -> target.withNamedEnv("TARGET_", name) }
            },
        ) { copy(targets = it) }
        .copyOnChange(target, { it?.withNamedEnv("TARGET_") }) { copy(target = it) }
        .copyOnChange(redis, { it.withEnv() }) { copy(redis = it) }

    companion object {
        fun load(path: String?): RestructureConfig =
            YAMLConfigLoader.load(path, RESTRUCTURE_CONFIG_FILE_NAME) {
                logger.info("No config file found. Using default configuration.")
                RestructureConfig()
            }

        private val logger = LoggerFactory.getLogger(RestructureConfig::class.java)
        internal const val RESTRUCTURE_CONFIG_FILE_NAME = "restructure.yml"

        private val illegalEnvSymbols = "[^A-Za-z0-9]+".toRegex()

        private fun ResourceConfig.withNamedEnv(prefix: String, targetName: String? = null): ResourceConfig {
            val withFixedPrefix = withEnv(prefix)
            val useName = targetName
                ?: this.name
                ?: return withFixedPrefix
            return withFixedPrefix.withEnv(
                prefix + useName.replace(illegalEnvSymbols, "_").uppercase(),
            )
        }

        inline fun <T> T.copyEnv(key: String, doCopy: T.(String) -> T): T =
            copyOnChange<T, String?>(
                null,
                modification = {
                    System.getenv(key)
                        ?.takeIf { it.isNotEmpty() }
                },
                doCopy = { doCopy(requireNotNull(it) { "Environment variable $key is empty" }) },
            )

        inline fun <T, V> T.copyOnChange(
            original: V,
            modification: (V) -> V,
            doCopy: T.(V) -> T,
        ): T {
            val newValue = modification(original)
            return if (newValue != original) {
                doCopy(newValue)
            } else {
                this
            }
        }
    }
}
