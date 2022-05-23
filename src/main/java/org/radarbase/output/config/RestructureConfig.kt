package org.radarbase.output.config

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
        args.inputPaths?.let { inputs -> copy(paths = paths.copy(inputs = inputs.map { Paths.get(it) })) }
        args.outputDirectory?.let { copy(paths = paths.copy(output = Paths.get(it))) }
        args.hdfsName?.let {
            copy(source = source.copy(hdfs = source.hdfs?.copy(nameNodes = listOf(it))
                ?: HdfsConfig(nameNodes = listOf(it))))
        }
        args.format?.let { copy(format = format.copy(type = it)) }
        args.deduplicate?.let {
            copy(format = format.copy(deduplication = format.deduplication.copy(enable = it)))
        }
        args.compression?.let { copy(compression = compression.copy(type = it)) }
        args.clean?.let { copy(cleaner = cleaner.copy(enable = it)) }
        args.noRestructure?.let { copy(worker = worker.copy(enable = !it)) }
    }

    fun withEnv(): RestructureConfig = this
        .copyOnChange(source, { it.withEnv("SOURCE_") }) { copy(source = it) }
        .copyOnChange(target, { it.withEnv("TARGET_") }) { copy(target = it) }
        .copyOnChange(redis, { it.withEnv() }) { copy(redis = it) }

    companion object {
        fun load(path: String?): RestructureConfig =
            YAMLConfigLoader.load(path, RESTRUCTURE_CONFIG_FILE_NAME) {
                logger.info("No config file found. Using default configuration.")
                RestructureConfig()
            }

        private val logger = LoggerFactory.getLogger(RestructureConfig::class.java)
        internal const val RESTRUCTURE_CONFIG_FILE_NAME = "restructure.yml"

        inline fun <T> T.copyEnv(key: String, doCopy: T.(String) -> T): T =
            copyOnChange<T, String?>(
                null,
                modification = { System.getenv(key) },
                doCopy = { doCopy(requireNotNull(it)) }
            )

        inline fun <T, V> T.copyOnChange(
            original: V,
            modification: (V) -> V,
            doCopy: T.(V) -> T,
        ): T {
            val newValue = modification(original)
            return if (newValue != original) {
                doCopy(newValue)
            } else this
        }
    }
}

