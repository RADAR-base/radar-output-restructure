package org.radarbase.output.config

import org.radarbase.output.path.FormattedPathFactory
import org.radarbase.output.path.RecordPathFactory
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.io.path.createTempDirectory
import kotlin.reflect.jvm.jvmName

data class PathConfig(
    override val factory: String = FormattedPathFactory::class.jvmName,
    override val properties: Map<String, String> = emptyMap(),
    /** Input paths referencing the source resource. */
    val inputs: List<Path> = emptyList(),
    /** Temporary directory for processing output files before uploading. */
    val temp: Path = createTempDirectory("radar-output-restructure"),
    /** Output path on the target resource. */
    val output: Path = Paths.get("output"),
    /** Path formatting rules. */
    val path: PathFormatterConfig = PathFormatterConfig(),
    /**
     * Bucket formatting rules for the target storage. If no configuration is provided, this
     * will not format any bucket for local or HDFS storage, and it will use the target bucket (s3)
     * or container (azure) as the default target bucket.
     */
    val bucket: BucketFormatterConfig? = null,
) : PluginConfig {
    fun createFactory(
        target: ResourceConfig,
        extension: String,
        topics: Map<String, TopicConfig>,
    ): RecordPathFactory {
        val pathFactory = factory.constructClass<RecordPathFactory>()

        val bucketConfig = bucket
            ?: when (target.sourceType) {
                ResourceType.AZURE -> {
                    val container = requireNotNull(target.azure?.container) { "Either target container or bucket formatter config needs to be configured." }
                    BucketFormatterConfig(format = container, plugins = "", defaultName = container)
                }
                ResourceType.S3 -> {
                    val bucket = requireNotNull(target.s3?.bucket) { "Either target container or bucket formatter config needs to be configured." }
                    BucketFormatterConfig(format = bucket, plugins = "", defaultName = bucket)
                }
                else -> null
            }

        pathFactory.init(
            extension = extension,
            config = copy(bucket = bucketConfig),
            topics = topics,
        )

        return pathFactory
    }
}
