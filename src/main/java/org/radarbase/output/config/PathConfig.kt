package org.radarbase.output.config

import org.radarbase.output.path.FormattedPathFactory
import org.radarbase.output.path.RecordPathFactory
import org.radarbase.output.target.TargetStorage
import java.nio.file.Path
import kotlin.io.path.createTempDirectory
import kotlin.reflect.jvm.jvmName

data class PathConfig(
    override val factory: String = FormattedPathFactory::class.jvmName,
    override val properties: Map<String, String> = emptyMap(),
    /** Temporary directory for processing output files before uploading. */
    val temp: Path = createTempDirectory("radar-output-restructure"),
    /** Path formatting rules. */
    val path: PathFormatterConfig = PathFormatterConfig(),
    /**
     * Formatting rules for the target storage.
     */
    val target: TargetFormatterConfig = TargetFormatterConfig(),
) : PluginConfig {
    fun createFactory(
        targetStorage: TargetStorage,
        extension: String,
        topics: Map<String, TopicConfig>,
    ): RecordPathFactory {
        val pathFactory = factory.constructClass<RecordPathFactory>()

        require(targetStorage.allowsPrefix(target.defaultName)) { "Default bucket ${target.defaultName} is not specified as a target storage" }

        pathFactory.init(
            targetStorage = targetStorage,
            extension = extension,
            config = this,
            topics = topics,
        )

        return pathFactory
    }
}
