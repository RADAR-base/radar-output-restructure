package org.radarbase.output.config

import org.radarbase.output.path.FormattedPathFactory
import org.radarbase.output.path.RecordPathFactory
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.io.path.createTempDirectory

data class PathConfig(
    override val factory: String = FormattedPathFactory::class.qualifiedName!!,
    override val properties: Map<String, String> = emptyMap(),
    /** Input paths referencing the source resource. */
    val inputs: List<Path> = emptyList(),
    /** Temporary directory for processing output files before uploading. */
    val temp: Path = createTempDirectory("radar-output-restructure"),
    /** Output path on the target resource. */
    val output: Path = Paths.get("output"),
    /** Output path on the target resource. */
    val snapshots: Path = Paths.get("snapshots"),
) : PluginConfig {
    fun createFactory(): RecordPathFactory = factory.toPluginInstance(properties)
}
