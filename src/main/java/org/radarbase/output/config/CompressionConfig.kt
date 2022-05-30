package org.radarbase.output.config

import org.radarbase.output.compression.Compression
import org.radarbase.output.compression.CompressionFactory

data class CompressionConfig(
    override val factory: String = CompressionFactory::class.qualifiedName!!,
    override val properties: Map<String, String> = emptyMap(),
    /** Compression type. Currently one of gzip, zip or none. */
    val type: String = "none",
) : PluginConfig {
    fun createFactory(): CompressionFactory = factory.toPluginInstance(properties)
    fun createCompression(): Compression = createFactory()[type]
}
