package org.radarbase.output.config

import org.radarbase.output.format.FormatFactory
import org.radarbase.output.format.RecordConverterFactory

data class FormatConfig(
    override val factory: String = FormatFactory::class.qualifiedName!!,
    override val properties: Map<String, String> = emptyMap(),
    /** Output format. One of csv or json. */
    val type: String = "csv",
    /** Whether and how to remove duplicate entries. */
    val deduplication: DeduplicationConfig = DeduplicationConfig(
        enable = false,
        distinctFields = emptySet(),
        ignoreFields = emptySet(),
    ),
    val excludeFields: Set<String> = emptySet(),
) : PluginConfig {
    fun createFactory(): FormatFactory = factory.toPluginInstance(properties)
    fun createConverter(): RecordConverterFactory = createFactory()[type]
}
