package org.radarbase.output.config

data class BucketFormatterConfig(
    val format: String = "radar-output-storage",
    val plugins: String = "fixed time key value",
    val defaultName: String = "radar-output-storage",
    val disabledFormats: List<String> = emptyList(),
    val properties: Map<String, String> = emptyMap(),
)
