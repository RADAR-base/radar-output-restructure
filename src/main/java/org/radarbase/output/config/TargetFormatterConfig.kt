package org.radarbase.output.config

/** Configuration on how to format the target storage name to be used. */
data class TargetFormatterConfig(
    /** Format string. May include any variables computed by the configured plugins. */
    val format: String = "radar-output-storage",
    /**
     * Spaces separated list of plugins to use for formatting the format string. May include
     * custom class names.
     */
    val plugins: String = "fixed time key value",
    /** List of regexes to disable the formatted string for and use [defaultName] instead. */
    val disabledFormats: List<String> = emptyList(),
    /**
     * Default name to use for the output storage if the output format is disabled via
     * [disabledFormats].
     */
    val defaultName: String = "radar-output-storage",
    /** Additional plugin properties. */
    val properties: Map<String, String> = emptyMap(),
)
