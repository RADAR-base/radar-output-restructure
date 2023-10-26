package org.radarbase.output.config

/** Configuration on how to format the target storage name to be used. */
data class TargetFormatterConfig(
    /** Format string. May include any variables computed by the configured plugins. */
    val format: String? = null,
    /**
     * Spaces separated list of plugins to use for formatting the format string. May include
     * custom class names.
     */
    val plugins: String = "fixed time key value",
    /** List of regexes to disable the formatted string for and use [default] instead. */
    val disabledFormats: List<String> = emptyList(),
    /**
     * Default name to use for the output storage if the output format is disabled via
     * [disabledFormats].
     */
    val default: String = "radar-output-storage",
    /** Additional plugin properties. */
    val properties: Map<String, String> = emptyMap(),
)
