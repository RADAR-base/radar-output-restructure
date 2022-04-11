package org.radarbase.output.config

interface PluginConfig {
    /** Factory class to use. */
    val factory: String

    /** Additional plugin-specific properties. */
    val properties: Map<String, String>
}
