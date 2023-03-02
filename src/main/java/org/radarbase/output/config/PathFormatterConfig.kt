package org.radarbase.output.config

data class PathFormatterConfig(
    val format: String = DEFAULT_FORMAT,
    val plugins: String = "fixed time key value",
    val properties: Map<String, String> = mapOf(),
) {
    fun copy(values: PathFormatterConfig): PathFormatterConfig {
        val copy = PathFormatterConfig(
            format = values.format,
            plugins = values.plugins,
            properties = buildMap(properties.size + values.properties.size) {
                putAll(properties)
                putAll(values.properties)
            },
        )
        return if (this == copy) this else copy
    }

    companion object {
        const val DEFAULT_FORMAT = "\${projectId}/\${userId}/\${topic}/\${filename}"
    }
}
