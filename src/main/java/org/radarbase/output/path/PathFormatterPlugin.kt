package org.radarbase.output.path

import org.slf4j.LoggerFactory
import kotlin.reflect.jvm.jvmName

private val logger = LoggerFactory.getLogger(PathFormatterPlugin::class.java)

internal fun String.toPathFormatterPlugins(
    properties: Map<String, String>,
): List<PathFormatterPlugin> =
    splitToSequence("\\s+".toRegex())
        .filter { it.isNotEmpty() }
        .mapNotNull { it.toPathFormatterPlugin(properties) }
        .toList()

internal fun String.toPathFormatterPlugin(
    properties: Map<String, String>,
): PathFormatterPlugin? = when (this) {
    "fixed" -> FixedPathFormatterPlugin().create(properties)
    "time" -> TimePathFormatterPlugin()
    "key" -> KeyPathFormatterPlugin()
    "value" -> ValuePathFormatterPlugin()
    "mp" -> MPPathFormatterPlugin().create(properties)
    else -> {
        try {
            val clazz = Class.forName(this)
            when (val plugin = clazz.getConstructor().newInstance()) {
                is PathFormatterPlugin -> plugin
                is PathFormatterPlugin.Factory -> plugin.create(properties)
                else -> {
                    logger.error(
                        "Failed to instantiate plugin {}, it does not extend {} or {}",
                        this,
                        PathFormatterPlugin::class.jvmName,
                        PathFormatterPlugin.Factory::class.jvmName,
                    )
                    null
                }
            }
        } catch (ex: ReflectiveOperationException) {
            logger.error("Failed to instantiate plugin {}", this)
            null
        }
    }
}

abstract class PathFormatterPlugin {
    /**
     * Short name to reference this plugin by.
     */
    open val name: String
        get() = prefix ?: javaClass.canonicalName

    /**
     * Prefix for parameter names covered by this plugin. If null, [extractParamContents] must be
     * overridden to cover only supported parameters.
     */
    open val prefix: String? = null

    /** Textual format of formats allowed to be represented. */
    abstract val allowedFormats: String

    /**
     * Create a lookup table from parameter names to
     * its value for a given record. Only parameter names supported by this plugin will be mapped.
     * @throws IllegalArgumentException if any of the parameter contents are invalid.
     */
    fun createLookupTable(
        parameterNames: Collection<String>,
    ): Map<String, suspend PathFormatParameters.() -> String> = buildMap {
        parameterNames.forEach { paramName ->
            val paramContents = extractParamContents(paramName)
            if (paramContents != null) {
                put(paramName, lookup(paramContents))
            }
        }
    }

    /**
     * Validate a parameter name and extract its contents to use in the lookup.
     *
     * @return name to use in the lookup or null if the parameter is not supported by this plugin
     */
    protected open fun extractParamContents(paramName: String): String? {
        val prefixString = prefix?.let { "$it:" } ?: return null
        if (!paramName.startsWith(prefixString)) return null
        val parameterContents = paramName.removePrefix(prefixString).trim()
        require(parameterContents.isNotEmpty()) { "Parameter contents of '$paramName' are empty" }
        return parameterContents
    }

    /**
     * Create a lookup function from a record to formatted value, based on parameter contents.
     * @throws IllegalArgumentException if the parameter contents are invalid.
     */
    protected abstract fun lookup(parameterContents: String): suspend PathFormatParameters.() -> String

    interface Factory {
        /**
         * Factory to create new plugin with.
         */
        fun create(
            properties: Map<String, String> = emptyMap(),
        ): PathFormatterPlugin
    }
}
