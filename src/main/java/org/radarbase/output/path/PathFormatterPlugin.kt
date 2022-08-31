package org.radarbase.output.path

import org.radarbase.output.Plugin

abstract class PathFormatterPlugin : Plugin {
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
        parameterNames: Collection<String>
    ): Map<String, PathFormatParameters.() -> String> = buildMap {
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
    protected abstract fun lookup(parameterContents: String): PathFormatParameters.() -> String
}
