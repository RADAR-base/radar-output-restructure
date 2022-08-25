package org.radarbase.output.path

import org.radarbase.output.Plugin

abstract class PathFormatterPlugin : Plugin {
    abstract val allowedFormats: String

    abstract fun createLookupTable(
        parameterNames: Set<String>
    ): Map<String, PathFormatParameters.() -> String>
}
