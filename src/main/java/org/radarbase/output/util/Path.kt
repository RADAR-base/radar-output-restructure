package org.radarbase.output.util

import java.nio.file.Path
import java.nio.file.Paths

private val rootPath = Paths.get("/")

internal fun Path.toKey() = if (startsWith(rootPath)) {
    rootPath.relativize(this).toString()
} else toString()
