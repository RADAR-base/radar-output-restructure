package org.radarbase.output.target

import kotlinx.coroutines.coroutineScope
import org.radarbase.kotlin.coroutines.launchJoin
import org.radarbase.output.path.TargetPath

class TargetManager(
    private val delegates: Map<String, TargetStorage>,
    default: String,
) {
    constructor(name: String, targetStorage: TargetStorage) : this(mapOf(name to targetStorage), name)

    private val defaultDelegate =
        requireNotNull(delegates[default]) { "Default target storage $default not found in ${delegates.keys}" }

    operator fun contains(target: String) = target in delegates

    operator fun get(targetPath: TargetPath) = delegates[targetPath.target] ?: defaultDelegate

    suspend fun initialize() = coroutineScope {
        delegates.values.launchJoin { it.initialize() }
    }
}
