package org.radarbase.output.config

import org.radarbase.output.Plugin

internal inline fun <reified T : Plugin> String.toPluginInstance(properties: Map<String, String>): T {
    return try {
        (Class.forName(this).getConstructor().newInstance() as T)
            .also { it.init(properties) }
    } catch (ex: ReflectiveOperationException) {
        throw IllegalStateException("Cannot map class $this to ${T::class.java.name}")
    }
}
