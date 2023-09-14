package org.radarbase.output.path

import kotlinx.coroutines.*
import org.radarbase.kotlin.coroutines.CacheConfig
import org.radarbase.kotlin.coroutines.CachedMap
import org.radarbase.ktor.auth.ClientCredentialsConfig
import org.radarbase.ktor.auth.clientCredentials
import org.radarbase.management.client.MPClient
import org.radarbase.management.client.MPProject
import org.radarbase.management.client.MPSubject
import org.radarbase.management.client.mpClient
import org.radarbase.output.path.RecordPathFactory.Companion.getOrNull
import org.radarbase.output.path.RecordPathFactory.Companion.sanitizeId
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

/**
 * Plugin to read values from ManagementPortal. It requires the plugin properties
 * managementPortalUrl, managementPortalClientId and managementPortalClientSecret to be set,
 * or managementPortalUrl in combination with the environment variables MANAGEMENT_PORTAL_CLIENT_ID
 * and MANAGEMENT_PORTAL_CLIENT_SECRET.
 */
class MPPathFormatterPlugin : PathFormatterPlugin.Factory {
    private val supervisorJob = SupervisorJob()
    private val pluginScope = CoroutineScope(Dispatchers.Default + supervisorJob)

    override fun create(
        properties: Map<String, String>,
    ): PathFormatterPlugin = Plugin(properties, pluginScope)

    internal class Plugin(
        properties: Map<String, String>,
        pluginScope: CoroutineScope,
    ) : PathFormatterPlugin() {
        override val name: String = "mp"

        override val allowedFormats: String = setOf(
            "organization",
            "project",
            "user",
            "externalId",
            "group",
            "<attribute>",
            "project:<project-attribute>",
        ).joinToString { ", " }

        private val mpClient: MPClient

        private val cacheConfig = CacheConfig(
            refreshDuration = 10.minutes,
            retryDuration = 10.seconds,
            maxSimultaneousCompute = 2,
        )
        private val projectCache: CachedMap<String, MPProject>
        private val subjectCache: ConcurrentMap<String, CachedMap<String, MPSubject>> = ConcurrentHashMap()

        init {
            val mpUrl = requireNotNull(properties["managementPortalUrl"]) { "Missing managementPortalUrl configuration" }
                .trimEnd('/')

            mpClient = mpClient {
                url = "$mpUrl/"
                auth {
                    clientCredentials(
                        ClientCredentialsConfig(
                            tokenUrl = "$mpUrl/oauth/token",
                            clientId = properties["managementPortalClientId"],
                            clientSecret = properties["managementPortalClientSecret"],
                        ).copyWithEnv(),
                    )
                }
            }

            projectCache = CachedMap(cacheConfig) {
                mpClient.requestProjects().associateBy { it.id }
            }

            pluginScope.launch {
                while (isActive) {
                    delay(30.minutes)
                    subjectCache
                        .filter { it.value.isStale(20.minutes) }
                        .forEach { (key, value) ->
                            subjectCache.remove(key, value)
                        }
                    if (projectCache.isStale()) {
                        projectCache.clear()
                    }
                }
            }
        }

        override fun lookup(parameterContents: String): suspend PathFormatParameters.() -> String =
            when (parameterContents) {
                "organization" -> projectProperty("unknown-organization") {
                    organization?.id
                }
                "project" -> projectProperty("unknown-project") { id }
                "group" -> subjectProperty("default") { group }
                "externalId" -> subjectProperty("unknown-user") { externalId ?: id }
                "userId", "login", "id" -> subjectProperty("unknown-user") { id }
                else -> if (parameterContents.startsWith("project:")) {
                    projectProperty("unknown-$parameterContents") {
                        attributes[parameterContents.removePrefix("project:")]
                    }
                } else {
                    subjectProperty("unknown-$parameterContents") {
                        attributes[parameterContents]
                    }
                }
            }

        private inline fun subjectProperty(
            defaultValue: String,
            crossinline compute: MPSubject.() -> String?,
        ): suspend PathFormatParameters.() -> String = {
            sanitizeId(lookupSubject()?.compute(), defaultValue)
        }

        private suspend fun PathFormatParameters.lookupSubject(): MPSubject? {
            val projectId = key.getOrNull("projectId") ?: return null
            val userId = key.getOrNull("userId") ?: return null

            val cache = subjectCache.computeIfAbsent(projectId.toString()) { projectIdString ->
                CachedMap(cacheConfig) {
                    val subjects = mpClient.requestSubjects(projectIdString)
                    buildMap(subjects.size) {
                        subjects.forEach { subject ->
                            val subjectId = subject.id ?: return@forEach
                            put(subjectId, subject)
                        }
                    }
                }
            }
            return cache.get(userId.toString())
        }

        private inline fun projectProperty(
            defaultValue: String,
            crossinline compute: MPProject.() -> String?,
        ): suspend PathFormatParameters.() -> String = {
            sanitizeId(lookupProject()?.compute(), defaultValue)
        }

        private suspend fun PathFormatParameters.lookupProject(): MPProject? {
            val projectId = key.getOrNull("projectId") ?: return null
            return projectCache.get(projectId.toString())
        }
    }
}
