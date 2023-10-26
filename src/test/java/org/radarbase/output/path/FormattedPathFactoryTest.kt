package org.radarbase.output.path

import kotlinx.coroutines.runBlocking
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.instanceOf
import org.hamcrest.Matchers.nullValue
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.io.TempDir
import org.radarbase.output.config.LocalConfig
import org.radarbase.output.config.PathConfig
import org.radarbase.output.config.PathFormatterConfig
import org.radarbase.output.target.LocalTargetStorage
import org.radarbase.output.target.TargetManager
import org.radarcns.kafka.ObservationKey
import org.radarcns.passive.phone.PhoneLight
import java.nio.file.Path
import java.time.Instant
import kotlin.reflect.jvm.jvmName

internal class FormattedPathFactoryTest {
    private lateinit var targetStorage: TargetManager

    @BeforeEach
    fun setUp(@TempDir dir: Path) {
        targetStorage = TargetManager(
            "radar-output-storage",
            LocalTargetStorage(dir, LocalConfig()),
        )
    }

    @Test
    fun testFormat() = runBlocking {
        val factory = createFactory(
            format = "\${topic}/\${projectId}/\${userId}/\${sourceId}/\${time:yyyyMM}/\${time:dd}/\${filename}",
        )

        val t = Instant.parse("2021-01-02T10:05:00Z")

        val path = factory.relativePath(
            PathFormatParameters(
                topic = "t",
                key = ObservationKey(
                    "p",
                    "u",
                    "s",
                ),
                value = PhoneLight(
                    t.epochSecond.toDouble(),
                    t.epochSecond.toDouble(),
                    1.0f,
                ),
                time = t,
                attempt = 0,
            ),
        )

        assertEquals("t/p/u/s/202101/02/20210102_1000.csv.gz", path)
    }

    @Test
    fun unparameterized() = runBlocking {
        val factory = FormattedPathFactory().apply {
            init(
                targetManager = targetStorage,
                extension = ".csv.gz",
                config = PathConfig(),
            )
        }
        val t = Instant.parse("2021-01-02T10:05:00Z")
        val path = factory.relativePath(
            PathFormatParameters(
                topic = "t",
                key = ObservationKey(
                    "p",
                    "u",
                    "s",
                ),
                value = PhoneLight(
                    t.epochSecond.toDouble(),
                    t.epochSecond.toDouble(),
                    1.0f,
                ),
                time = t,
                attempt = 0,
            ),
        )
        assertEquals("p/u/t/20210102_1000.csv.gz", path)
    }

    @Test
    fun testMissingTopic() {
        assertThrows<IllegalArgumentException> {
            createFactory("\${projectId}/\${userId}/\${sourceId}/\${time:yyyyMM}/\${time:dd}/\${filename}")
        }
    }

    @Test
    fun testMissingFilename() {
        assertThrows<IllegalArgumentException> {
            createFactory("\${topic}/\${projectId}/\${userId}/\${sourceId}/\${time:yyyyMM}/\${time:dd}")
        }
    }

    @Test
    fun testUnknownParameter() {
        assertThrows<IllegalArgumentException> {
            createFactory("\${topic}/\${projectId}/\${userId}/\${sourceId}/\${time:yyyyMM}/\${time:dd}/\${filename}\${unknown}")
        }
    }

    @Test
    fun testAttemptAndExtensionPresent() {
        createFactory("\${topic}/\${projectId}/\${userId}/\${sourceId}/\${time:yyyyMM}/\${time:dd}/\${attempt}\${extension}")
        assertThrows<IllegalArgumentException> {
            createFactory("\${topic}/\${projectId}/\${userId}/\${sourceId}/\${time:yyyyMM}/\${time:dd}/\${attempt}")
        }
        assertThrows<IllegalArgumentException> {
            createFactory("\${topic}/\${projectId}/\${userId}/\${sourceId}/\${time:yyyyMM}/\${time:dd}/\${extension}")
        }
    }

    private fun createFactory(format: String): FormattedPathFactory = FormattedPathFactory().apply {
        init(
            targetManager = targetStorage,
            extension = ".csv.gz",
            config = PathConfig(
                path = PathFormatterConfig(
                    format = format,
                ),
            ),
        )
    }

    @Test
    fun testNamedPluginCreate() {
        assertThat("fixed".toPathFormatterPlugin(emptyMap()), instanceOf(PathFormatterPlugin::class.java))
        assertThat("time".toPathFormatterPlugin(emptyMap()), instanceOf(PathFormatterPlugin::class.java))
        assertThat("key".toPathFormatterPlugin(emptyMap()), instanceOf(PathFormatterPlugin::class.java))
        assertThat("value".toPathFormatterPlugin(emptyMap()), instanceOf(PathFormatterPlugin::class.java))
    }

    @Test
    fun testBadPluginCreate() {
        assertThat("unknown".toPathFormatterPlugin(emptyMap()), nullValue())
    }

    @Test
    fun testClassPathPluginCreate() {
        assertThat(
            FixedPathFormatterPlugin::class.jvmName.toPathFormatterPlugin(emptyMap()),
            instanceOf(PathFormatterPlugin::class.java),
        )
    }
}
