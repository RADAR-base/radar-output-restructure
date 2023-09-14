package org.radarbase.output.path

import kotlinx.coroutines.runBlocking
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.equalTo
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.radarbase.output.config.PathFormatterConfig.Companion.DEFAULT_FORMAT
import org.radarcns.kafka.ObservationKey
import org.radarcns.monitor.application.ApplicationServerStatus
import org.radarcns.monitor.application.ServerStatus
import java.time.Instant

internal class PathFormatterTest {
    private lateinit var fixedPlugin: PathFormatterPlugin
    private lateinit var params: PathFormatParameters

    @BeforeEach
    fun setupRecord() {
        params = PathFormatParameters(
            topic = "my_topic",
            key = ObservationKey(
                "p",
                "u",
                "s",
            ),
            value = ApplicationServerStatus(
                1.0,
                ServerStatus.CONNECTED,
                "1.1.1.1",
            ),
            time = Instant.ofEpochMilli(1000),
            attempt = 0,
        )
        fixedPlugin = FixedPathFormatterPlugin().create(mapOf("extension" to ".csv"))
    }

    @Test
    fun testDefaultPath() = runBlocking {
        val formatter = PathFormatter(
            format = DEFAULT_FORMAT,
            plugins = listOf(
                fixedPlugin,
                TimePathFormatterPlugin(),
                KeyPathFormatterPlugin(),
                ValuePathFormatterPlugin(),
            ),
        )
        assertThat(formatter.format(params), equalTo("p/u/my_topic/19700101_0000.csv"))
    }

    @Test
    fun testDefaultPathFewerPlugins() = runBlocking {
        val formatter = PathFormatter(
            format = DEFAULT_FORMAT,
            plugins = listOf(
                fixedPlugin,
            ),
        )
        assertThat(formatter.format(params), equalTo("p/u/my_topic/19700101_0000.csv"))
    }

    @Test
    fun testDefaultPathNoTime() = runBlocking {
        val formatter = PathFormatter(
            format = DEFAULT_FORMAT,
            plugins = listOf(
                fixedPlugin,
            ),
        )
        assertThat(formatter.format(params.copy(time = null)), equalTo("p/u/my_topic/unknown-time.csv"))
    }

    @Test
    fun testDefaultPathWrongPlugins() {
        assertThrows(IllegalArgumentException::class.java) {
            PathFormatter(
                format = DEFAULT_FORMAT,
                plugins = listOf(
                    TimePathFormatterPlugin(),
                    KeyPathFormatterPlugin(),
                    ValuePathFormatterPlugin(),
                ),
            )
        }
    }

    @Test
    fun testCorrectTimeFormatPlugins() = runBlocking {
        val formatter = PathFormatter(
            format = "\${topic}/\${time:YYYY-MM-dd_HH:mm:ss}\${attempt}\${extension}",
            plugins = listOf(
                fixedPlugin,
                TimePathFormatterPlugin(),
            ),
        )
        assertThat(formatter.format(params), equalTo("my_topic/1970-01-01_000001.csv"))
    }

    @Test
    fun testBadTimeFormatPlugins(): Unit = runBlocking {
        assertThrows(IllegalArgumentException::class.java) {
            PathFormatter(
                format = "\${topic}/\${time:VVV}\${attempt}\${extension}",
                plugins = listOf(
                    fixedPlugin,
                    TimePathFormatterPlugin(),
                ),
            )
        }
    }

    @Test
    fun testCorrectKeyFormat() = runBlocking {
        val formatter = PathFormatter(
            format = "\${topic}/\${key:projectId}\${attempt}\${extension}",
            plugins = listOf(
                fixedPlugin,
                KeyPathFormatterPlugin(),
            ),
        )

        assertThat(formatter.format(params), equalTo("my_topic/p.csv"))
    }

    @Test
    fun testUnknownKeyFormat() = runBlocking {
        val formatter = PathFormatter(
            format = "\${topic}/\${key:doesNotExist}\${attempt}\${extension}",
            plugins = listOf(
                fixedPlugin,
                KeyPathFormatterPlugin(),
            ),
        )

        assertThat(formatter.format(params), equalTo("my_topic/unknown-key.csv"))
    }

    @Test
    fun testCorrectValueFormat() = runBlocking {
        val formatter = PathFormatter(
            format = "\${topic}/\${value:serverStatus}\${attempt}\${extension}",
            plugins = listOf(
                fixedPlugin,
                ValuePathFormatterPlugin(),
            ),
        )

        assertThat(formatter.format(params), equalTo("my_topic/CONNECTED.csv"))
    }
}
