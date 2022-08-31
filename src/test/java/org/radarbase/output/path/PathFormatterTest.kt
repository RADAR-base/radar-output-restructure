package org.radarbase.output.path

import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.equalTo
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.radarcns.kafka.ObservationKey
import org.radarcns.monitor.application.ApplicationServerStatus
import org.radarcns.monitor.application.ServerStatus
import java.nio.file.Paths
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

internal class PathFormatterTest {
    lateinit var params: PathFormatParameters

    @BeforeEach
    fun setupRecord() {
        val defaultTimeFormat = DateTimeFormatter.ofPattern("YYYYMMdd_HH'00'")
            .withZone(ZoneOffset.UTC)

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
            extension = ".csv",
            computeTimeBin = { t -> if (t != null) defaultTimeFormat.format(t) else "unknown-time" }
        )
    }

    @Test
    fun testDefaultPath() {
        val formatter = PathFormatter(
            format = checkNotNull(FormattedPathFactory.Companion.DEFAULTS["format"]),
            plugins = listOf(
                FixedPathFormatterPlugin(),
                TimePathFormatterPlugin(),
                KeyPathFormatterPlugin(),
                ValuePathFormatterPlugin(),
            )
        )
        assertThat(formatter.format(params), equalTo(Paths.get("p/u/my_topic/19700101_0000.csv")))
    }

    @Test
    fun testDefaultPathFewerPlugins() {
        val formatter = PathFormatter(
            format = checkNotNull(FormattedPathFactory.Companion.DEFAULTS["format"]),
            plugins = listOf(
                FixedPathFormatterPlugin(),
            )
        )
        assertThat(formatter.format(params), equalTo(Paths.get("p/u/my_topic/19700101_0000.csv")))
    }

    @Test
    fun testDefaultPathNoTime() {
        val formatter = PathFormatter(
            format = checkNotNull(FormattedPathFactory.Companion.DEFAULTS["format"]),
            plugins = listOf(
                FixedPathFormatterPlugin(),
            )
        )
        assertThat(formatter.format(params.copy(time = null)), equalTo(Paths.get("p/u/my_topic/unknown-time.csv")))
    }

    @Test
    fun testDefaultPathWrongPlugins() {
        assertThrows(IllegalArgumentException::class.java) {
            PathFormatter(
                format = checkNotNull(FormattedPathFactory.Companion.DEFAULTS["format"]),
                plugins = listOf(
                    TimePathFormatterPlugin(),
                    KeyPathFormatterPlugin(),
                    ValuePathFormatterPlugin(),
                )
            )
        }
    }

    @Test
    fun testCorrectTimeFormatPlugins() {
        val formatter = PathFormatter(
            format = "\${topic}/\${time:YYYY-MM-dd_HH:mm:ss}\${attempt}\${extension}",
            plugins = listOf(
                FixedPathFormatterPlugin(),
                TimePathFormatterPlugin(),
            ),
        )
        assertThat(formatter.format(params), equalTo(Paths.get("my_topic/1970-01-01_000001.csv")))
    }

    @Test
    fun testBadTimeFormatPlugins() {
        assertThrows(IllegalArgumentException::class.java) {
            PathFormatter(
                format = "\${topic}/\${time:VVV}\${attempt}\${extension}",
                plugins = listOf(
                    FixedPathFormatterPlugin(),
                    TimePathFormatterPlugin(),
                )
            )
        }
    }

    @Test
    fun testCorrectKeyFormat() {
        val formatter = PathFormatter(
            format = "\${topic}/\${key:projectId}\${attempt}\${extension}",
            plugins = listOf(
                FixedPathFormatterPlugin(),
                KeyPathFormatterPlugin(),
            )
        )

        assertThat(formatter.format(params), equalTo(Paths.get("my_topic/p.csv")))
    }

    @Test
    fun testUnknownKeyFormat() {
        val formatter = PathFormatter(
            format = "\${topic}/\${key:doesNotExist}\${attempt}\${extension}",
            plugins = listOf(
                FixedPathFormatterPlugin(),
                KeyPathFormatterPlugin(),
            )
        )

        assertThat(formatter.format(params), equalTo(Paths.get("my_topic/unknown-key.csv")))
    }

    @Test
    fun testCorrectValueFormat() {
        val formatter = PathFormatter(
            format = "\${topic}/\${value:serverStatus}\${attempt}\${extension}",
            plugins = listOf(
                FixedPathFormatterPlugin(),
                ValuePathFormatterPlugin(),
            )
        )

        assertThat(formatter.format(params), equalTo(Paths.get("my_topic/CONNECTED.csv")))
    }
}
