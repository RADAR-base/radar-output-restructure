package org.radarbase.output.path

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.radarcns.kafka.ObservationKey
import org.radarcns.passive.phone.PhoneLight
import java.nio.file.Paths
import java.time.Instant

internal class FormattedPathFactoryTest {
    @Test
    fun testFormat() {
        val factory = createFactory(
            format = "\${topic}/\${projectId}/\${userId}/\${sourceId}/\${time:yyyyMM}/\${time:dd}/\${filename}"
        )

        val t = Instant.parse("2021-01-02T10:05:00Z")

        val path = factory.getRelativePath(
            "t",
            ObservationKey(
                "p",
                "u",
                "s",
            ),
            PhoneLight(
                t.epochSecond.toDouble(),
                t.epochSecond.toDouble(),
                1.0f,
            ),
            t,
            0,
        )

        assertEquals(Paths.get("t/p/u/s/202101/02/20210102_1000.csv.gz"), path)
    }

    @Test
    fun unparameterized() {
        val factory = FormattedPathFactory().apply {
            init(emptyMap())
            extension = ".csv.gz"
        }
        val t = Instant.parse("2021-01-02T10:05:00Z")
        val path = factory.getRelativePath(
            "t",
            ObservationKey(
                "p",
                "u",
                "s",
            ),
            PhoneLight(
                t.epochSecond.toDouble(),
                t.epochSecond.toDouble(),
                1.0f,
            ),
            t,
            0,
        )
        assertEquals(Paths.get("p/u/t/20210102_1000.csv.gz"), path)
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
            mapOf("format" to format),
        )
        extension = ".csv.gz"
    }
}
