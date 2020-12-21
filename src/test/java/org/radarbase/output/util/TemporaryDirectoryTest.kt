package org.radarbase.output.util

import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.`is`
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path

internal class TemporaryDirectoryTest {
    @Test
    fun createAndDelete(@TempDir root: Path) {
        TemporaryDirectory(root, "worker-").use {
            assertThat(Files.list(root).count(), `is`(1L))
            Files.createTempFile(it.path, "test", "txt")
            Files.createTempFile(it.path, "test", "txt")

            assertThat(Files.list(it.path).count(), `is`(2L))
            assertThat(Files.list(root).count(), `is`(1L))
        }

        assertThat(Files.list(root).count(), `is`(0L))
    }
}
