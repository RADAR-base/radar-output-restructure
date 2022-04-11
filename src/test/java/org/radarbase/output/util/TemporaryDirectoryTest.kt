package org.radarbase.output.util

import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.`is`
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path
import kotlin.io.path.createTempFile
import kotlin.io.path.listDirectoryEntries

internal class TemporaryDirectoryTest {
    @Test
    fun createAndDelete(@TempDir root: Path) {
        TemporaryDirectory(root, "worker-").use {
            assertThat(root.listDirectoryEntries().size, `is`(1))
            createTempFile(it.path, "test", "txt")
            createTempFile(it.path, "test", "txt")

            assertThat(it.path.listDirectoryEntries().size, `is`(2))
            assertThat(root.listDirectoryEntries().size, `is`(1))
        }

        assertThat(root.listDirectoryEntries().size, `is`(0))
    }
}
