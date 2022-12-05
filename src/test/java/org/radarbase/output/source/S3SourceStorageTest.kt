package org.radarbase.output.source

import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.radarbase.output.source.S3SourceStorage.Companion.faultTolerant
import java.io.IOException

class S3SourceStorageTest {
    @Test
    fun testSuspend() {
        assertThrows<IOException> {
            runBlocking {
                launch { throw IOException("ex") }
            }
        }
    }
    @Test
    fun testFaultTolerant() {
        assertThrows<IOException> {
            runBlocking {
                faultTolerant { throw IOException("test") }
            }
        }
    }
}
