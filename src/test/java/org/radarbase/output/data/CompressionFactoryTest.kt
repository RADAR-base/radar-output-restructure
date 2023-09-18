package org.radarbase.output.data

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.radarbase.output.compression.Compression
import org.radarbase.output.compression.CompressionFactory
import org.radarbase.output.compression.GzipCompression
import org.radarbase.output.compression.IdentityCompression
import org.radarbase.output.compression.ZipCompression
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.nio.charset.StandardCharsets.US_ASCII

class CompressionFactoryTest {

    private lateinit var factory: CompressionFactory

    @BeforeEach
    fun setUp() {
        factory = CompressionFactory()
    }

    @Test
    @Throws(IOException::class)
    fun testIdentity() {
        assertSame(IdentityCompression::class.java, factory["none"].javaClass)
        assertSame(IdentityCompression::class.java, factory["identity"].javaClass)

        val compression = factory["none"]

        assertEquals("", compression.extension)

        val out = ByteArrayOutputStream()
        val compressedStream = compression.compress("something", out)
        compressedStream.write("abcdef".toByteArray(US_ASCII))
        compressedStream.close()

        assertEquals("abcdef", String(out.toByteArray(), US_ASCII))

        val `in` = ByteArrayInputStream("abcdef".toByteArray(US_ASCII))
        val compressedIn = compression.decompress(`in`)
        val result = ByteArray(10)
        assertEquals(6, compressedIn.read(result))
        assertEquals("abcdef", String(result, 0, 6, US_ASCII))
    }

    @Test
    @Throws(IOException::class)
    fun testGzip() {
        assertSame(GzipCompression::class.java, factory["gzip"].javaClass)

        val compression = factory["gzip"]

        assertEquals(".gz", compression.extension)
        testReCompression(compression)
    }

    private fun testReCompression(compression: Compression) {
        val compressed = ByteArrayOutputStream().use { out ->
            compression.compress("something", out).use {
                it.write("abcdef".toByteArray(US_ASCII))
            }
            out.toByteArray()
        }

        val result = ByteArray(10)
        val numBytes = ByteArrayInputStream(compressed).use { `in` ->
            compression.decompress(`in`).use {
                it.read(result)
            }
        }
        assertEquals(6, numBytes)
        assertEquals("abcdef", String(result, 0, 6, US_ASCII))
    }

    @Test
    @Throws(IOException::class)
    fun testZip() {
        assertSame(ZipCompression::class.java, factory["zip"].javaClass)

        val compression = factory["zip"]

        assertEquals(".zip", compression.extension)

        testReCompression(compression)
    }
}
