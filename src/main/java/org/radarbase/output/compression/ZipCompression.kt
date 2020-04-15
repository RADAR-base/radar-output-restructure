package org.radarbase.output.compression

import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.util.zip.ZipEntry
import java.util.zip.ZipInputStream
import java.util.zip.ZipOutputStream

class ZipCompression : Compression {

    override val formats = setOf("zip")

    override val extension = ".zip"

    @Throws(IOException::class)
    override fun compress(fileName: String, out: OutputStream) = ZipOutputStream(out).apply {
        setLevel(7)
        putNextEntry(ZipEntry(fileName))
    }

    @Throws(IOException::class)
    override fun decompress(`in`: InputStream): InputStream = ZipInputStream(`in`).apply {
        nextEntry
    }
}
