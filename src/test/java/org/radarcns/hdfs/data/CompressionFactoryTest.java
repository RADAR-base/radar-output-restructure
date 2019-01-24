package org.radarcns.hdfs.data;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CompressionFactoryTest {

    private CompressionFactory factory;

    @BeforeEach
    public void setUp() {
        factory = new CompressionFactory();
    }

    @Test
    public void testIdentity() throws IOException {
        assertSame(IdentityCompression.class, factory.get("none").getClass());
        assertSame(IdentityCompression.class, factory.get("identity").getClass());

        Compression compression = factory.get("none");

        assertEquals("", compression.getExtension());

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        OutputStream compressedStream = compression.compress("something", out);
        compressedStream.write("abcdef".getBytes(US_ASCII));
        compressedStream.close();

        assertEquals("abcdef", new String(out.toByteArray(), US_ASCII));

        ByteArrayInputStream in = new ByteArrayInputStream("abcdef".getBytes(US_ASCII));
        InputStream compressedIn = compression.decompress(in);
        byte[] result = new byte[10];
        assertEquals(6, compressedIn.read(result));
        assertEquals("abcdef", new String(result, 0, 6, US_ASCII));
    }

    @Test
    public void testGzip() throws IOException {
        assertSame(GzipCompression.class, factory.get("gzip").getClass());

        Compression compression = factory.get("gzip");

        assertEquals(".gz", compression.getExtension());

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        OutputStream compressedStream = compression.compress("something", out);
        compressedStream.write("abcdef".getBytes(US_ASCII));
        compressedStream.close();

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        InputStream compressedIn = compression.decompress(in);
        byte[] result = new byte[10];
        assertEquals(6, compressedIn.read(result));
        assertEquals("abcdef", new String(result, 0, 6, US_ASCII));
    }


    @Test
    public void testZip() throws IOException {
        assertSame(ZipCompression.class, factory.get("zip").getClass());

        Compression compression = factory.get("zip");

        assertEquals(".zip", compression.getExtension());

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        OutputStream compressedStream = compression.compress("something", out);
        compressedStream.write("abcdef".getBytes(US_ASCII));
        compressedStream.close();

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        InputStream compressedIn = compression.decompress(in);
        byte[] result = new byte[10];
        assertEquals(6, compressedIn.read(result));
        assertEquals("abcdef", new String(result, 0, 6, US_ASCII));
    }
}
