package org.radarcns.hdfs.data;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class GzipCompression implements Compression {
    @Override
    public OutputStream compress(OutputStream out) throws IOException {
        return new GZIPOutputStream(out);
    }

    @Override
    public InputStream decompress(InputStream in) throws IOException {
        return new GZIPInputStream(in);
    }

    @Override
    public Collection<String> getFormats() {
        return Collections.singleton("gzip");
    }

    @Override
    public String getExtension() {
        return ".gz";
    }
}
