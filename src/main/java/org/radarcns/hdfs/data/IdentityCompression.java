package org.radarcns.hdfs.data;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collection;

public class IdentityCompression implements Compression {
    @Override
    public OutputStream compress(OutputStream out) {
        return out;
    }

    @Override
    public InputStream decompress(InputStream in) {
        return in;
    }

    @Override
    public Collection<String> getFormats() {
        return Arrays.asList("identity", "none");
    }

    @Override
    public String getExtension() {
        return "";
    }
}
