package org.radarcns.hdfs.data;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Collections;

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
        return Collections.singleton("identity");
    }

    @Override
    public String getExtension() {
        return "";
    }
}
