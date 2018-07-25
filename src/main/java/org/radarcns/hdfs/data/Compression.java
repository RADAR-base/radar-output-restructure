package org.radarcns.hdfs.data;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface Compression extends Format {
    OutputStream compress(OutputStream out) throws IOException;
    InputStream decompress(InputStream in) throws IOException;

    String getExtension();
}
