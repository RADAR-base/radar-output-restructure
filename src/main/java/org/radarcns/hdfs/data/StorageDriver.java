package org.radarcns.hdfs.data;

import org.radarcns.hdfs.Plugin;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;

public interface StorageDriver extends Plugin {
    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    boolean exists(Path path);
    InputStream newInputStream(Path path) throws IOException;
    void move(Path oldPath, Path newPath) throws IOException;
    void store(Path localPath, Path newPath) throws IOException;
    long size(Path path) throws IOException;

    /**
     * Reads all bytes from an input stream and writes them to an output stream.
     */
    static void copy(InputStream source, OutputStream sink) throws IOException {
        byte[] buf = new byte[8196];
        int n;
        while ((n = source.read(buf)) > 0) {
            sink.write(buf, 0, n);
        }
    }
}
