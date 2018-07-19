package org.radarcns.hdfs.data;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;

public class LocalStorageDriver implements StorageDriver {
    @Override
    public boolean exists(Path path) {
        return Files.exists(path);
    }

    @Override
    public InputStream newInputStream(Path path) throws IOException {
        return Files.newInputStream(path);
    }

    @Override
    public OutputStream newOutputStream(Path path, boolean append) throws IOException {
        if (append) {
            return Files.newOutputStream(path, APPEND, CREATE);
        } else {
            return Files.newOutputStream(path);
        }
    }

    @Override
    public void move(Path oldPath, Path newPath) throws IOException {
        Files.move(oldPath, newPath, REPLACE_EXISTING);
    }

    @Override
    public void store(Path oldPath, Path newPath) throws IOException {
        move(oldPath, newPath);
    }

    @Override
    public long size(Path path) throws IOException {
        return Files.size(path);
    }
}
