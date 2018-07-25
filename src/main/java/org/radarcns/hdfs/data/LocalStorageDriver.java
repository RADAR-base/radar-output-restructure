package org.radarcns.hdfs.data;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.COPY_ATTRIBUTES;
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
        try {
            Files.move(oldPath, newPath, REPLACE_EXISTING, ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException ex) {
            Files.move(oldPath, newPath, REPLACE_EXISTING);
        }
    }

    @Override
    public void store(Path oldPath, Path newPath) throws IOException {
        move(oldPath, newPath);
        Files.setPosixFilePermissions(newPath, PosixFilePermissions.fromString("rw-r--r--"));
    }

    @Override
    public long size(Path path) throws IOException {
        return Files.size(path);
    }

    @Override
    public void delete(Path path) throws IOException {
        Files.delete(path);
    }
}
