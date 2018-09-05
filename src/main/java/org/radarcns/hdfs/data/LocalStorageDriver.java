/*
 * Copyright 2018 The Hyve
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.radarcns.hdfs.data;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalStorageDriver implements StorageDriver {
    private static final Logger logger = LoggerFactory.getLogger(LocalStorageDriver.class);

    private int uid = -1;
    private int gid = -1;

    @Override
    public void init(Map<String, String> properties) {
        String localUid = properties.get("local-uid");

        if (localUid != null) {
            try {
                this.uid = Integer.parseInt(localUid);
            } catch (NumberFormatException ex) {
                logger.error("Local user ID must be a number");
            }
        }
        String localGid = properties.get("local-gid");
        if (localGid != null) {
            try {
                this.gid = Integer.parseInt(localGid);
            } catch (NumberFormatException ex) {
                logger.error("Local group ID must be a number");
            }
        }
    }

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
        if (uid >= 0) {
            Files.setAttribute(oldPath, "unix:uid", uid);
        }
        if (gid >= 0) {
            Files.setAttribute(oldPath, "unix:gid", gid);
        }
        Files.setPosixFilePermissions(oldPath, PosixFilePermissions.fromString("rw-r--r--"));
        move(oldPath, newPath);
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
