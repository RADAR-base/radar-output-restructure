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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.nio.file.Path;
import org.radarcns.hdfs.Plugin;

public interface StorageDriver extends Plugin {
    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    boolean exists(Path path);
    InputStream newInputStream(Path path) throws IOException;
    OutputStream newOutputStream(Path path, boolean append) throws IOException;

    void move(Path oldPath, Path newPath) throws IOException;
    void store(Path localPath, Path newPath) throws IOException;
    long size(Path path) throws IOException;
    void delete(Path path) throws IOException;

    default BufferedReader newBufferedReader(Path path) throws IOException {
        Reader reader = new InputStreamReader(newInputStream(path));
        return new BufferedReader(reader);
    }

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
