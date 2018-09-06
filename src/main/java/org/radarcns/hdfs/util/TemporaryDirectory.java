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

package org.radarcns.hdfs.util;

import static org.radarcns.hdfs.util.ThrowingConsumer.tryCatch;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Temporary directory that will be removed on close or shutdown. */
public class TemporaryDirectory implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(TemporaryDirectory.class);

    private final Thread shutdownHook;
    private final Path path;

    public TemporaryDirectory(Path root, String prefix) throws IOException {
        Files.createDirectories(root);
        path  = Files.createTempDirectory(root, prefix);
        shutdownHook = new Thread(this::doClose,
                "remove-" + path.toString().replaceAll("/", "-"));
        try {
            Runtime.getRuntime().addShutdownHook(shutdownHook);
        } catch (IllegalStateException ex) {
            close();
            throw ex;
        }
    }

    public Path getPath() {
        return this.path;
    }

    private void doClose() {
        try {
            // Ignore errors the first time
            Files.walk(path)
                    .forEach(tryCatch(Files::deleteIfExists, (p, ex) -> {}));
        } catch (IOException ex) {
            // ignore the first time
        }

        try {
            if (Files.exists(path)) {
                try {
                    Thread.sleep(500L);
                } catch (InterruptedException ex) {
                    logger.debug("Waiting for temporary directory deletion interrupted");
                    Thread.currentThread().interrupt();
                }
                Files.walk(path)
                        .forEach(tryCatch(Files::deleteIfExists,
                                (p, ex) -> logger.warn("Cannot delete temporary path {}: {}",
                                        p, ex.toString())));
            }
        } catch (IOException ex) {
            logger.warn("Cannot delete temporary directory {}: {}", path, ex.toString());
        }
    }

    @Override
    public void close() {
        doClose();
        try {
            Runtime.getRuntime().removeShutdownHook(shutdownHook);
        } catch (IllegalStateException ex) {
            // already shutting down
        }
    }
}
