package org.radarcns.hdfs.data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.radarcns.hdfs.util.ThrowingConsumer.tryCatch;

/** Temporary directory that will be removed on close or shutdown. */
public class TemporaryDirectory implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(TemporaryDirectory.class);

    private final Thread shutdownHook;
    private final Path path;

    public TemporaryDirectory(Path root, String prefix) throws IOException {
        Files.createDirectories(root);
        path = Files.createTempDirectory(root, prefix);
        shutdownHook = new Thread(this::doClose,
                "remove-" + path.toString().replaceAll("/", "-"));
        Runtime.getRuntime().addShutdownHook(shutdownHook);
    }

    public Path getPath() {
        return this.path;
    }

    private void doClose() {
        try {
            Files.walk(path)
                    .forEach(tryCatch(Files::deleteIfExists,
                            (p, ex) -> logger.warn("Cannot delete temporary path {}: {}", p, ex)));
        } catch (IOException ex) {
            logger.warn("Cannot delete temporary directory {}: {}", path, ex);
        }
    }

    public void close() {
        doClose();
        Runtime.getRuntime().removeShutdownHook(shutdownHook);
    }
}
