package org.radarcns.hdfs.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * File writer where data is written in a separate thread with a timeout.
 */
public abstract class PostponedWriter implements Closeable, Flushable {
    private static final Logger logger = LoggerFactory.getLogger(PostponedWriter.class);

    private final ScheduledExecutorService executor;
    private final long timeout;
    private final TimeUnit timeoutUnit;
    private final String name;
    private final AtomicReference<Future<?>> writeFuture;
    private Path tempDir;

    /**
     * Constructor with timeout.
     * @param name thread name.
     * @param timeout maximum time between triggering a write and actually writing the file.
     * @param timeoutUnit timeout unit.
     */
    public PostponedWriter(String name, long timeout, TimeUnit timeoutUnit) {
        this.name = name;
        executor = Executors.newScheduledThreadPool(1, r -> new Thread(r, name));
        this.timeout = timeout;
        this.timeoutUnit = timeoutUnit;
        writeFuture = new AtomicReference<>(null);
    }

    /**
     * Trigger a write to occur within set timeout. If a write was already triggered earlier but has
     * not yet taken place, the write will occur earlier.
     */
    public void triggerWrite() {
        Future<?> localWriteFuture = writeFuture.get();
        if (localWriteFuture == null) {
            localWriteFuture = executor.schedule(this::startWrite, timeout, timeoutUnit);
            if (!writeFuture.compareAndSet(null, localWriteFuture)) {
                localWriteFuture.cancel(false);
            }
        }
    }

    /** Start the write in the writer thread. */
    protected void startWrite() {
        writeFuture.set(null);
        doWrite();
    }

    /** Perform the write. */
    protected abstract void doWrite();

    @Override
    public void close() throws IOException {
        doFlush(true);
        try {
            executor.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error("Failed to write {} data: interrupted", name);
        }
    }

    private void doFlush(boolean shutdown) throws IOException {
        Future<?> localFuture = executor.submit(this::startWrite);
        Future<?> oldFuture = writeFuture.getAndUpdate(f -> localFuture);
        if (oldFuture != null) {
            oldFuture.cancel(false);
        }
        if (shutdown) {
            executor.shutdown();
        }

        try {
            localFuture.get(30, TimeUnit.SECONDS);
        } catch (ExecutionException ex) {
            logger.error("Failed to write data for {}", name, ex.getCause());
            throw new IOException("Failed to write data", ex.getCause());
        } catch (InterruptedException | TimeoutException e) {
            logger.error("Failed to write {} data: timeout", name);
        }
    }

    @Override
    public void flush() throws IOException {
        doFlush(false);
    }

    /** Create temporary file to write to. */
    protected Path createTempFile(String prefix, String suffix) throws IOException {
        if (tempDir == null) {
            return Files.createTempFile(prefix, suffix);
        } else {
            return Files.createTempFile(tempDir, prefix, suffix);
        }
    }

    /** Directory to write temporary files in. */
    public void setTempDir(Path tempDir) {
        this.tempDir = tempDir;
    }
}
