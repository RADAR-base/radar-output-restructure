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

public abstract class PostponedWriter implements Closeable, Flushable {
    private static final Logger logger = LoggerFactory.getLogger(PostponedWriter.class);

    private final ScheduledExecutorService executor;
    private final long timeout;
    private final TimeUnit timeoutUnit;
    private final String name;
    private final AtomicReference<Future<?>> writeFuture;
    private Path tempDir;

    public PostponedWriter(String name, long timeout, TimeUnit timeoutUnit) {
        this.name = name;
        executor = Executors.newScheduledThreadPool(1, r -> new Thread(r, name));
        this.timeout = timeout;
        this.timeoutUnit = timeoutUnit;
        writeFuture = new AtomicReference<>(null);
    }

    public void triggerWrite() {
        Future<?> localWriteFuture = writeFuture.get();
        if (localWriteFuture == null) {
            localWriteFuture = executor.schedule(this::startWrite, timeout, timeoutUnit);
            if (!writeFuture.compareAndSet(null, localWriteFuture)) {
                localWriteFuture.cancel(false);
            }
        }
    }

    protected void startWrite() {
        writeFuture.set(null);
        doWrite();
    }

    protected abstract void doWrite();

    @Override
    public void close() throws IOException {
        doFlush(true);
        try {
            executor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error("Failed to write data {}", name, e);
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
            localFuture.get(5, TimeUnit.SECONDS);
        } catch (ExecutionException ex) {
            logger.error("Failed to write data", ex);
            throw new IOException("Failed to write data", ex.getCause());
        } catch (InterruptedException | TimeoutException e) {
            logger.error("Failed to write data: timeout");
        }
    }

    @Override
    public void flush() throws IOException {
        doFlush(false);
    }

    protected Path createTempFile(String prefix, String suffix) throws IOException {
        return Files.createTempFile(tempDir, prefix, suffix);
    }

    public void setTempDir(Path tempDir) {
        this.tempDir = tempDir;
    }
}
