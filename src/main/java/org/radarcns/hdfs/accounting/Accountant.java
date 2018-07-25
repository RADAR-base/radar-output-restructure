package org.radarcns.hdfs.accounting;

import org.radarcns.hdfs.FileStoreFactory;
import org.radarcns.hdfs.config.RestructureSettings;
import org.radarcns.hdfs.data.StorageDriver;
import org.radarcns.hdfs.util.DirectFunctionalValue;
import org.radarcns.hdfs.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import static org.radarcns.hdfs.util.ThrowingConsumer.tryCatch;

public class Accountant implements Flushable, Closeable {
    private static final Logger logger = LoggerFactory.getLogger(Accountant.class);

    private static final java.nio.file.Path BINS_FILE_NAME = Paths.get("bins.csv");
    private static final java.nio.file.Path OFFSETS_FILE_NAME = Paths.get("offsets.csv");
    private final OffsetRangeFile offsetFile;
    private final BinFile binFile;
    private final Path tempDir;

    public Accountant(FileStoreFactory factory) throws IOException {
        StorageDriver storage = factory.getStorageDriver();
        RestructureSettings settings = factory.getSettings();

        tempDir = Files.createTempDirectory(settings.getTempDir(), "accounting");
        this.offsetFile = OffsetRangeFile.read(storage, settings.getOutputPath().resolve(OFFSETS_FILE_NAME));
        this.offsetFile.setTempDir(tempDir);
        this.binFile = BinFile.read(storage, settings.getOutputPath().resolve(BINS_FILE_NAME));
        this.binFile.setTempDir(tempDir);
    }

    public void process(Ledger ledger) {
        long timeProcess = System.nanoTime();
        binFile.putAll(ledger.bins);
        binFile.triggerWrite();
        offsetFile.addAll(ledger.offsets);
        offsetFile.triggerWrite();
        Timer.getInstance().add("accounting.process", timeProcess);
    }

    @Override
    public void close() throws IOException {
        long timeClose = System.nanoTime();
        IOException exception = null;
        try {
            binFile.close();
        } catch (IOException ex) {
            logger.error("Failed to close bins", ex);
            exception = ex;
        }

        try {
            offsetFile.close();
        } catch (IOException ex) {
            logger.error("Failed to close offsets", ex);
            exception = ex;
        }

        Files.walk(tempDir)
                .forEach(tryCatch(Files::deleteIfExists,
                        (f, ex) -> logger.error("Failed to delete temporary directory")));

        if (exception != null) {
            throw exception;
        }
        Timer.getInstance().add("accounting.close", timeClose);
    }

    public OffsetRangeSet getOffsets() {
        return offsetFile.getOffsets();
    }

    @Override
    public void flush() throws IOException {
        long timeFlush = System.nanoTime();
        IOException exception = null;
        try {
            binFile.flush();
        } catch (IOException ex) {
            logger.error("Failed to close bins", ex);
            exception = ex;
        }

        try {
            offsetFile.flush();
        } catch (IOException ex) {
            logger.error("Failed to close offsets", ex);
            exception = ex;
        }

        if (exception != null) {
            throw exception;
        }
        Timer.getInstance().add("accounting.flush", timeFlush);
    }

    public BinFile getBins() {
        return binFile;
    }

    public static class Ledger {
        private final OffsetRangeSet offsets;
        private final Map<Bin, Long> bins;

        public Ledger() {
            offsets = new OffsetRangeSet(() -> new DirectFunctionalValue<>(new TreeSet<>()));
            bins = new HashMap<>();
        }

        public void add(Transaction transaction) {
            long timeAdd = System.nanoTime();
            offsets.add(transaction.offset);
            bins.compute(transaction.bin, (b, vOld) -> vOld == null ? 1L : vOld + 1L);
            Timer.getInstance().add("accounting.add", timeAdd);
        }
    }

    public static class Transaction {
        private final OffsetRange offset;
        private final Bin bin;

        public Transaction(OffsetRange offset, Bin bin) {
            this.offset = offset;
            this.bin = bin;
        }
    }
}
