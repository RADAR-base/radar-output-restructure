package org.radarcns.hdfs.config;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.radarcns.hdfs.Application.CACHE_SIZE_DEFAULT;
import static org.radarcns.hdfs.util.commandline.CommandLineArgs.nonNullOrDefault;

public class RestructureSettings {
    private final String compression;
    private final boolean deduplicate;
    private final String format;
    private final int cacheSize;
    private final Path tempDir;
    private final Path outputPath;
    private final int numThreads;

    private RestructureSettings(Builder builder) {
        this.compression = builder.compression;
        this.deduplicate = builder.doDeduplicate;
        this.format = builder.format;
        this.cacheSize = builder.cacheSize;
        this.tempDir = builder.tempDir;
        this.outputPath = builder.outputPath;
        this.numThreads = builder.numThreads;
    }

    public String getCompression() {
        return compression;
    }

    public boolean isDeduplicate() {
        return deduplicate;
    }

    public String getFormat() {
        return format;
    }

    public int getCacheSize() {
        return cacheSize;
    }

    public Path getTempDir() {
        return tempDir;
    }

    public Path getOutputPath() {
        return outputPath;
    }

    public int getNumThreads() {
        return this.numThreads;
    }

    public static class Builder {
        private int numThreads = 1;
        private String compression;
        private boolean doDeduplicate;
        private String format;
        private int cacheSize = CACHE_SIZE_DEFAULT;
        private Path tempDir;
        private final Path outputPath;

        public Builder(String outputPath) {
            this.outputPath = Paths.get(outputPath.replaceAll("/+$", ""));
        }

        public Builder compression(final String compression) {
            this.compression = compression;
            return this;
        }

        public Builder doDeduplicate(final boolean dedup) {
            this.doDeduplicate = dedup;
            return this;
        }

        public Builder format(final String format) {
            this.format = format;
            return this;
        }

        public Builder cacheSize(Integer size) {
            if (size == null) {
                return this;
            }
            if (size < 1) {
                throw new IllegalArgumentException("Cannot use cache size smaller than 1");
            }
            cacheSize = size;
            return this;
        }

        public Builder tempDir(String tmpDir) {
            this.tempDir = Paths.get(tmpDir);
            return this;
        }

        public Builder numThreads(int num) {
            if (num < 1) {
                throw new IllegalArgumentException("Number of threads must be at least 1");
            }
            this.numThreads = num;
            return this;
        }

        public RestructureSettings build() {
            compression = nonNullOrDefault(compression, () -> "identity");
            format = nonNullOrDefault(format, () -> "csv");
            tempDir = nonNullOrDefault(tempDir, () -> {
                try {
                    return Files.createTempDirectory("radar-hdfs-restructure");
                } catch (IOException ex) {
                    throw new IllegalStateException("Cannot create temporary directory");
                }
            });
            return new RestructureSettings(this);
        }
    }
}
