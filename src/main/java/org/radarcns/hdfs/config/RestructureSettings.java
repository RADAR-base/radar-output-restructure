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

package org.radarcns.hdfs.config;

import static org.radarcns.hdfs.Application.CACHE_SIZE_DEFAULT;
import static org.radarcns.hdfs.util.commandline.CommandLineArgs.nonNullOrDefault;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class RestructureSettings {
    private final String compression;
    private final boolean deduplicate;
    private final String format;
    private final int cacheSize;
    private final Path tempDir;
    private final Path outputPath;
    private final int numThreads;
    private final int maxFilesPerTopic;
    private final List<String> excludeTopics;

    private RestructureSettings(Builder builder) {
        this.compression = builder.compression;
        this.deduplicate = builder.doDeduplicate;
        this.format = builder.format;
        this.cacheSize = builder.cacheSize;
        this.tempDir = builder.tempDir;
        this.outputPath = builder.outputPath;
        this.numThreads = builder.numThreads;
        this.maxFilesPerTopic = builder.maxFilesPerTopic;
        this.excludeTopics = builder.excludeTopics;
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

    public int getMaxFilesPerTopic() {
        return maxFilesPerTopic;
    }

    public List<String> getExcludeTopics() {
        return excludeTopics;
    }

    public static class Builder {
        private int numThreads = 1;
        private String compression;
        private boolean doDeduplicate;
        private String format;
        private int cacheSize = CACHE_SIZE_DEFAULT;
        private Path tempDir;
        private final Path outputPath;
        public int maxFilesPerTopic;
        private List<String> excludeTopics;

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

        public Builder maxFilesPerTopic(int num) {
            this.maxFilesPerTopic = num;
            return this;
        }

        public Builder excludeTopics(List<String> topics) {
            this.excludeTopics = topics;
            return this;
        }

        public RestructureSettings build() {
            compression = nonNullOrDefault(compression, () -> "identity");
            format = nonNullOrDefault(format, () -> "csv");
            tempDir = nonNullOrDefault(tempDir, () -> {
                try {
                    return Files.createTempDirectory("radar-hdfs-restructure");
                } catch (IOException ex) {
                    throw new UncheckedIOException("Cannot create temporary directory", ex);
                }
            });
            excludeTopics = nonNullOrDefault(excludeTopics, ArrayList::new);

            return new RestructureSettings(this);
        }
    }
}
