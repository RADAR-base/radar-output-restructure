package org.radarcns;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileWriter;
import java.io.Flushable;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileCache implements Flushable, Closeable {
    private static final Logger logger = LoggerFactory.getLogger(FileCache.class);

    private final int maxFiles;
    private final Map<File, SingleFileCache> caches;

    public FileCache(int maxFiles) {
        this.maxFiles = maxFiles;
        this.caches = new HashMap<>();
    }

    public void appendLine(File file, String data) throws IOException {
        SingleFileCache cache = caches.get(file);
        if (cache == null) {
            if (caches.size() == maxFiles) {
                ArrayList<SingleFileCache> cacheList = new ArrayList<>(caches.values());
                Collections.sort(cacheList);
                for (int i = 0; i < cacheList.size() / 2; i++) {
                    SingleFileCache rmCache = cacheList.get(i);
                    caches.remove(rmCache.getFile());
                    rmCache.close();
                }
            }

            File dir = file.getParentFile();
            if (!dir.exists()){
                if (dir.mkdirs()) {
                    logger.info("Created directory: {}", dir.getAbsolutePath());
                } else {
                    logger.warn("FAILED to create directory: " + dir.getAbsolutePath());
                }
            }

            cache = new SingleFileCache(file);
            caches.put(file, cache);
        }
        cache.appendLine(data);
    }

    @Override
    public void flush() throws IOException {
        for (SingleFileCache cache : caches.values()) {
            cache.flush();
        }
    }

    @Override
    public void close() throws IOException {
        try {
            for (SingleFileCache cache : caches.values()) {
                cache.close();
            }
        } finally {
            caches.clear();
        }
    }

    private static class SingleFileCache implements Closeable, Flushable, Comparable<SingleFileCache> {
        private final BufferedWriter bufferedWriter;
        private final Writer fileWriter;
        private final File file;
        private long lastUse;

        private SingleFileCache(File file) throws IOException {
            this.file = file;
            this.fileWriter = new FileWriter(file, true);
            this.bufferedWriter = new BufferedWriter(fileWriter);
        }

        private void appendLine(String data) throws IOException {
            bufferedWriter.write(data);
            bufferedWriter.write('\n');
            lastUse = System.nanoTime();
        }

        @Override
        public void close() throws IOException {
            bufferedWriter.close();
            fileWriter.close();
        }

        @Override
        public void flush() throws IOException {
            bufferedWriter.flush();
        }

        @Override
        public int compareTo(SingleFileCache other) {
            return Long.compare(lastUse, other.lastUse);
        }

        private File getFile() {
            return file;
        }
    }
}
