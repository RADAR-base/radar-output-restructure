package org.radarcns.util;

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

/**
 * Caches open file handles. If more than the limit is cached, the half of the files that were used
 * the longest ago cache are evicted from cache.
 */
public class FileCache implements Flushable, Closeable {
    private static final Logger logger = LoggerFactory.getLogger(FileCache.class);

    private final int maxFiles;
    private final Map<File, SingleFileCache> caches;

    public FileCache(int maxFiles) {
        this.maxFiles = maxFiles;
        this.caches = new HashMap<>(maxFiles * 4 / 3 + 1);
    }

    /**
     * Append a line to given file. This will append a line ending to given data.
     * If the file handle and writer are already open in this cache,
     * those will be used. Otherwise, the file will be opened and the file handle cached.
     *
     * @param file file to append data to
     * @param data data without line ending
     * @return true if the cache was used, false if a new file was opened.
     * @throws IOException when failing to open a file or writing to it.
     */
    public boolean appendLine(File file, String data) throws IOException {
        SingleFileCache cache = caches.get(file);
        if (cache != null) {
            cache.appendLine(data);
            return true;
        } else {
            ensureCapacity();

            File dir = file.getParentFile();
            if (!dir.exists()){
                if (dir.mkdirs()) {
                    logger.debug("Created directory: {}", dir.getAbsolutePath());
                } else {
                    logger.warn("FAILED to create directory: {}", dir.getAbsolutePath());
                }
            }

            cache = new SingleFileCache(file);
            caches.put(file, cache);
            cache.appendLine(data);
            return false;
        }
    }

    /**
     * Ensure that a new filecache can be added. Evict files used longest ago from cache if needed.
     */
    private void ensureCapacity() throws IOException {
        if (caches.size() == maxFiles) {
            ArrayList<SingleFileCache> cacheList = new ArrayList<>(caches.values());
            Collections.sort(cacheList);
            for (int i = 0; i < cacheList.size() / 2; i++) {
                SingleFileCache rmCache = cacheList.get(i);
                caches.remove(rmCache.getFile());
                rmCache.close();
            }
        }
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
