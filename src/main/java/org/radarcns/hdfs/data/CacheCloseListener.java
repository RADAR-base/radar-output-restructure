package org.radarcns.hdfs.data;

@FunctionalInterface
public interface CacheCloseListener {
    void onCacheClosed();
}
