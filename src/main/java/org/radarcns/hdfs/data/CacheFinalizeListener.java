package org.radarcns.hdfs.data;

@FunctionalInterface
public interface CacheFinalizeListener {
    void onCacheCloseFinished();
}
