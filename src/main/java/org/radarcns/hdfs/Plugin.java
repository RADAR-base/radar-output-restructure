package org.radarcns.hdfs;

import java.io.IOException;
import java.util.Map;

public interface Plugin {
    default void init(Map<String, String> properties) throws IOException {
        // do nothing
    }
}
