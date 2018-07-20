package org.radarcns.hdfs.data;

import java.util.Collection;
import java.util.Locale;

public interface Format {
    Collection<String> getFormats();

    String getExtension();

    default boolean matchesFilename(String name) {
        return name.toLowerCase(Locale.US).endsWith(getExtension().toLowerCase(Locale.US));
    }
}
