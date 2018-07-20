package org.radarcns.hdfs.data;

import org.radarcns.hdfs.Plugin;

import java.util.List;
import java.util.Locale;

public interface FormatProvider<T extends Format> extends Plugin {
    List<T> getAll();

    default T get(String format) {
        return getAll()
                .stream()
                .filter(r -> r.getFormats().stream().anyMatch(s -> s.equalsIgnoreCase(format)))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "Format " + format + " is not supported"));
    }

    default T getForFile(String fileName) {
        return getAll()
                .stream()
                .filter(r -> r.matchesFilename(fileName))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "File name " + fileName + " is not supported"));
    }
}
