package org.radarcns.hdfs.data;

import java.util.Arrays;
import java.util.List;

public class CompressionFactory implements FormatProvider<Compression> {
    public final List<Compression> getAll() {
        return Arrays.asList(
                new GzipCompression(),
                new IdentityCompression());
    }
}
