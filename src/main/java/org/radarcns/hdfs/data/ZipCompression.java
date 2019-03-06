package org.radarcns.hdfs.data;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

public class ZipCompression implements Compression {
    @Override
    public OutputStream compress(String name, OutputStream out) throws IOException {
        ZipOutputStream zipOut = new ZipOutputStream(out);
        zipOut.setLevel(7);
        zipOut.putNextEntry(new ZipEntry(name));
        return zipOut;
    }

    @Override
    public InputStream decompress(InputStream in) throws IOException {
        ZipInputStream zipIn = new ZipInputStream(in);
        zipIn.getNextEntry();
        return zipIn;
    }

    @Override
    public Collection<String> getFormats() {
        return Collections.singleton("zip");
    }

    @Override
    public String getExtension() {
        return ".zip";
    }
}
