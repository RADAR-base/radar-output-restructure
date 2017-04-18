package org.radarcns.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class FileCacheTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void appendLine() throws IOException {
        File f1 = folder.newFile();
        File f2 = folder.newFile();
        File f3 = folder.newFile();
        File d4 = folder.newFolder();
        File f4 = new File(d4, "f4.txt");

        assertTrue(f1.delete());
        assertTrue(d4.delete());

        try (FileCache cache = new FileCache(2)) {
            assertFalse(cache.appendLine(f1, "something"));
            assertTrue(cache.appendLine(f1, "somethingElse"));
            assertFalse(cache.appendLine(f2, "something"));
            assertTrue(cache.appendLine(f1, "third"));
            assertFalse(cache.appendLine(f3, "f3"));
            assertFalse(cache.appendLine(f2, "f2"));
            assertTrue(cache.appendLine(f3, "f3"));
            assertFalse(cache.appendLine(f4, "f4"));
            assertTrue(cache.appendLine(f3, "f3"));
        }

        assertEquals("something\nsomethingElse\nthird\n", new String(Files.readAllBytes(f1.toPath())));
        assertEquals("something\nf2\n", new String(Files.readAllBytes(f2.toPath())));
        assertEquals("f3\nf3\nf3\n", new String(Files.readAllBytes(f3.toPath())));
        assertEquals("f4\n", new String(Files.readAllBytes(f4.toPath())));
    }
}
