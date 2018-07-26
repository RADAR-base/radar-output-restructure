/*
 * Copyright 2017 The Hyve
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.radarcns.hdfs.data;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.radarcns.hdfs.Application;
import org.radarcns.hdfs.FileStoreFactory;
import org.radarcns.hdfs.accounting.Accountant;
import org.radarcns.hdfs.accounting.Bin;
import org.radarcns.hdfs.accounting.OffsetRange;
import org.radarcns.hdfs.accounting.TopicPartition;
import org.radarcns.hdfs.config.RestructureSettings;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FileCacheStoreTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void appendLine() throws IOException {
        Path f1 = folder.newFile().toPath();
        Path f2 = folder.newFile().toPath();
        Path f3 = folder.newFile().toPath();
        Path d4 = folder.newFolder().toPath();
        Path f4 = d4.resolve("f4.txt");
        Path newFile = folder.newFile().toPath();
        Path tmpDir = folder.newFolder().toPath();

        Files.delete(f1);
        Files.delete(d4);

        Schema simpleSchema = SchemaBuilder.record("simple").fields()
                .name("a").type("string").noDefault()
                .endRecord();

        Schema conflictSchema = SchemaBuilder.record("simple").fields()
                .name("a").type("string").noDefault()
                .name("b").type("string").noDefault()
                .endRecord();

        GenericRecord record;

        TopicPartition topicPartition0 = new TopicPartition("t", 0);
        TopicPartition topicPartition1 = new TopicPartition("t", 1);

        OffsetRange offsetRange0 = new OffsetRange(topicPartition0, 0, 0);
        OffsetRange offsetRange1 = new OffsetRange(topicPartition1, 0, 8);
        Bin bin = new Bin("t", "c", "00");

        RestructureSettings settings = new RestructureSettings.Builder(folder.getRoot().toString())
                .cacheSize(2)
                .tempDir(tmpDir.toString())
                .build();

        FileStoreFactory factory = new Application.Builder(settings).build();
        Accountant accountant = new Accountant(factory);

        try (FileCacheStore cache = factory.newFileCacheStore(accountant)) {
            int i0 = 0;
            int i1 = 0;
            Accountant.Transaction transaction;

            record = new GenericRecordBuilder(simpleSchema).set("a", "something").build();
            transaction = new Accountant.Transaction(topicPartition1, i1++, bin);
            assertEquals(FileCacheStore.WriteResponse.NO_CACHE_AND_WRITE,
                    cache.writeRecord(f1, record, transaction));
            record = new GenericRecordBuilder(simpleSchema).set("a", "somethingElse").build();
            transaction = new Accountant.Transaction(topicPartition1, i1++, bin);
            assertEquals(FileCacheStore.WriteResponse.CACHE_AND_WRITE,
                    cache.writeRecord(f1, record, transaction));
            record = new GenericRecordBuilder(simpleSchema).set("a", "something").build();
            transaction = new Accountant.Transaction(topicPartition0, i0++, bin);
            assertEquals(FileCacheStore.WriteResponse.NO_CACHE_AND_WRITE,
                    cache.writeRecord(f2, record, transaction));
            record = new GenericRecordBuilder(simpleSchema).set("a", "third").build();
            transaction = new Accountant.Transaction(topicPartition1, i1++, bin);
            assertEquals(FileCacheStore.WriteResponse.CACHE_AND_WRITE,
                    cache.writeRecord(f1, record, transaction));
            record = new GenericRecordBuilder(simpleSchema).set("a", "f3").build();
            transaction = new Accountant.Transaction(topicPartition1, i1++, bin);
            assertEquals(FileCacheStore.WriteResponse.NO_CACHE_AND_WRITE,
                    cache.writeRecord(f3, record, transaction));
            record = new GenericRecordBuilder(simpleSchema).set("a", "f2").build();
            transaction = new Accountant.Transaction(topicPartition1, i1++, bin);
            assertEquals(FileCacheStore.WriteResponse.NO_CACHE_AND_WRITE,
                    cache.writeRecord(f2, record, transaction));
            record = new GenericRecordBuilder(simpleSchema).set("a", "f3").build();
            transaction = new Accountant.Transaction(topicPartition1, i1++, bin);
            assertEquals(FileCacheStore.WriteResponse.CACHE_AND_WRITE,
                    cache.writeRecord(f3, record, transaction));
            record = new GenericRecordBuilder(simpleSchema).set("a", "f4").build();
            transaction = new Accountant.Transaction(topicPartition1, i1++, bin);
            assertEquals(FileCacheStore.WriteResponse.NO_CACHE_AND_WRITE,
                    cache.writeRecord(f4, record, transaction));
            record = new GenericRecordBuilder(simpleSchema).set("a", "f3").build();
            transaction = new Accountant.Transaction(topicPartition1, i1++, bin);
            assertEquals(FileCacheStore.WriteResponse.CACHE_AND_WRITE,
                    cache.writeRecord(f3, record, transaction));
            record = new GenericRecordBuilder(conflictSchema).set("a", "f3"). set("b", "conflict").build();
            transaction = new Accountant.Transaction(topicPartition1, i1, bin);
            assertEquals(FileCacheStore.WriteResponse.CACHE_AND_NO_WRITE,
                    cache.writeRecord(f3, record, transaction));
            record = new GenericRecordBuilder(conflictSchema).set("a", "f1"). set("b", "conflict").build();
            // Cannot write to file even though the file is not in cache since schema is different
            assertEquals(FileCacheStore.WriteResponse.NO_CACHE_AND_NO_WRITE,
                    cache.writeRecord(f1, record, transaction));
            // Can write the same record to a new file
            assertEquals(FileCacheStore.WriteResponse.NO_CACHE_AND_WRITE,
                    cache.writeRecord(newFile, record, transaction));
        }

        assertTrue(accountant.getOffsets().contains(offsetRange0));
        assertTrue(accountant.getOffsets().contains(offsetRange1));
        assertTrue(accountant.getBins().toString().contains(" - 10"));

        assertEquals("a\nsomething\nsomethingElse\nthird\n", new String(Files.readAllBytes(f1)));
        assertEquals("a\nsomething\nf2\n", new String(Files.readAllBytes(f2)));
        assertEquals("a\nf3\nf3\nf3\n", new String(Files.readAllBytes(f3)));
        assertEquals("a\nf4\n", new String(Files.readAllBytes(f4)));
        assertEquals("a,b\nf1,conflict\n", new String(Files.readAllBytes(newFile)));
    }
}
