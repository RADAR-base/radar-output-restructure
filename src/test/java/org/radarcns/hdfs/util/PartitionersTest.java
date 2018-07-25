package org.radarcns.hdfs.util;

import org.junit.Test;

import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

public class PartitionersTest {

    @Test
    public void unorderedBatches() {
        assertEquals(Arrays.asList(Arrays.asList(1, 2), Arrays.asList(5, 2), Arrays.asList(4)),
                IntStream.of(1, 2, 5, 2, 4)
                        .boxed()
                        .collect(Partitioners.unorderedBatches(2, Collectors.toList())));
    }


    @Test
    public void unorderedBatchesValued() {
        assertEquals(Arrays.asList(Arrays.asList(1, 2), Arrays.asList(5), Arrays.asList(2, 4)),
                IntStream.of(1, 2, 5, 2, 4)
                        .boxed()
                        .collect(Partitioners.unorderedBatches(3, Function.identity(),
                                Collectors.toList())));
    }

}