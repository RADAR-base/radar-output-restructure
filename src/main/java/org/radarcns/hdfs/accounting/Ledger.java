package org.radarcns.hdfs.accounting;

import java.util.HashMap;
import java.util.Map;

public class Ledger {
    private final OffsetRangeSet offsets;
    private final Map<Bin, Long> bins;

    public Ledger() {
        offsets = new OffsetRangeSet();
        bins = new HashMap<>();
    }

    public void add(Transaction transaction) {
        offsets.add(transaction.getOffset());
        bins.compute(transaction.getBin(), (b, vOld) -> vOld == null ? 1L : vOld + 1L);
    }

    public Map<? extends Bin,? extends Number> getBins() {
        return bins;
    }

    public OffsetRangeSet getOffsets() {
        return offsets;
    }
}
