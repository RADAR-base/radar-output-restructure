package org.radarcns.hdfs.accounting;

public class Transaction {
    private final OffsetRange offset;
    private final Bin bin;

    public Transaction(OffsetRange offset, Bin bin) {
        this.offset = offset;
        this.bin = bin;
    }

    public OffsetRange getOffset() {
        return offset;
    }

    public Bin getBin() {
        return bin;
    }
}
