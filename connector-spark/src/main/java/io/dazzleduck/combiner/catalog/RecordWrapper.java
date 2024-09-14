package io.dazzleduck.combiner.catalog;

public class RecordWrapper<R> {
    public final R record;
    public final long batchId;

    public RecordWrapper(R record, long batchId) {
        this.record = record;
        this.batchId = batchId;
    }
}
