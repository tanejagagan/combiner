package io.dazzleduck.combiner.catalog;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Header {
    public final long[] validBatches;
    public final long lastAppendBatch;
    public final long timestamp;

    @JsonCreator
    public Header(@JsonProperty("validBatches") long[] validBatches,
                  @JsonProperty("lastAppendBatch") long lastAppendBatch,
                  @JsonProperty("timestamp") long timestamp) {
        this.validBatches = validBatches;
        this.lastAppendBatch = lastAppendBatch;
        this.timestamp = timestamp;
    }
}
