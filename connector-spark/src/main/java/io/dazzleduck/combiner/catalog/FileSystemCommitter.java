package io.dazzleduck.combiner.catalog;


import java.util.*;
import java.util.function.Function;

interface Compressor<K, R> {
    K getKey(R r);

    R compress(R oldRecord, R newRecord);

    boolean retain(R record);
}

public interface FileSystemCommitter<M, R, K> {
    MetadataAndData loadLatest();

    default boolean append(R r) {
        List<R> l = new ArrayList<>();
        l.add(r);
        return append(l.iterator(), m -> m, m -> true);
    }

    boolean append(Iterator<R> data,
                   Function<M, M> metadataResolution,
                   Function<M, Boolean> proceed) throws ConcurrentModificationException;

    boolean update(Function<R, R> rewrite) throws ConcurrentModificationException;

    void remove(Function<R, Boolean> filter) throws ConcurrentModificationException;

    boolean upsert(Iterator<R> insert,
                   Function<R, R> replace) throws ConcurrentModificationException;

    Optional<Compressor<K, R>> getCompressor();

    MetadataAndData loadState(long startBatchId,
                              Iterator<R> currentState);


    static class MetadataAndData<M, T> {
        public final M metadata;
        public final Iterator<T> data;
        public final Long nextBatchId;
        public final boolean replace;

        public MetadataAndData(M metadata,
                               Iterator<T> data,
                               long nextBatchId,
                               boolean replace) {
            this.metadata = metadata;
            this.data = data;
            this.nextBatchId = nextBatchId;
            this.replace = replace;
        }
    }
}

/**
 * @param <M>
 * @param <R>
 * @param <K> Support functionality using append to filesystem logs
 *            1. Add will be with action ADD
 *            2. Delete will be with action DELETE if you still want to keep it for specified time till next compaction
 *            3. Update will be with action UPDATE
 *            4. At compaction entries marked delete will be dropped.
 *            5. To avoid list to get the latest file consumer can do following.
 *            Assume client is at batch 13. If batch 14 is present then it need to do the listing to get the latest batch.
 *            If batch 14 does not exist then two possibilities.
 *            1. 14 is not created and 13 is the latest batch
 *            2. 14 is created but it was delete because retention setting. For example retention is set to 100 and current batch is 150. Then 14 will for surely deleted
 */
interface AppendOnlyFileSystemCommitter<M, R, K> {
    default boolean append(R r, M metadata) {
        List<R> l = new ArrayList<>();
        l.add(r);
        return append(l, metadata);
    }

    Optional<State<M, R, K>> latestState();

    boolean append(Collection<R> data, M metadata) throws ConcurrentModificationException;

    KeyProvider<K, R> keyProvider();

    Function<R, Boolean> retainer();

    Result proceed(long batchId, M oldMetadata, M newMetadata);

    M resolveMetaData(long batchId, M oldMetadata, M newMetadata);

    enum Result {
        OK, NO_OP, CONCURRENT_MODIFICATION
    }
}

interface KeyProvider<K, R> {
    K getKey(R record);
}

class State<M, R, K> {
    public final Map<K, RecordWrapper<R>> map;
    public final M latestMetadata;
    public final long[] batchIds;

    public State(Map<K, RecordWrapper<R>> map, M latestMetadata, long[] batchIds) {
        this.map = map;
        this.latestMetadata = latestMetadata;
        this.batchIds = batchIds;
    }
}

