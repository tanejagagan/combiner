package io.dazzleduck.combiner.catalog;

public interface DAO<R, K> {
    void add(R r);

    void delete(R r);

    R get(K k);

    Iterable<R> listAll();
}
