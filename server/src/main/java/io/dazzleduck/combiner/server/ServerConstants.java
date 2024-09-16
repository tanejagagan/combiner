package io.dazzleduck.combiner.server;

public class ServerConstants {
    public final static String QUERY_PATH = "v1/q";
    public final static String SERIES_PATH = "series";
    public final static String PARQUET_PATH = "parquet";
    public final static int DEFAULT_NETWORK_BUFFER = 1024 * 1024 * 8;
    public final static String PARQUET_FORMAT = "parquet";
    public static String COMPLETE_PARQUET_PATH = String.format("%s/%s", QUERY_PATH, PARQUET_PATH);

}
