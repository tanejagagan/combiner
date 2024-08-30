package io.dazzleduck.combiner.common;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.duckdb.DuckDBConnection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;

public class Pools {

    static {
        try {
            Class.forName("org.duckdb.DuckDBDriver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
    public static ThreadLocal<BufferAllocator> ALLOCATOR_POOL = ThreadLocal.withInitial(RootAllocator::new);

    public static final Map<String, String> config = Map.of("s3_url_style", "'path'");

    public static ThreadLocal<DuckDBConnection> DD_CONNECTION_POOL = ThreadLocal.withInitial(() -> {
        DuckDBConnection conn;
        try {

            conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
            setConfig(conn, config);
        } catch (SQLException exception) {
            throw new RuntimeException(exception);
        }
        return conn;
    });

    public static void setConfig(Connection connection,
                                 Map<String, String> configs) throws SQLException {
        for (Map.Entry<String, String> e : configs.entrySet()) {
            try(var statement = connection.createStatement()){
                statement.execute(String.format("SET %s=%s", e.getKey(), e.getValue()));
            }
        }
    }
}
