package io.dazzleduck.combiner.common;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.duckdb.DuckDBConnection;
import org.duckdb.DuckDBDriver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

public class Pools {

    static {
        try {
            Class.forName("org.duckdb.DuckDBDriver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
    public static ThreadLocal<BufferAllocator> ALLOCATOR_POOL = ThreadLocal.withInitial(RootAllocator::new);

    public static final Map<String, String> runtimeConfig = Map.of("s3_url_style", "'path'");

    private static final DuckDBConnection connection;

    static {
        Properties props = new Properties();
        props.setProperty(DuckDBDriver.JDBC_STREAM_RESULTS, String.valueOf(true));
        try {
            connection = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:", props);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static ThreadLocal<DuckDBConnection> DD_CONNECTION_POOL = ThreadLocal.withInitial(() -> {
        DuckDBConnection conn;
        try {
            conn = (DuckDBConnection) connection.duplicate();
            setRuntimeConfig(conn, runtimeConfig);
        } catch (SQLException exception) {
            throw new RuntimeException(exception);
        }
        return conn;
    });

    public static void setRuntimeConfig(Connection connection,
                                        Map<String, String> configs) throws SQLException {
        for (Map.Entry<String, String> e : configs.entrySet()) {
            try(var statement = connection.createStatement()){
                statement.execute(String.format("SET %s=%s", e.getKey(), e.getValue()));
            }
        }
    }
}
