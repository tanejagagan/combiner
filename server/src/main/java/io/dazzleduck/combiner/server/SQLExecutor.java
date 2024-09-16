package io.dazzleduck.combiner.server;

import io.dazzleduck.combiner.common.Pools;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.duckdb.DuckDBResultSet;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class SQLExecutor {

    public static void executeSql(String sql,
                                  OutputStream pipedOutputStream,
                                  int arrowBatchSize) throws IOException, SQLException {
        var conn = Pools.DD_CONNECTION_POOL.get();
        var allocator = Pools.ALLOCATOR_POOL.get();
        try (var stmt = conn.prepareStatement(sql);
             var resultSet = (DuckDBResultSet) stmt.executeQuery();) {
            try (var reader = (ArrowReader) resultSet.arrowExportStream(allocator, arrowBatchSize)) {
                VectorSchemaRoot vectorSchemaRoot = reader.getVectorSchemaRoot();
                var writer = new ArrowStreamWriter(vectorSchemaRoot, null, pipedOutputStream);
                writer.start();
                while (reader.loadNextBatch()) {
                    writer.writeBatch();
                }
                writer.end();
                pipedOutputStream.flush();
            }
        }
    }

    public static Collector executeAndProvideCollect(String sql, OutputStream outputStream,
                                                     int arrowBatchSize) throws SQLException {
        var conn = Pools.DD_CONNECTION_POOL.get();
        var allocator = Pools.ALLOCATOR_POOL.get();
        var stmt = conn.prepareStatement(sql);
        var resultSet = (DuckDBResultSet) stmt.executeQuery();
        return new Collector(stmt, resultSet, allocator, arrowBatchSize, outputStream);
    }

    public static class Collector {
        public Collector( PreparedStatement statement,
                DuckDBResultSet resultSet, BufferAllocator allocator, int arrowBatchSize, OutputStream outputStream) {
            this.preparedStatement = statement;
            this.resultSet = resultSet;
            this.allocator = allocator;
            this.arrowBatchSize = arrowBatchSize;
            this.outputStream = outputStream;
        }
        private PreparedStatement preparedStatement;
        private DuckDBResultSet resultSet;
        private BufferAllocator allocator;
        private int arrowBatchSize;
        private OutputStream outputStream;

        public void executeCollect()  {
            try (var reader = (ArrowReader) resultSet.arrowExportStream(allocator, arrowBatchSize)) {
                VectorSchemaRoot vectorSchemaRoot = reader.getVectorSchemaRoot();
                var writer = new ArrowStreamWriter(vectorSchemaRoot, null, outputStream);
                writer.start();
                while (reader.loadNextBatch()) {
                    writer.writeBatch();
                }
                writer.end();
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                try {
                    outputStream.flush();
                    outputStream.close();
                    resultSet.close();
                    preparedStatement.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
