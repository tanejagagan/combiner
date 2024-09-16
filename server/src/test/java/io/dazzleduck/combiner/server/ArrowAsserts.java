package io.dazzleduck.combiner.server;

import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import io.dazzleduck.combiner.common.ConfigParameters;
import io.dazzleduck.combiner.common.Pools;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.table.Table;
import org.duckdb.DuckDBResultSet;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ArrowAsserts {
    public static void equals(String sql, byte[] bytes) throws SQLException, IOException {
        equals(sql, new ByteBufferBackedInputStream(ByteBuffer.wrap(bytes)));
    }

    public static void equals(String sql, InputStream stream) throws SQLException, IOException {

        var allocator = Pools.ALLOCATOR_POOL.get();
        var resultTables = new ArrayList<Table>();
        try (var reader = new ArrowStreamReader(stream, allocator)) {
            while (reader.loadNextBatch()) {
                var readRoot = reader.getVectorSchemaRoot();
                resultTables.add(new Table(readRoot));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        var resultSize = resultTables.stream().collect(Collectors.summingLong(x -> x.getRowCount()));

        var connection = Pools.DD_CONNECTION_POOL.get();
        var expectedTables = new ArrayList<Table>();
        try (var statement= connection.createStatement();
             var resultSet = (DuckDBResultSet) statement.executeQuery(sql);
             var expectedReader = (ArrowReader) resultSet.arrowExportStream(allocator, ConfigParameters.ARROW_DEFAULT_BATCH_SIZE);) {
            while (expectedReader.loadNextBatch()) {
                var vsr = expectedReader.getVectorSchemaRoot();
                expectedTables.add(new Table(vsr));
            }
        }
        var expectedSize = expectedTables.stream().collect(Collectors.summingLong(x -> x.getRowCount()));
        assertEquals( expectedSize, resultSize, "expected length don't match with result");
        resultTables.stream().forEach( t -> t.close());
        expectedTables.forEach(t -> t.close());

    }

    public static void printResult(String sql) throws SQLException, IOException {
        var connection = Pools.DD_CONNECTION_POOL.get();
        var allocator = Pools.ALLOCATOR_POOL.get();
        var expectedTables = new ArrayList<Table>();
        try (var statement= connection.createStatement();
             var resultSet = (DuckDBResultSet) statement.executeQuery(sql);
             var expectedReader = (ArrowReader) resultSet.arrowExportStream(allocator, ConfigParameters.ARROW_DEFAULT_BATCH_SIZE);) {
            while (expectedReader.loadNextBatch()) {
                var vsr = expectedReader.getVectorSchemaRoot();
                System.out.println(vsr.contentToTSVString());
            }
        }
    }
}
