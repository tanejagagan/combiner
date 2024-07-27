package io.dazzleduck.combiner.connector.spark;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.ArrowUtils;
import org.apache.spark.sql.vectorized.ArrowColumnVector;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.util.Collections;

public final class ArrowColumnarBatch extends ColumnarBatch {

    public final ArrowRecordBatch arrowRecordBatch;

    private ArrowColumnarBatch(VectorSchemaRoot vectorSchemaRoot) {
        this(getColumnVectors(vectorSchemaRoot), vectorSchemaRoot.getRowCount(),
                new VectorUnloader(vectorSchemaRoot).getRecordBatch());
    }

    public ArrowColumnarBatch(ColumnVector [] vectors,
                              int rowCount,
                              ArrowRecordBatch recordBatch ) {
        super(vectors, rowCount);
        this.arrowRecordBatch = recordBatch;
    }

    public ArrowReader getReader(BufferAllocator allocator,
                                 StructType sparkSchema,
                                 String timezoneString) {
        Schema schema = ArrowUtils.toArrowSchema(sparkSchema, timezoneString, true, false );
        return new ListArrowReader(allocator, schema, Collections.singletonList(arrowRecordBatch));
    }

    private static ColumnVector[] getColumnVectors(VectorSchemaRoot vectorSchemaRoot) {
        return vectorSchemaRoot
                .getFieldVectors()
                .stream()
                .map(ArrowColumnVector::new)
                .toArray(ColumnVector[]::new);
    }

    public static ArrowColumnarBatch fromVectorSchemaRoot(VectorSchemaRoot vectorSchemaRoot) {
        return new ArrowColumnarBatch(vectorSchemaRoot);
    }
}
