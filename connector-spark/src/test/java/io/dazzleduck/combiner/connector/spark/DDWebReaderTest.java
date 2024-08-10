package io.dazzleduck.combiner.connector.spark;

import io.dazzleduck.combiner.common.Constants;
import io.dazzleduck.combiner.common.Pools;
import io.dazzleduck.combiner.common.model.QueryGenerator;
import io.dazzleduck.combiner.common.model.QueryObject;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.UploadObjectArgs;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ArrowColumnVector;
import org.apache.spark.unsafe.types.UTF8String;
import org.duckdb.DuckDBResultSet;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.containers.Network;

import java.io.File;
import java.io.IOException;
import java.sql.Array;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static io.dazzleduck.combiner.connector.spark.MinioContainerTestUtil.bucketName;
import static io.dazzleduck.combiner.connector.spark.MinioContainerTestUtil.parquetObjectName;
import static org.junit.jupiter.api.Assertions.assertEquals;


public class DDWebReaderTest {

    public static Network network = Network.newNetwork();

    public static GenericContainer<?> combiner = CombinerContainerTestUtil.createContainer("combiner", network);

    public static MinIOContainer minio =
            MinioContainerTestUtil.createContainer("minio", network);

    public static MinioClient minioClient ;

    @BeforeAll
    public static void beforeAll() throws Exception {
        minio.start();
        combiner.start();
        minioClient = MinioContainerTestUtil.createClient(minio);
        minioClient.makeBucket(MakeBucketArgs.builder().bucket(MinioContainerTestUtil.bucketName).build());
        File file = new File("src/test/resources/parquet/date=2024-01-01/test.parquet");
        minioClient.uploadObject(
                UploadObjectArgs
                        .builder()
                        .bucket(MinioContainerTestUtil.bucketName)
                        .object("db/table/test.parquet")
                        .filename(file.getAbsolutePath()).build());
    }

    @AfterAll
    public static void afterAll() throws Exception {
        minioClient.close();
        minio.stop();
        combiner.stop();
    }

    @Test
    public void testDirectS3Query() throws SQLException {
        var connection = Pools.DD_CONNECTION_POOL.get();
        var queryObject = testS3QueryObject(MinioContainerTestUtil.getS3ParamForInProcessConnection(minio));
        var sql = QueryGenerator.DuckDBQueryGenerator.DUCK_DB.generate(queryObject, "parquet");
        try (var statement = connection.createStatement();
             var rs = statement.executeQuery(sql)){
            int rowCount = 0;
            while (rs.next()){
                rowCount++;
            }
            Assertions.assertEquals(1, rowCount);
        }
    }

    @Test
    public void testDDReaderNoPartition() throws SQLException, IOException {
        var queryObject = testS3QueryObject(MinioContainerTestUtil.getS3ParamForRemoteContainer(minio));
        var localObject = testS3QueryObject(MinioContainerTestUtil.getS3ParamForInProcessConnection(minio));
        var sql = QueryGenerator.DuckDBQueryGenerator.DUCK_DB.generate(localObject, "parquet");
        internalTestWithPartitions(sql, queryObject, fileOutputSchema(), new StructType(), null);
    }

    @Test
    /**
     * Partition (string, int, date, timestamp)
     */
    public void testWithPartitions() throws SQLException, IOException {
        var queryObject = testS3QueryObject(MinioContainerTestUtil.getS3ParamForRemoteContainer(minio));
        var localObject =  QueryObject.builder(queryObject)
                .projections(List.of("'p1'", "1", "cast(1 as bigint)", "cast('2024-01-01' as date)", "cast('2024-01-01' as timestamp)", "*"))
                .parameters(MinioContainerTestUtil.getS3ParamForInProcessConnection(minio)).build();
        var partitionSchema =
                (StructType) DataType.fromDDL("string_p string, int_p int, bigint_p bigint, date_p date, timestamp_p timestamp");
        var internalRow = new GenericInternalRow(List.of(UTF8String.fromString("p1"), 1, 1L, 19723, 1704067200000000L ).toArray());
        var expectedSql = QueryGenerator.DuckDBQueryGenerator
                .DUCK_DB
                .generate(localObject, "parquet");
        internalTestWithPartitions(expectedSql, queryObject,
                new StructType(
                        Stream.concat(Arrays.stream(partitionSchema.fields()),
                                Arrays.stream(fileOutputSchema().fields())).toArray(len -> new StructField[len])),
                partitionSchema, internalRow);
    }

    @Test
    public void testLocalDirectQuery() throws IOException, SQLException {
        var queryObject = testLocalQueryObject(Map.of());
        var expectedSql = QueryGenerator.DuckDBQueryGenerator
                .DUCK_DB
                .generate(queryObject, "parquet");
        internalTestDirectWithPartitions( expectedSql, queryObject, fileOutputSchema(), new StructType(), null);

    }

    private void internalTestWithPartitions(String expectedSql, QueryObject queryObject,
                                            StructType outputSchema, StructType partitionSchema, InternalRow partition) throws SQLException, IOException {
        String url = CombinerContainerTestUtil.getURL(combiner);
        DDWebReader reader = new DDWebReader(outputSchema, queryObject,
                Map.of(Constants.URL_KEY, url), partitionSchema, partition);
        assertResultsEquals(expectedSql, reader);
    }

    private void internalTestDirectWithPartitions(String expectedSql, QueryObject queryObject,
                                                  StructType outputSchema,
                                                  StructType partitionSchema, InternalRow partition) throws SQLException, IOException {
        String url = CombinerContainerTestUtil.getURL(combiner);
        DDReader reader = new DDDirectReader(outputSchema, queryObject,
                Map.of(Constants.URL_KEY, url), partitionSchema, partition);
        assertResultsEquals(expectedSql, reader);
    }

    private QueryObject testS3QueryObject(Map<String, String> parameters) {
        File file = new File("src/test/resources/parquet/date=2024-01-01/test.parquet");
        return QueryObject.builder()
                .projections(List.of("key", "value", "time"))
                .parameters(parameters)
                .sources(List.of(String.format("s3://%s/%s", bucketName, parquetObjectName)))
                .build();
    }

    private StructType fileOutputSchema(){
        return StructType.fromDDL("key string, value bigint, time timestamp");
    }

    private QueryObject testLocalQueryObject(Map<String, String> parameters) {

        File file = new File("src/test/resources/parquet/date=2024-01-01/test.parquet");
        return QueryObject.builder()
                .projections(List.of("key", "value", "time"))
                .parameters(parameters)
                .sources(List.of(file.getAbsolutePath()))
                .build();
    }

    private void assertResultsEquals(String expectedSql, DDReader reader) throws SQLException, IOException {
        var resultBatches = new ArrayList<ArrowColumnarBatch>();
        while (reader.next()){
            resultBatches.add((ArrowColumnarBatch)reader.get());
        }
        var expected = new ArrayList<ArrowColumnarBatch>();
        var connection = Pools.DD_CONNECTION_POOL.get();
        try (var statement = connection.createStatement();
             var rs = (DuckDBResultSet)statement.executeQuery(expectedSql)){
            try (var arrowReader = (ArrowReader) rs.arrowExportStream(Pools.ALLOCATOR_POOL.get(), 256)) {
                while (arrowReader.loadNextBatch()){
                    var vsr = arrowReader.getVectorSchemaRoot();
                    var array = vsr.getFieldVectors()
                            .stream()
                            .map(fv -> new ArrowColumnVector(vsr.getVector(fv.getName())))
                            .toArray(a -> new ArrowColumnVector[a]);
                    var batch = new ArrowColumnarBatch(array, vsr.getRowCount(), null);
                    expected.add(batch);
                }
                assertArrowColumnarBatch(expected, resultBatches);
            }
        }
    }

    private void assertArrowColumnarBatch(List<ArrowColumnarBatch> expected, List<ArrowColumnarBatch> result) {
        List<InternalRow> expectedRow  = addToRows(expected);
        List<InternalRow> resultRow = addToRows(result);
        assertEquals(expectedRow.size(), resultRow.size(), "Length is not equal");
        for(int row =0; row < expectedRow.size(); row ++){
            assertEquals(expectedRow.get(row).copy(), resultRow.get(row).copy());
        }
    }

    private List<InternalRow> addToRows(List<ArrowColumnarBatch> arrowColumnarBatches) {
        List<InternalRow> result = new ArrayList<>();
        for(ArrowColumnarBatch b : arrowColumnarBatches){
            for(int i =0; i< b.numRows(); i++){
                result.add(b.getRow(i));
            }
        }
        return result;
    }
}
