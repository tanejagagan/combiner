package io.dazzleduck.combiner.connector.spark;

import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.UploadObjectArgs;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.containers.Network;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

public class IntegrationTest {

    public static Network network = Network.newNetwork();

    public static GenericContainer<?> combiner = CombinerContainerTestUtil.createContainer("combiner", network);

    public static MinIOContainer minio =
            MinioContainerTestUtil.createContainer("minio", network);

    public static MinioClient minioClient ;

    public static SparkSession sparkSession;
    @BeforeAll
    public static void beforeAll() throws Exception {
        minio.start();
        combiner.start();
        minioClient = MinioContainerTestUtil.createClient(minio);
        minioClient.makeBucket(MakeBucketArgs.builder().bucket(MinioContainerTestUtil.bucketName).build());
        sparkSession = SparkSessionHelper.getSparkSession(minio);
    }

    @AfterAll
    public static void afterAll() throws Exception {
        sparkSession.stop();
        minio.stop();
        combiner.stop();
    }


    @Test
    public void testFilterAndProject() throws IOException {
        var database = "test_local";
        sparkSession.sql("use " + SparkSessionHelper.LOCAL_CATALOG);
        sparkSession.sql("show databases").show();
        sparkSession.sql(String.format("create database %s", database));
        sparkSession.sql(String.format("use %s", database));
        sparkSession.sql("create table test( key string, value int, partition string) using parquet partitioned by (partition)");
        sparkSession.sql("insert into test values ('k1', 10, 'p1'), ('k2', 20, 'p2')");
        Dataset<Row> res = sparkSession.sql("select key, (value + 10) from test where (value%10) = 0 ");
        System.out.println(res.queryExecution().executedPlan());
        sparkSession.sql("select key, (value + 10) from test where (value%10) = 0 ").show();
        res.collect();
    }

    @Test
    public void testDDFilterAndProject() throws IOException {
        var database = "tests3";
        sparkSession.sql("use " + SparkSessionHelper.S3_CATALOG);
        sparkSession.sql("show databases").show();
        sparkSession.sql(String.format("create database %s", database));
        sparkSession.sql(String.format("use %s", database));
        sparkSession.sql("create table test( key string, value int, partition string) using parquet partitioned by (partition)");
        sparkSession.sql("insert into test values ('k1', 10, 'p1'), ('k2', 20, 'p2')");
        Dataset<Row> res = sparkSession.sql("select key, (value + 10) from test where (value%10) = 0 ");
        System.out.println(res.queryExecution().executedPlan());
        sparkSession.sql("select key, (value + 10) from test where (value%10) = 0 ").show();
        res.collect();
    }

    @Test
    public void testRemoteFilterAndProject() throws IOException {
        var url = CombinerContainerTestUtil.getURL(combiner);
        var database = "tests4";
        sparkSession.sql("use " + SparkSessionHelper.S3_CATALOG);
        sparkSession.sql("show databases").show();
        sparkSession.sql(String.format("create database %s", database));
        sparkSession.sql(String.format("use %s", database));
        var minioEndpoint = MinioContainerTestUtil.getS3ParamForRemoteContainer(minio).get("s3_endpoint");
        sparkSession.sql(
                String.format("create table test( key string, value int, partition string) using parquet partitioned by (partition) options ('url'='%s', 's3_endpoint'='%s')", url, minioEndpoint));

        sparkSession.sql("insert into test values ('k1', 10, 'p1'), ('k2', 20, 'p2')");
        Dataset<Row> res = sparkSession.sql("select key, (value + 10) from test where (value%10) = 0 ");

        System.out.println(res.queryExecution().executedPlan());
        sparkSession.sql("select key, (value + 10) from test where (value%10) = 0 ").show();
        res.collect();
    }

    public void createTestTableWithOptions(SparkSession sparkSession,
                                           String database, String table, Map<String, String> options ) {
        String optionStr = "";
        if(!options.isEmpty()) {
            var toAppend = options.entrySet().stream().map(kv->
                    String.format("'%s'='%s'", kv.getKey(), kv.getValue())).collect(Collectors.joining(","));
            optionStr = String.format("options (%s)", toAppend);
        }

        String ddl = String.format("create table %s.%s( key string, value int, partition string) using parquet partitioned by (partition) %s", database, table, optionStr );
        sparkSession.sql("create table %s.%s( key string, value int, partition string) using parquet partitioned by (partition)");
    }
}
