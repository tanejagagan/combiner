package io.dazzleduck.combiner.connector.spark;

import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.containers.Network;

import java.util.List;
import java.util.Locale;

import static java.nio.file.Files.createTempDirectory;

public class IntegrationTest {

    public static Network network = Network.newNetwork();

    public static GenericContainer<?> combiner = CombinerContainerTestUtil.createContainer("combiner", network);

    public static MinIOContainer minio =
            MinioContainerTestUtil.createContainer("minio", network);

    public static MinioClient minioClient ;

    public static String catalogPath;

    public static SparkSession sparkSession;
    @BeforeAll
    public static void beforeAll() throws Exception {
        catalogPath = createTempDirectory("tmpCatalogs").toFile().getAbsolutePath();
        minio.start();
        combiner.start();
        minioClient = MinioContainerTestUtil.createClient(minio);
        minioClient.makeBucket(MakeBucketArgs.builder().bucket(MinioContainerTestUtil.bucketName).build());
        sparkSession = SparkSessionHelper.getSparkSession(minio, catalogPath);
    }

    @AfterAll
    public static void afterAll() {
        sparkSession.stop();
        minio.stop();
        combiner.stop();
    }


    @Test
    public void testFilterAndProject() {
        var database = "test_local";
        var table = "test";
        var catalog = SparkSessionHelper.LOCAL_CATALOG;
        var catalogPath = IntegrationTest.catalogPath;

        sparkSession.sql("use " + catalog);
        sparkSession.sql("show databases").show();
        sparkSession.sql(String.format("create database %s", database));
        sparkSession.sql(String.format("use %s", database));
        var schema = "key string, value int, partition string";
        sparkSession.sql(String.format("create table %s( %s) using parquet partitioned by (partition)", table, schema));
        //sparkSession.sql("insert into test values ('k1', 10, 'p1'), ('k2', 20, 'p2')");
        var df = sparkSession.createDataFrame(List.of(RowFactory.create("k1", 10, "p1"),
                RowFactory.create("k2", 100, "p2")),
                StructType.fromDDL(schema));
        df.write().mode("append").partitionBy("partition").parquet(getPath(catalogPath, database, table));
        // sparkSession.sql("select key, (value + 10) from test where (value%10) = 0 ").show();
        sparkSession.sql("select key, (value + 10), partition from test where (value%10) = 0 ").show();
    }


    @Test
    public void testDDFilterAndProject() {
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
    public void testRemoteFilterAndProject() {
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

    @Test
    public void testRemoteAggregation() {
        var url = CombinerContainerTestUtil.getURL(combiner);
        var database = "tests5";
        sparkSession.sql("use " + SparkSessionHelper.S3_CATALOG);
        sparkSession.sql("show databases").show();
        sparkSession.sql(String.format("create database %s", database));
        sparkSession.sql(String.format("use %s", database));
        var minioEndpoint = MinioContainerTestUtil.getS3ParamForRemoteContainer(minio).get("s3_endpoint");
        sparkSession.sql(
                String.format("create table test( key string, value int, partition string) using parquet partitioned by (partition) options ('url'='%s', 's3_endpoint'='%s')", url, minioEndpoint));

        sparkSession.sql("insert into test values ('k1', 10, 'p1'), ('k2', 20, 'p2')");
        Dataset<Row> res2 = sparkSession.sql("select count(*), sum(value), partition from test group by partition");
        res2.show();
    }

    private String getPath(String catalogPath, String database, String table) {
        return new Path(catalogPath, new Path(database.toLowerCase(Locale.ROOT), table.toLowerCase(Locale.ROOT) )).toString();
    }
}
