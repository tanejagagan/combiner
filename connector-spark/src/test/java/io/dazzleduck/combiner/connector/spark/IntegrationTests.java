package io.dazzleduck.combiner.connector.spark;

import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.util.List;
import java.util.Locale;

import static java.nio.file.Files.createTempDirectory;

public class IntegrationTests {

    public static Network network = Network.newNetwork();

    public static Logger LOGGER = LoggerFactory.getLogger(IntegrationTests.class);

    public static GenericContainer<?> combiner = CombinerContainerTestUtil.createContainer("combiner", network);

    public static MinIOContainer minio =
            MinioContainerTestUtil.createContainer("minio", network);

    public static MinioClient minioClient ;

    public static String localCatalogPath;

    public static String catalogPath;

    public static SparkSession sparkSession;
    public static String schema =
            "key string," +
                    "value int, " +
                    "ss  struct< ss1 : string, ss2 : string, ss3 : string, ss4 : struct<sss1 : string, sss2 : string>>," +
                    "arr array<bigint>," +
                    "partition string";
    public static String database = "test_db";
    public static String remote_table = "test_remote_table";
    public static String direct_table = "test_direct_table";
    public static String catalog  = SparkHelper.S3_CATALOG;
    public static String writePath;

    @BeforeAll
    public static void beforeAll() throws Exception {
        localCatalogPath = createTempDirectory("tmpCatalogs").toFile().getAbsolutePath();
        catalogPath = String.format("s3a://%s/", MinioContainerTestUtil.bucketName);
        minio.start();
        combiner.start();
        combiner.followOutput(new Slf4jLogConsumer(LOGGER));
        minioClient = MinioContainerTestUtil.createClient(minio);
        minioClient.makeBucket(MakeBucketArgs.builder().bucket(MinioContainerTestUtil.bucketName).build());
        sparkSession = SparkHelper.getSparkSession(minio, localCatalogPath, catalogPath);
        var url = CombinerContainerTestUtil.getURL(combiner);
        sparkSession.sql("use " + catalog);
        sparkSession.sql("show databases").show();
        sparkSession.sql(String.format("create database %s", database));
        var s1 = RowFactory.create("ss11", "ss12", "ss13", RowFactory.create("sss11", "sss12"));
        var s2 = RowFactory.create("ss21", "ss22", "ss13", RowFactory.create("sss21", "sss22"));
        long[] arr1 = {1L, 2L};
        long[] arr2 = {3L, 4L};

        sparkSession.sql(String.format("use %s", database));
        var df = sparkSession.createDataFrame(List.of(RowFactory.create("k1", 10, s1, arr1, "p1"),
                RowFactory.create("k2", 100, s2, arr2, "p2")),
                StructType.fromDDL(schema));
        writePath = new Path(catalogPath, new Path(database.toLowerCase(Locale.ROOT), remote_table.toLowerCase(Locale.ROOT) )).toString();
        df.write().mode("append").partitionBy("partition").parquet(writePath);
        df.write().mode("append").parquet("/tmp/nested");

        var minioEndpoint = MinioContainerTestUtil.getS3ParamForRemoteContainer(minio).get("s3_endpoint");
        sparkSession.sql(String.format("create table %s( %s) using parquet partitioned by (partition) options ('path' = '%s', 'url'='%s', 's3_endpoint'='%s')", remote_table, schema, writePath, url, minioEndpoint));
        sparkSession.sql(String.format("create table %s( %s) using parquet partitioned by (partition) options ('path'='%s')" , direct_table, schema, writePath));
    }

    @ParameterizedTest
    @MethodSource("getTestSQLsAndCatalogs")
    void testSql( String catalog, String table, String sql) {
        String remoteTableSql = String.format(sql, database + "." + table);
        String pathSql = String.format(sql, "parquet.`" + writePath + "`");
        sparkSession.sql(String.format("use %s", catalog));
        SparkHelper.assertEqual(sparkSession, pathSql, remoteTableSql);
        SparkHelper.assertDDExecution(sparkSession, remoteTableSql);
    }

    public static Arguments[] getTestSQLsAndCatalogs() {
        String[] testSQLs = getTestSQLs();
        String [] catalogs  = getCatalogs();
        String [] tables = getTables();
        Arguments[] result = new Arguments[catalogs.length * testSQLs.length * tables.length];
        int index = 0;
        for (String s : catalogs) {
            for (String table : tables) {
                for (String testSQL : testSQLs) {
                    result[index] = Arguments.of(s, table, testSQL);
                    index++;
                }
            }
        }
        return  result;
    }

    public static String[] getCatalogs() {
        return new String[]{ catalog };
    }

    private static String[] getTables() {
        return new String[]{
                remote_table,
                direct_table, };
    }

    public static String[] getTestSQLs() {
        return new String[]{
                "select * from %s",
                "select count(*) from %s",
                "select ss.ss3, ss.ss1, ss.ss4.sss2, arr[0] from %s",
                "select ss, arr from %s",
                "select hash(key), value from %s where (value + 100) = 200 order by key",
                "select hash(key), value from %s where partition = 'p1' order by key",
                "select * from %s where key = 'k1' order by key",
                "select * from %s order by key",
                "select count(*), partition from %s group by partition order by partition",
                "select count(*), partition, sum(value), min(value), max(value), partition from %s group by partition order by partition",
                "select count(*), sum(value), min(value), max(value), key from %s group by key order by key",
                "select count(*), sum(value), key, min(value), max(value), key from %s group by key order by key",
                "select count(*), sum(value), hash(key), min(value), max(value) from %s group by hash(key) order by hash(key)"
        };
    }

    @AfterAll
    public static void afterAll() {
        sparkSession.stop();
        minio.stop();
        combiner.stop();
    }
}
