package io.dazzleduck.combiner.connector.spark;

import io.dazzleduck.combiner.connector.spark.catalog.DDInMemoryTableCatalog;
import io.dazzleduck.combiner.connector.spark.extension.DDExtensions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.testcontainers.containers.MinIOContainer;

import java.io.IOException;
import java.util.function.Function;

import static java.nio.file.Files.createTempDirectory;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class SparkHelper {

    public static final String LOCAL_CATALOG ="dd_catalog_local";
    public static final String S3_CATALOG ="dd_catalog_remote";


    public static SparkSession getSparkSession (MinIOContainer minio,
                                                String localCatalogPath,
                                                String s3CatalogPath) throws IOException {

            return SparkSession
                    .builder()
                    .master("local")
                    .config("spark.sql.catalog.dd_catalog_local", DDInMemoryTableCatalog.class.getName())
                    .config("spark.sql.catalog.dd_catalog_local.path", localCatalogPath)
                    .config("spark.sql.catalog.dd_catalog_remote", DDInMemoryTableCatalog.class.getName())
                    .config("spark.sql.catalog.dd_catalog_remote.path", s3CatalogPath)
                    .config("spark.sql.parquet.aggregatePushdown", true)
                    .config("spark.sql.codegen.wholeStage", false)
                    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                    .config("spark.hadoop.fs.s3a.path.style.access", "true")
                    .config("spark.hadoop.fs.s3a.secret.key", minio.getPassword())
                    .config("spark.hadoop.fs.s3a.access.key", minio.getUserName())
                    .config("spark.hadoop.fs.s3a.endpoint", minio.getS3URL())
                    .config("spark.sql.extensions", DDExtensions.class.getName())
                    .getOrCreate();
    }

    public static <R> R withTempLocation(Function<String, R> function) throws IOException {
        String tmpdir = createTempDirectory("tmpCatalogs").toFile().getAbsolutePath();
        return function.apply(tmpdir);
    }

    static void assertEqual(SparkSession sparkSession, String expectedSql, String resultSql) {
        var expected = sparkSession.sql(expectedSql);
        var result = sparkSession.sql(resultSql);
        Row[] e = (Row[] )expected.collect();
        Row[] r = (Row[] )result.collect();
        assertArrayEquals(e, r, String.format("\n %s\n %s\n %s\n %s",
                expectedSql, resultSql,
                expected.showString(20, 100, false),
                result.showString(20, 100, false)));
    }
    static void assertDFEquals(Dataset<Row> expected, Dataset<Row> result){
        Row[] e = (Row[] )expected.collect();
        Row[] r = (Row[] )result.collect();
        assertArrayEquals(e, r, String.format("\n%s \n%s",
                expected.showString(20, 100, false),
                result.showString(20, 100, false)));
    }
}
