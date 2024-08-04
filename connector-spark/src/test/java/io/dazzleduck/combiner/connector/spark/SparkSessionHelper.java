package io.dazzleduck.combiner.connector.spark;

import io.dazzleduck.combiner.connector.spark.catalog.DDInMemoryTableCatalog;
import io.dazzleduck.combiner.connector.spark.extension.DDExtensions;
import org.apache.spark.sql.SparkSession;
import org.testcontainers.containers.MinIOContainer;

import java.io.IOException;
import java.util.function.Function;

import static java.nio.file.Files.createTempDirectory;

public class SparkSessionHelper {

    public static final String LOCAL_CATALOG ="dd_catalog_local";
    public static final String S3_CATALOG ="dd_catalog_remote";


    public static SparkSession getSparkSession (MinIOContainer minio, String localCatalogPath) throws IOException {
        String s3CatalogPath = String.format("s3a://%s/", MinioContainerTestUtil.bucketName);
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
}
