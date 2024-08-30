package io.dazzleduck.combiner.connector.spark;

import io.minio.MinioClient;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.containers.Network;

import java.util.Map;

public class MinioContainerTestUtil {

    public static final String parquetObjectName = "db/table/test.parquet";
    public static final String largeParquetObjectName = "db/table/large_test.snappy.parquet";
    public static final int MINIO_S3_PORT = 9000;
    public static final int MINIO_MGMT_PORT = 9001;
    public static String bucketName = "test-bucket";

    public static Map<String, String> getS3ParamForRemoteContainer(MinIOContainer minio) {
        String hostname = "minio";
        return Map.of("s3_endpoint", hostname + ":" + MINIO_S3_PORT,
                "s3_access_key_id", minio.getUserName(),
                "s3_secret_access_key", minio.getPassword(),
                "s3_use_ssl", "false");
    }

    public static Map<String, String> getS3ParamForInProcessConnection(MinIOContainer minio) {
        String hostname = "localhost";
        int port = minio.getMappedPort(MINIO_S3_PORT);
        return Map.of("s3_endpoint", hostname + ":" + port,
                "s3_access_key_id", minio.getUserName(),
                "s3_secret_access_key", minio.getPassword(),
                "s3_use_ssl", "false");
    }

    public static Map<String, String> getSparkConfig(MinIOContainer minio) {
        var password = minio.getPassword();
        return Map.of("spark.hadoop.fs.s3a.access.key", minio.getUserName(),
                "spark.hadoop.fs.s3a.secret.key", minio.getPassword(),
                "spark.hadoop.fs.s3a.endpoint", minio.getS3URL(),
                "spark.hadoop.fs.s3a.connection.ssl.enabled", "false",
                "spark.hadoop.fs.s3a.path.style.access", "true",
                "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");

                /*
                "fs.s3a.path.style.access", "true",
                "fs.s3a.attempts.maximum", "1",
                "fs.s3a.connection.establish.timeout", "5000",
                "fs.s3a.connection.timeout", "10000");
                 */
    }

    public static String getSourceString(MinIOContainer minio, String bucketName, String objectName){
        int port = minio.getMappedPort(MINIO_S3_PORT);
        return String.format("read_parquet(['s3://%s/%s?s3_endpoint=localhost:%s&s3_access_key_id=%s&s3_secret_access_key=%s&s3_use_ssl=false'])",
                bucketName, objectName, port, minio.getUserName(), minio.getPassword());
    }

    public static  MinIOContainer createContainer(String alias, Network network) {
        return new MinIOContainer("minio/minio:RELEASE.2023-09-04T19-57-37Z")
                .withNetwork(network)
                .withNetworkAliases(alias)
                .withExposedPorts(MINIO_S3_PORT, MINIO_MGMT_PORT);
    }

    public static MinioClient createClient(MinIOContainer minio) {
        return MinioClient.builder().endpoint(minio.getS3URL())
                .credentials(minio.getUserName(), minio.getPassword()).build();
    }
}