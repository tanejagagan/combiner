package io.dazzleduck.combiner.server;

import io.minio.MinioClient;
import org.testcontainers.containers.MinIOContainer;

import java.util.Map;

public class MinioContainerTestUtil {

    public static String bucketName = "test-bucket";
    public static final String parquetObjectName = "db/table/test.parquet";
    public static final String largeParquetObjectName = "db/table/large_test.snappy.parquet";


    public static Map<String, String> getS3Param(MinIOContainer minio) {
        return Map.of("s3_endpoint", "localhost:" + minio.getMappedPort(9000),
                "s3_access_key_id", minio.getUserName(),
                "s3_secret_access_key", minio.getPassword(),
                "s3_use_ssl", "false");
    }

    public static String getSourceString(MinIOContainer minio, String bucketName, String objectName){
        int port = minio.getMappedPort(9000);
        int adminPort = minio.getMappedPort(9001);
        return String.format("read_parquet(['s3://%s/%s?s3_endpoint=localhost:%s&s3_access_key_id=%s&s3_secret_access_key=%s&s3_use_ssl=false'])",
                bucketName, objectName, port, minio.getUserName(), minio.getPassword());
    }

    public static  MinIOContainer createContainer() {
        return new MinIOContainer("minio/minio:RELEASE.2023-09-04T19-57-37Z").withExposedPorts(9000, 9001);
    }

    public static MinioClient createClient(MinIOContainer minio) {
        return MinioClient.builder().endpoint(minio.getS3URL())
                .credentials(minio.getUserName(), minio.getPassword()).build();
    }
}
