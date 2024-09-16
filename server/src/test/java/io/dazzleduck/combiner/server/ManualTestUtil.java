package io.dazzleduck.combiner.server;

import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.UploadObjectArgs;
import io.minio.errors.*;

import java.io.File;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

public class ManualTestUtil {

    public static void main(String[] args) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, ErrorResponseException, XmlParserException, InternalException {
        String url ;
        if(args.length < 1) {
            url = "http://localhost:9000";
        } else {
            url = args[args.length - 1];
        }
        var minioClient = MinioClient.builder().endpoint(url)
                .credentials("minioadmin", "minioadmin").build();
        try {
            minioClient.makeBucket(MakeBucketArgs.builder().bucket(MinioContainerTestUtil.bucketName).build());
        } catch (Exception e){
            System.out.println(e);
        }
        uploadMinIOFiles(minioClient, "src/test/resources/large_parquet/test.snappy.parquet",  MinioContainerTestUtil.bucketName,
                MinioContainerTestUtil.largeParquetObjectName );
        uploadMinIOFiles(minioClient, "src/test/resources/parquet/date=2024-01-01/test.parquet", MinioContainerTestUtil.bucketName,
                MinioContainerTestUtil.parquetObjectName );
    }


    private static void uploadMinIOFiles(MinioClient minioClient, String src, String bucketName, String dest) throws IOException, ServerException, InsufficientDataException, InternalException, InvalidResponseException, InvalidKeyException, NoSuchAlgorithmException, XmlParserException, ErrorResponseException {
        System.out.printf("Uploading %s ==> %s/%s%n", src, bucketName, dest);
        var file = new File(src);
        minioClient.uploadObject(UploadObjectArgs
                .builder()
                .bucket(bucketName)
                .object(dest)
                .filename(file.getAbsolutePath())
                .build());
    }
}
