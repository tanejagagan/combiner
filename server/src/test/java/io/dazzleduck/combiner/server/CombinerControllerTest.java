package io.dazzleduck.combiner.server;

import io.dazzleduck.combiner.common.Constants;
import io.dazzleduck.combiner.server.model.QueryObject;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.UploadObjectArgs;
import io.minio.errors.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import jakarta.inject.Inject;
import org.testcontainers.containers.MinIOContainer;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.net.URL;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static io.dazzleduck.combiner.server.MinioContainerTestUtil.*;

@MicronautTest
public class CombinerControllerTest {

    @Inject
    @Client("/")
    HttpClient client;

    public static MinIOContainer minio =
            MinioContainerTestUtil.createContainer();

    public static MinioClient minioClient ;

    @BeforeAll
    public static void beforeAll() throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, ErrorResponseException, URISyntaxException {
        minio.start();
        minioClient = MinioContainerTestUtil.createClient(minio);
        minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
        URL url = CombinerControllerTest.class.getResource("/parquet/date=2024-01-01/test.parquet");
        // Copy the resource in temp dir because in some enviroment
        var name = Paths.get(url.toURI()).toString();
        var uploadObjectArg = UploadObjectArgs.builder()
                .bucket(bucketName)
                .object("/db/table/test.parquet")
                .filename(name).build();
        minioClient.uploadObject(uploadObjectArg);
        System.out.println("Upload complete");
    }


    @Test
    public void testGenerateSeries() throws IOException, SQLException {

        HttpRequest<?> request = HttpRequest
                .GET(String.format("%s/%s?size=200", ServerConstants.QUERY_PATH, ServerConstants.SERIES_PATH))
                .header(Constants.QUERY_HEADER, UUID.randomUUID().toString())
                .accept(MediaType.APPLICATION_OCTET_STREAM_TYPE);
        HttpResponse<byte[]> resp = client.toBlocking().exchange(request, byte[].class);
        byte[] responseBytes = resp.body();
        ArrowAsserts.equals("select * from generate_series(200)", responseBytes);
    }

    /*
    Uncomment this test when validation will start working
    @Test
    public void testIncorrectSeries() throws IOException, SQLException {
        HttpRequest<?> request = HttpRequest.GET(String.format("%s/%s?size=20000", Constants.QUERY_PATH, Constants.SERIES_PATH)).accept(MediaType.APPLICATION_OCTET_STREAM_TYPE);
        try {
            HttpResponse<byte[]> resp = client.toBlocking().exchange(request, byte[].class);
            // Code should not reach here
            assertEquals(10, 20);
        } catch (HttpClientResponseException e){
            var msg = e.getMessage();
            e.printStackTrace();
        }
    }

     */

    @Test
    public void testExecuteQuery() throws IOException, SQLException {
        var queryObject = new QueryObject();
        queryObject.setProjections(List.of("*"));
        queryObject.setSources(List.of(String.format("s3://%s/%s", bucketName, parquetObjectName)));
        queryObject.setParameters(getS3Param());
        testParquetQueryWith(String.format("select * from %s", getSourceString()), queryObject);
    }

    @Test
    public void testAggregation() throws IOException, SQLException {
        var queryObject = new QueryObject();
        queryObject.setProjections(List.of("key, count(*)"));
        queryObject.setSources(List.of(String.format("s3://%s/%s", bucketName, parquetObjectName)));
        queryObject.setGroupBys(List.of("key"));
        queryObject.setParameters(getS3Param());
        String expected = String.format("select key, count(*) from %s group by key",
                getSourceString());
        testParquetQueryWith(expected, queryObject);
    }



    private void testParquetQueryWith(String expected, QueryObject queryObject) throws IOException, SQLException {
        String path = String.format("%s/%s", ServerConstants.QUERY_PATH, ServerConstants.PARQUET_PATH);
        HttpRequest<?> request = HttpRequest
                .POST(path, queryObject)
                .accept(MediaType.APPLICATION_OCTET_STREAM_TYPE)
                .header(Constants.QUERY_HEADER, UUID.randomUUID().toString());
        HttpResponse<byte[]> resp = client.toBlocking().exchange(request, byte[].class);
        byte[] responseBytes = resp.body();
        ArrowAsserts.equals(expected,
                responseBytes);
    }


    @Test
    public void testIncorrectSql() {
        var queryObject3 = new QueryObject();
        queryObject3.setProjections(List.of("_")); // missing select
        queryObject3.setSources(List.of("200"));
        HttpRequest<?> request = HttpRequest.POST(ServerConstants.QUERY_PATH, queryObject3).accept(MediaType.APPLICATION_OCTET_STREAM_TYPE);
        try {
           client.toBlocking().exchange(request, byte[].class);
            // Code should not reach here
            assertEquals(10, 20);
        } catch (HttpClientResponseException e){
            e.getMessage();
            e.printStackTrace();
        }
    }

    @Test
    public void testMinio() throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, ErrorResponseException, SQLException {
        var sql = String.format("select * from %s", getSourceString());
        System.out.println(sql);
        ArrowAsserts.printResult(sql);
    }

    private String getSourceString() {
        return MinioContainerTestUtil.getSourceString(minio, bucketName, parquetObjectName);
    }

    private Map<String, String > getS3Param() {
        return MinioContainerTestUtil.getS3Param(minio);
    }
}