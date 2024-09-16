package io.dazzleduck.combiner.server;

import io.dazzleduck.combiner.common.ConfigParameters;
import io.dazzleduck.combiner.common.client.CombinerClient;
import io.dazzleduck.combiner.common.model.QueryObject;
import io.micronaut.context.ApplicationContext;
import io.micronaut.runtime.server.EmbeddedServer;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.UploadObjectArgs;
import io.minio.errors.*;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.MinIOContainer;

import java.io.*;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static io.dazzleduck.combiner.server.MinioContainerTestUtil.*;


@MicronautTest
public class CombinerClientTest {
    @Inject
    EmbeddedServer server;

    @Inject
    ApplicationContext context;

    public static MinIOContainer minio =
            MinioContainerTestUtil.createContainer();

    public static MinioClient minioClient ;

    public static final String healthCheck = "health";

    private static QueryObject countStartQueryObject;

    @BeforeAll
    public static void beforeAll() throws IOException, InvalidKeyException,
            InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, ErrorResponseException {
        minio.start();
        minioClient = MinioContainerTestUtil.createClient(minio);
        minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
        var file = new File("src/test/resources/parquet/date=2024-01-01/test.parquet");
        minioClient.uploadObject(UploadObjectArgs
                .builder()
                .bucket(bucketName)
                .object("db/table/test.parquet")
                .filename(file.getAbsolutePath())
                .build());

        var largeFile = new File("src/test/resources/large_parquet/test.snappy.parquet");
        minioClient.uploadObject(UploadObjectArgs
                .builder()
                .bucket(bucketName)
                .object(largeParquetObjectName)
                .filename(largeFile.getAbsolutePath())
                .build());
        countStartQueryObject = createCountStartObject();
    }

    @Test
    public void withClient() throws IOException, InterruptedException, SQLException {
        var queryObjectWithBuilder = QueryObject
                .builder()
                .projections(List.of("key", "count(*)"))
                .groupBys(List.of("key"))
                .parameters(MinioContainerTestUtil.getS3Param(minio))
                .sources(List.of(String.format("s3://%s/%s", bucketName, parquetObjectName))).build();
        var url = server.getURL();
        var completePath = String.format("%s/%s", url, ServerConstants.COMPLETE_PARQUET_PATH);
        var inputStream = CombinerClient.getArrowStream(completePath,
                queryObjectWithBuilder
        );
        String expected = String.format("select key, count(*) from %s group by key", getSourceString());
        ArrowAsserts.equals(expected, inputStream );
    }

    @Test
    public void testHealthCheck() throws IOException, InterruptedException {
        var url = server.getURL();
        var client = HttpClient.newHttpClient();
        var dest = String.format("%s/%s", url.toString(), healthCheck);
        var response = client.send(
                    HttpRequest.newBuilder()
                            .GET()
                            .uri(URI.create(dest)).build(), HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(200, response.statusCode());
    }


    @Test
    public void selectStarLargeFile() throws IOException, SQLException, InterruptedException {
        var queryObjectWithBuilder = QueryObject
                .builder()
                .projections(List.of("*"))
                .parameters(MinioContainerTestUtil.getS3Param(minio))
                .sources(List.of(String.format("s3://%s/%s", bucketName, largeParquetObjectName))).build();
        var completePath = String.format("%s/%s", server.getURL(), ServerConstants.COMPLETE_PARQUET_PATH);
        var inputStream = CombinerClient.getArrowStream(completePath, queryObjectWithBuilder);
        readToNull(inputStream, true);
    }


    @Test
    public void selectCountStarLargeFile() throws IOException, SQLException, InterruptedException {
        var completePath = String.format("%s/%s", server.getURL(), ServerConstants.COMPLETE_PARQUET_PATH);
        var inputString = CombinerClient.getArrowStream(completePath, countStartQueryObject);
        String expected = String.format("select count(*) from %s", getLargeFileSourceString());
        ArrowAsserts.equals(expected, inputString);
    }

    @ParameterizedTest
    @ValueSource(ints = {25000})
    public void testNCountStartTest(int n ) throws InterruptedException {
        var executor = Executors.newFixedThreadPool(4);
        for (int i = 0; i < n; i++) {
            executor.execute(() -> {
                try {
                    var completePath = String.format("%s/%s", server.getURL(), ServerConstants.COMPLETE_PARQUET_PATH);
                    var inputStream = CombinerClient.getArrowStream(completePath, countStartQueryObject);
                    readToNull(inputStream, true);
                } catch (Exception e) {
                    e.printStackTrace(System.out);
                }
            });
        }
        executor.shutdown();
        var waitMinutes = 2;
        var finished = executor.awaitTermination(waitMinutes, TimeUnit.MINUTES);
        if (!finished) {
            throw new RuntimeException("Test Failed: Could not finish in minutes " + waitMinutes);
        }
    }

    @Test
    public void testDuckDBPerformanceWriteToFile() throws IOException, SQLException {
        var uuid = UUID.randomUUID();
        var fileOutputStream = new FileOutputStream(String.format("/tmp/%s.arrow", uuid));
        var output = new PipedOutputStream();
        var input = new PipedInputStream(output, ServerConstants.DEFAULT_NETWORK_BUFFER);
        var thread = new Thread(new Runnable() {
            byte[] bytes = new byte[8 * 1024 * 1024];
            @Override
            public void run() {
                try {
                    int read ;
                    do {
                        read = input.read(bytes);
                        if (read < 0) {
                            fileOutputStream.close();
                            break;
                        } else {
                            fileOutputStream.write(bytes);
                        }
                    } while (true);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
        thread.start();
        var sql = String.format("select * from %s", getLargeFileSourceString());
        var collector = SQLExecutor.executeAndProvideCollect(sql, output, ConfigParameters.ARROW_MAX_BATCH_SIZE);
        collector.executeCollect();
    }

    private String getSourceString() {
        return MinioContainerTestUtil.getSourceString(minio, bucketName, parquetObjectName);
    }

    private String getLargeFileSourceString() {
        return MinioContainerTestUtil.getSourceString(minio, bucketName, largeParquetObjectName);
    }

    private InputStream runCountStart() throws IOException, InterruptedException {
        var completePath = String.format("%s/%s", server.getURL(), ServerConstants.COMPLETE_PARQUET_PATH);
        return CombinerClient.getArrowStream(completePath, countStartQueryObject);
    }

    private static QueryObject createCountStartObject() {
        return QueryObject
                .builder()
                .projections(List.of("count(*)"))
                .parameters(MinioContainerTestUtil.getS3Param(minio))
                .sources(List.of(String.format("s3://%s/%s", bucketName, largeParquetObjectName))).build();

    }

    private void readToNull(InputStream inputStream, boolean silent) throws IOException {
        var bytes = new byte[1024 * 1024 * 4];
        var readBytes = 0;
        do {
            var read = inputStream.read(bytes);
            if(read < 0 ){
                break;
            } else {
                if (!silent) {
                    System.out.println("Read Bytes" + read);
                }
                readBytes += read;
            }
        }
        while (true);
    }
}
