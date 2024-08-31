package io.dazzleduck.combiner.common.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dazzleduck.combiner.common.Constants;
import io.dazzleduck.combiner.common.model.QueryObject;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.UUID;

public class CombinerClient {

    private static ObjectMapper objectMapper = new ObjectMapper();

    public static InputStream getArrowStream(String uri,
                                             QueryObject queryObject) throws IOException, InterruptedException {
        var queryId = UUID.randomUUID();
        return getArrowStream(uri, queryObject, queryId);
    }

    public static InputStream getArrowStream(String uri,
                                             QueryObject queryObjectWithFactory,
                                             UUID queryId) throws IOException, InterruptedException {
        String json = objectMapper.writeValueAsString(queryObjectWithFactory);
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(uri))
                .header("Content-Type", "application/json")
                .header(Constants.QUERY_HEADER, queryId.toString())
                .POST(HttpRequest
                        .BodyPublishers
                        .ofString(json)).build();
        HttpResponse<InputStream> response = client.send(request,
                HttpResponse.BodyHandlers.ofInputStream());
        if (response.statusCode() == 200 ) {
            return response.body();
        } else {
            throw new RuntimeException(
                    String.format("Failed to work with %s : status Code %s, Error %s", uri, response.statusCode(),
                            new String(response.body().readAllBytes())));
        }
    }

    public static InputStream getArrowSeries(String uri, int size) throws IOException, InterruptedException {
        return getArrowSeries(uri, size, UUID.randomUUID());
    }

    public static InputStream getArrowSeries(String uri, int size, UUID queryId) throws IOException, InterruptedException {
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(uri)).GET().build();
        HttpResponse<InputStream> response = client.send(request,
                HttpResponse.BodyHandlers.ofInputStream());
        if (response.statusCode() == 200 ) {
            return response.body();
        } else {
            throw new RuntimeException(
                    String.format("Failed to work with %s : status Code %s, Error %s", uri, response.statusCode(),
                            new String(response.body().readAllBytes())));
        }
    }
}
