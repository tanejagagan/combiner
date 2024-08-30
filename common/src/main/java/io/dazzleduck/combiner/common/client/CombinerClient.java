package io.dazzleduck.combiner.common.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dazzleduck.combiner.common.model.QueryObject;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class CombinerClient {

    private static ObjectMapper objectMapper = new ObjectMapper();

    public static InputStream getArrowStream(String uri,
                                             QueryObject queryObjectWithFactory) throws IOException, InterruptedException {
        String json = objectMapper.writeValueAsString(queryObjectWithFactory);
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(uri))
                .header("Content-Type", "application/json")
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
