package net.christophschubert.cp.testcontainers.util;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

/**
 * Minimal implementation of a Java client for Kafka Connect.
 * Duplicated here from https://github.com/christophschubert/kafka-connect-java-client to prevent cyclic dependencies.
 */
public class ConnectClient {

    private final String baseUrl;
    private final HttpClient httpClient;
    private final ObjectMapper mapper = new ObjectMapper();

    public ConnectClient(String baseURL) {
        this.baseUrl = baseURL;
        this.httpClient = HttpClient.newBuilder().build();
    }

    public void startConnector(ConnectorConfig config) throws IOException, InterruptedException {
        final var request = HttpRequest.newBuilder(URI.create(baseUrl + "/connectors"))
                .POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(config)))
                .header("Content-Type", "application/json")
                .build();
        System.out.println("submitting config: " + mapper.writeValueAsString(config));
        final var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        System.out.println(response);
        System.out.println(response.body());
    }
}
