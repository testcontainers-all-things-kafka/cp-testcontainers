package net.christophschubert.cp.testcontainers.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Authenticator;
import java.net.PasswordAuthentication;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Set;

/**
 * Minimal implementation of a Java client for Kafka Connect.
 * Duplicated here from https://github.com/christophschubert/kafka-connect-java-client to prevent cyclic dependencies.
 */
public class ConnectClient {

    private final static Logger logger = LoggerFactory.getLogger(ConnectClient.class);

    private final String baseUrl;
    private final HttpClient httpClient;
    private final ObjectMapper mapper = new ObjectMapper();


    public ConnectClient(String baseURL, String userName, String password) {
        this.baseUrl = baseURL;
        this.httpClient = HttpClient.newBuilder().authenticator(new Authenticator() {
            @Override
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(userName, password.toCharArray());
            }
        }).build();
    }

    public ConnectClient(String baseURL) {
        this.baseUrl = baseURL;
        this.httpClient = HttpClient.newBuilder().build();
    }


    public Set<String> getConnectors() throws IOException, InterruptedException {
        final var request = HttpRequest.newBuilder(URI.create(baseUrl + "/connectors")).build();
        final var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        return mapper.readValue(response.body(), new TypeReference<Set<String>>() {});
    }

    public void startConnector(ConnectorConfig config) throws IOException, InterruptedException {
        final var request = HttpRequest.newBuilder(URI.create(baseUrl + "/connectors"))
                .POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(config)))
                .header("Content-Type", "application/json")
                .build();
        logger.debug("submitting config: {}",  mapper.writeValueAsString(config));
        final var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() == 201) {
            logger.info("successfully started connector {}", config.name);
        } else {
            logger.error("Failed to start connector '{}'", config.name);
        }
    }
}
