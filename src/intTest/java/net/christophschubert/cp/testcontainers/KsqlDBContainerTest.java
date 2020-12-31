package net.christophschubert.cp.testcontainers;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;
import org.testcontainers.containers.Network;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;

public class KsqlDBContainerTest {
    final ObjectMapper mapper = new ObjectMapper();

    @Test
    public void setupKsqlDB() {
        final var containerFactory = new CPTestContainerFactory(Network.newNetwork());

        final var kafka = containerFactory.createKafka();
        kafka.start();

        final var ksqlDB = containerFactory.createKsqlDB(kafka);
        ksqlDB.withLogConsumer(outputFrame -> System.out.print(outputFrame.getUtf8String()));
        ksqlDB.start();
    }

    @Test
    public void setupKsqlDBWithSchemaRegistry() throws URISyntaxException, IOException, InterruptedException {
        final var containerFactory = new CPTestContainerFactory(Network.newNetwork());

        final var kafka = containerFactory.createKafka();
        kafka.start();

        final var schemaRegistry = containerFactory.createSchemaRegistry(kafka);
        schemaRegistry.start();

        final var serviceId = "test_ksqldb";
        final var ksqlDB = containerFactory
                .createKsqlDB(kafka)
                .withSchemaRegistry(schemaRegistry)
                .withServiceId(serviceId);
        ksqlDB.start();

        final var httpClient = HttpClient.newBuilder().build();
        final var request = HttpRequest.newBuilder(new URI(ksqlDB.getBaseUrl() + "/info")).build();
        final var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        Assert.assertEquals(200, response.statusCode());

        final var parsedResponse = mapper.readValue(response.body(), new TypeReference<Map<String, Map<String, String>>>() {});
        Assert.assertEquals(serviceId, parsedResponse.get("KsqlServerInfo").get("ksqlServiceId"));
    }
}
