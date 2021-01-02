package net.christophschubert.cp.testcontainers;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.christophschubert.cp.testcontainers.util.ConnectClient;
import net.christophschubert.cp.testcontainers.util.ConnectorConfig;
import net.christophschubert.cp.testcontainers.util.TestClients;
import net.christophschubert.cp.testcontainers.util.TestClients.TestConsumer;
import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Test;
import org.testcontainers.containers.Network;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
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


    @Test
    public void setupHeadlessKsqlDBWithSchemaRegistryAndConnect() throws IOException, InterruptedException {
        final var containerFactory = new CPTestContainerFactory(Network.newNetwork());

        final var kafka = containerFactory.createKafka();
        kafka.start();

        final var schemaRegistry = containerFactory.createSchemaRegistry(kafka);
        schemaRegistry.start();

        final var connect = containerFactory.createCustomConnector("confluentinc/kafka-connect-datagen:0.4.0", kafka);
        connect.start();

        final ConnectorConfig connectorConfig = ConnectorConfig.source("datagen-users",  "io.confluent.kafka.connect.datagen.DatagenConnector")
                .with("kafka.topic", "users")
                .with("quickstart", "users")
                .withKeyConverter("org.apache.kafka.connect.storage.StringConverter")
                .withValueConverter("org.apache.kafka.connect.json.JsonConverter")
                .with("value.converter.schemas.enable", "false")
                .with("max.interval", 1000)
                .with("iterations", 10000000);
        ConnectClient connectClient = new ConnectClient(connect.getBaseUrl());
        connectClient.startConnector(connectorConfig);

        final var serviceId = "test_ksqldb";
        final var ksqlDB = containerFactory
                .createKsqlDB(kafka)
                .withSchemaRegistry(schemaRegistry)
                .withConnect(connect)
                .withQueriesFile("./src/intTest/resources/ksqlTest.sql")
                .withServiceId(serviceId)
                .withStartupTimeout(Duration.ofMinutes(5));
        ksqlDB.start();

        final TestConsumer<String, GenericRecord> consumer = TestClients.createAvroConsumer(kafka.getBootstrapServers(), schemaRegistry.getBaseUrl());
        consumer.subscribe(List.of("users_avro"));

        var messages = consumer.consumeUntil(5);
        Assert.assertEquals(5, messages.size());
        Assert.assertNotNull(messages.get(0).get("USERID"));
    }
}
